// @effect-diagnostics nodeBuiltinImport:off - streams a fixed byte range and checks both stats on the same open file.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeStream from "node:stream";
import * as NodeZlib from "node:zlib";

const MAX_ROLLOUT_BYTES = 64 * 1024 * 1024;
const MAX_ROLLOUT_SEGMENTS = 16;
const ROLLOUT_ID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

interface HistoryBase {
  readonly thread_id: string;
  readonly end_byte_offset: number;
}

interface RolloutRecord {
  readonly timestamp?: string;
  readonly type?: string;
  readonly payload?: {
    readonly id?: string;
    readonly history_base?: HistoryBase;
    readonly type?: string;
    readonly turn_id?: string;
    readonly role?: string;
    readonly content?: string | ReadonlyArray<{ readonly type?: string; readonly text?: string }>;
  };
}

/** Resolve a legacy import's last visible reply to a completed native turn. */
export async function findCodexImportedHistoryAnchor(
  path: string,
  nativeThreadId: string,
  boundary: { readonly text: string; readonly createdAt: string },
  signal: AbortSignal,
): Promise<string | null> {
  if (!NodePath.isAbsolute(path) || !/\.jsonl(?:\.zst)?$/.test(path)) return null;
  const timestamp = Date.parse(boundary.createdAt);
  if (!Number.isFinite(timestamp) || boundary.text === "") return null;

  let segmentPath = path;
  let segmentId = nativeThreadId;
  let prefix: number | undefined;
  let remaining = MAX_ROLLOUT_BYTES;
  const seen = new Set([path]);
  while (remaining > 0 && seen.size <= MAX_ROLLOUT_SEGMENTS) {
    const segment = await readRolloutSegment(
      segmentPath,
      nativeThreadId,
      segmentId,
      boundary.text,
      timestamp,
      prefix,
      remaining,
      signal,
    );
    if (segment === null) return null;
    if (segment.turn !== null) return segment.turn;
    if (segment.base === undefined) return null;
    const parent = await findHistoryBasePath(path, segment.base.thread_id, signal);
    if (parent === null || seen.has(parent)) return null;
    seen.add(parent);
    segmentPath = parent;
    segmentId = segment.base.thread_id;
    prefix = segment.base.end_byte_offset;
    remaining -= segment.bytes;
  }
  return null;
}

async function findHistoryBasePath(head: string, id: string, signal: AbortSignal) {
  if (!ROLLOUT_ID.test(id)) return null;
  const directory = NodePath.dirname(head);
  const historyRoot = /[/\\]sessions[/\\]\d{4}[/\\]\d{2}[/\\]\d{2}$/.test(directory)
    ? NodePath.resolve(directory, "../../../..")
    : NodePath.basename(directory) === "archived_sessions"
      ? NodePath.dirname(directory)
      : null;
  if (historyRoot === null) return null;
  const paths = new Set<string>();
  for await (const entry of NodeFSP.glob(
    [
      `{sessions/*/*/*,archived_sessions}/rollout-{*-,*_}${id}.jsonl`,
      `{sessions/*/*/*,archived_sessions}/rollout-{*-,*_}${id}.jsonl.zst`,
    ],
    { cwd: historyRoot },
  )) {
    signal.throwIfAborted();
    paths.add(NodePath.join(historyRoot, entry));
    if (paths.size > 1) return null;
  }
  return paths.values().next().value ?? null;
}

async function readRolloutSegment(
  path: string,
  nativeThreadId: string,
  segmentId: string,
  boundaryText: string,
  timestamp: number,
  prefix: number | undefined,
  remaining: number,
  signal: AbortSignal,
) {
  await using file = await NodeFSP.open(path, "r");
  const before = await file.stat();
  const compressed = path.endsWith(".zst");
  if (
    !before.isFile() ||
    before.size === 0 ||
    before.size > MAX_ROLLOUT_BYTES ||
    (prefix !== undefined && (prefix <= 0 || prefix > remaining)) ||
    (!compressed && (prefix ?? before.size) > Math.min(remaining, before.size))
  )
    return null;
  const source = file.createReadStream({
    autoClose: false,
    end: (compressed ? before.size : (prefix ?? before.size)) - 1,
    signal,
  });
  const decompressor = compressed ? NodeZlib.createZstdDecompress() : null;
  if (decompressor !== null) {
    source.on("error", (error) => decompressor.destroy(error));
    source.pipe(decompressor);
  }
  const decoded = decompressor ?? source;
  let bytes = 0;
  let prefixEndsOnNewline = false;
  const stream = NodeStream.Readable.from(
    (async function* () {
      for await (const chunk of decoded as AsyncIterable<Buffer>) {
        const data = prefix === undefined ? chunk : chunk.subarray(0, prefix - bytes);
        bytes += data.length;
        if (bytes > remaining) return;
        prefixEndsOnNewline = data.at(-1) === 10;
        yield data;
        if (compressed && bytes === prefix) return;
      }
    })(),
  );
  const lines = NodeReadline.createInterface({ input: stream, crlfDelay: Infinity });
  let verified = false;
  let base: HistoryBase | undefined;
  let currentTurn: string | null = null;
  let matchingTurn: string | null = null;
  let completedTurn: string | null = null;
  let matches = 0;
  try {
    for await (const line of lines) {
      if (line.trim() === "") continue;
      // Tool payloads can be large. Decode only records that can establish the boundary.
      if (verified && !/"(?:turn_context|task_started|task_complete|message)"/.test(line)) continue;
      const { type, payload, timestamp: recordTimestamp } = JSON.parse(line) as RolloutRecord;
      if (!verified) {
        if (
          type !== "session_meta" ||
          (payload?.id !== nativeThreadId && payload?.id !== segmentId)
        )
          return null;
        const inherited = payload.history_base;
        if (inherited != null) {
          if (
            !ROLLOUT_ID.test(inherited.thread_id) ||
            !Number.isSafeInteger(inherited.end_byte_offset)
          )
            return null;
          base = inherited;
        }
        verified = true;
        continue;
      }
      if (payload === undefined) continue;
      if (type === "turn_context" || (type === "event_msg" && payload.type === "task_started")) {
        const nextTurn = payload.turn_id || null;
        if (nextTurn !== currentTurn) matchingTurn = null;
        currentTurn = nextTurn;
      } else if (type === "event_msg" && payload.type === "task_complete") {
        if (matchingTurn !== null && matchingTurn === payload.turn_id) completedTurn = matchingTurn;
        currentTurn = null;
        matchingTurn = null;
      } else if (
        type === "response_item" &&
        payload.type === "message" &&
        (payload.role === "assistant" || payload.role === "user")
      ) {
        const text =
          typeof payload.content === "string"
            ? payload.content.trim()
            : (payload.content ?? [])
                .filter((block) => ["text", "input_text", "output_text"].includes(block.type ?? ""))
                .map((block) => block.text?.trim() ?? "")
                .filter(Boolean)
                .join("\n");
        if (text === "") continue;
        matchingTurn = null;
        if (
          payload.role === "assistant" &&
          Date.parse(recordTimestamp ?? "") === timestamp &&
          text === boundaryText
        ) {
          matches++;
          matchingTurn = currentTurn;
        }
      }
    }
    const after = await file.stat();
    if (
      !verified ||
      matches > 1 ||
      (matches === 1 && completedTurn === null) ||
      bytes > remaining ||
      (prefix !== undefined && (bytes !== prefix || !prefixEndsOnNewline)) ||
      before.size !== after.size ||
      before.mtimeMs !== after.mtimeMs
    )
      return null;
    return { turn: completedTurn, base, bytes };
  } finally {
    lines.close();
    stream.destroy();
    decoded.destroy();
    source.destroy();
  }
}
