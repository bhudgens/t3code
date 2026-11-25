// @effect-diagnostics nodeBuiltinImport:off - streams a fixed byte range and checks both stats on the same open file.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";

const MAX_ROLLOUT_BYTES = 64 * 1024 * 1024;

interface RolloutRecord {
  readonly timestamp?: string;
  readonly type?: string;
  readonly payload?: {
    readonly id?: string;
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
  if (!NodePath.isAbsolute(path) || !path.endsWith(".jsonl")) return null;
  const timestamp = Date.parse(boundary.createdAt);
  if (!Number.isFinite(timestamp) || boundary.text === "") return null;

  await using file = await NodeFSP.open(path, "r");
  const before = await file.stat();
  if (!before.isFile() || before.size === 0 || before.size > MAX_ROLLOUT_BYTES) return null;
  const stream = file.createReadStream({ autoClose: false, end: before.size - 1, signal });
  const lines = NodeReadline.createInterface({ input: stream, crlfDelay: Infinity });
  let verified = false;
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
        if (type !== "session_meta" || payload?.id !== nativeThreadId) return null;
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
          text === boundary.text
        ) {
          matches++;
          matchingTurn = currentTurn;
        }
      }
    }
    const after = await file.stat();
    return matches === 1 && before.size === after.size && before.mtimeMs === after.mtimeMs
      ? completedTurn
      : null;
  } finally {
    lines.close();
    stream.destroy();
  }
}
