// @effect-diagnostics nodeBuiltinImport:off - creates a disposable on-disk rollout for the Node reader.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { expect, it } from "vite-plus/test";

import { findCodexImportedHistoryAnchor } from "./CodexImportedHistoryAnchor.ts";

const boundary = { text: "Fixed\nDone", createdAt: "2026-09-01T10:00:00.000Z" };
const session = { type: "session_meta", payload: { id: "native-thread" } };
const context = { type: "turn_context", payload: { turn_id: "native-turn" } };
const assistant = {
  timestamp: boundary.createdAt,
  type: "response_item",
  payload: {
    type: "message",
    role: "assistant",
    content: [
      { type: "output_text", text: " Fixed " },
      { type: "output_text", text: "Done" },
    ],
  },
};
const completed = { type: "event_msg", payload: { type: "task_complete", turn_id: "native-turn" } };

it.each([
  {
    name: "the last reply of a completed turn",
    records: [assistant, completed],
    expected: "native-turn",
  },
  { name: "an incomplete turn", records: [assistant], expected: null },
  {
    name: "a reply followed by another reply in the same turn",
    records: [assistant, { ...assistant, timestamp: "2026-09-01T10:01:00.000Z" }, completed],
    expected: null,
  },
  {
    name: "a duplicate exact match",
    records: [assistant, completed, context, assistant, completed],
    expected: null,
  },
  {
    name: "a reply with a different timestamp",
    records: [{ ...assistant, timestamp: "2026-09-01T10:01:00.000Z" }, completed],
    expected: null,
  },
])("resolves a rollout boundary for $name", async ({ records, expected }) => {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "codex-import-anchor-"));
  try {
    const path = NodePath.join(dir, "rollout.jsonl");
    await NodeFSP.writeFile(
      path,
      [session, context, ...records].map((record) => JSON.stringify(record)).join("\n") + "\n",
    );
    const signal = new AbortController().signal;
    expect(await findCodexImportedHistoryAnchor(path, "native-thread", boundary, signal)).toBe(
      expected,
    );
    expect(await findCodexImportedHistoryAnchor(path, "wrong-thread", boundary, signal)).toBeNull();
  } finally {
    await NodeFSP.rm(dir, { recursive: true });
  }
});

it("follows a compressed history prefix without reading the discarded suffix", async () => {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "codex-import-lineage-"));
  try {
    const parent = "01a1172e-b8ac-7bd1-bf3d-19075d5f750e";
    const parentDir = NodePath.join(dir, "sessions", "2026", "10", "07");
    const headDir = NodePath.join(dir, "sessions", "2026", "10", "29");
    await NodeFSP.mkdir(parentDir, { recursive: true });
    await NodeFSP.mkdir(headDir, { recursive: true });
    const encode = (records: readonly unknown[]) =>
      records.map((record) => JSON.stringify(record)).join("\n") + "\n";
    const retained = encode([session, context, assistant, completed]);
    await NodeFSP.writeFile(
      NodePath.join(parentDir, `rollout-2026-10-07T01-23-45_${parent}.jsonl.zst`),
      NodeZlib.zstdCompressSync(Buffer.from(retained + encode([context, assistant, completed]))),
    );
    const head = NodePath.join(headDir, "rollout-2026-10-29T01-23-45.jsonl");
    const signal = new AbortController().signal;
    const writeHead = (prefix: string) =>
      NodeFSP.writeFile(
        head,
        encode([
          {
            ...session,
            payload: {
              ...session.payload,
              history_base: {
                thread_id: parent,
                end_byte_offset: Buffer.byteLength(prefix),
              },
            },
          },
        ]),
      );

    await writeHead(retained);
    expect(await findCodexImportedHistoryAnchor(head, "native-thread", boundary, signal)).toBe(
      "native-turn",
    );
    await writeHead(encode([session, context]));
    expect(
      await findCodexImportedHistoryAnchor(head, "native-thread", boundary, signal),
    ).toBeNull();
  } finally {
    await NodeFSP.rm(dir, { recursive: true });
  }
});
