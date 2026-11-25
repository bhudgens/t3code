// @effect-diagnostics nodeBuiltinImport:off - creates a disposable on-disk rollout for the Node reader.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
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
