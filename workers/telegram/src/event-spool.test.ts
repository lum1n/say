import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { EventSpool } from "./event-spool.js";
import { envelope } from "./protocol.js";

test("event spool replays events until acknowledged", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "say-telegram-events-"));
  try {
    const first = new EventSpool(directory);
    await first.initialize();
    const stored = await first.append(envelope("account.status", { state: "live" }));
    assert.ok(stored.id);

    const restarted = new EventSpool(directory);
    await restarted.initialize();
    assert.deepEqual(await restarted.pending(), [stored]);
    await restarted.ack(stored.id);
    assert.deepEqual(await restarted.pending(), []);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
