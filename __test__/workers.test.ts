import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Worker } from 'node:worker_threads';
import { open } from '../index.js';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-wt-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const entry = resolve(__dirname, 'worker-entry.mjs');
function run(workerData: Record<string, unknown>): Promise<any> {
  return new Promise((res, rej) => {
    const w = new Worker(entry, { workerData });
    w.once('message', (m) => res(m));
    w.once('error', rej);
  });
}

describe('cross-worker shared engine', () => {
  it('a reader worker sees a writer worker’s insert live (no cross-process)', async () => {
    // Main thread creates the engine first so both workers attach to it.
    const ks = await open({ path: dir });
    await ks.partition('shared');

    const writer = run({ role: 'writer', path: dir, key: 'wk', value: 'wv' });
    await writer;
    const reader = await run({ role: 'reader', path: dir, key: 'wk', value: 'wv' });
    expect(reader.seen).toBe('wv');

    await ks.close();
  }, 20_000);
});
