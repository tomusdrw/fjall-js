import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';

import { open } from '../dist/index.js';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-recreate-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

// Regression for the "reset" workload (typeberry): one keyspace whose five
// partitions are repeatedly deleted and recreated. On fjall 2.11.2 this raced
// the background flusher and, after ~20-50 cycles, either threw
// `FjallError: PartitionDeleted` from `partition(name)` or panicked the flusher
// ("removal index (is 0) should be < len (is 0)"), poisoning the shared engine.
// Fixed by our patched fjall (fork) plus a bounded directory-removal wait in the
// napi wrapper; see Cargo.toml and src/engine.rs `delete_partition`.
describe('delete + recreate partitions repeatedly', () => {
  it('survives 100+ delete/recreate cycles under heavy background work', async () => {
    const names = ['headers', 'extrinsics', 'postStateRoots', 'states', 'values'];
    const ks = await open({ path: dir, cacheSizeBytes: 128 * 1024 * 1024 });

    // Pre-generated, reused: a big value drives the write buffer to flush.
    const payload = Buffer.alloc(2048, 7);
    const key = (i: number, j: number, prefix: string) => Buffer.from(`${prefix}-${i}-${j}`);

    try {
      for (let i = 0; i < 100; i++) {
        // (1) create five partitions
        const parts = await Promise.all(names.map((name) => ks.partition(name)));

        // (2) write enough data to exercise flush / background work
        await Promise.all(
          parts.map((p, idx) =>
            p.insertBatch(
              Array.from({ length: idx < 3 ? 20 : 400 }, (_, j) => ({
                key: key(i, j, names[idx]),
                value: idx < 3 ? randomBytes(64) : payload,
              })),
            ),
          ),
        );

        // (3) delete all five
        for (const name of names) {
          await ks.deletePartition(name);
        }

        // (4) recreate them, then (5) verify each is empty and writable
        const fresh = await Promise.all(names.map((name) => ks.partition(name)));
        for (let idx = 0; idx < fresh.length; idx++) {
          const p = fresh[idx];

          // empty: data written this cycle (before the delete) must be gone
          expect(p.get(key(i, 0, names[idx]))).toBeNull();
          // empty: the sentinel written into the recreated partition last cycle
          // must also be gone
          expect(p.get(Buffer.from(`fresh-${idx}`))).toBeNull();

          // writable: a fresh write round-trips
          const fk = Buffer.from(`fresh-${idx}`);
          await p.insert(fk, Buffer.from([idx]));
          expect(p.get(fk)?.[0]).toBe(idx);
        }
      }
    } finally {
      await ks.close();
    }
  }, 120_000);
});
