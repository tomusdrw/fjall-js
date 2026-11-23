import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { open } from '../dist/index.js';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-leak-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe('leak loop', () => {
  it('many open→insert→persist→close cycles keep RSS bounded', async () => {
    const value = randomBytes(2 * 1024 * 1024); // pre-generated, reused
    const key = Buffer.from('k');
    const sample = () => process.memoryUsage().rss;

    // Warm up until the allocator's retained-memory high-water mark is reached,
    // so the baseline is taken on the plateau. On macOS 26 the system malloc
    // keeps ~120 MB of freed 2 MiB blocks resident and only plateaus after
    // ~75 cycles; measuring before that reads as "growth" even though it is
    // bounded. A genuine per-cycle leak (2 MiB × 60 = 120 MiB) still blows the
    // budget below.
    for (let i = 0; i < 80; i++) {
      const ks = await open({ path: dir });
      const p = await ks.partition('p');
      await p.insert(key, value);
      await ks.persist();
      await ks.close();
    }
    const baseline = sample();
    let peak = baseline;
    for (let i = 0; i < 60; i++) {
      const ks = await open({ path: dir });
      const p = await ks.partition('p');
      await p.insert(key, value);
      await ks.persist();
      await ks.close();
      peak = Math.max(peak, sample());
    }
    // Deterministic close frees native memory each cycle; allow generous slack.
    expect(peak - baseline).toBeLessThan(64 * 1024 * 1024);
  }, 120_000);
});
