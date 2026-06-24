import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { open } from '../index.js';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-batch-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});
const k = (s: string) => Buffer.from(s, 'utf8');

describe('insertBatch', () => {
  it('commits many pairs atomically and reads them back', async () => {
    const ks = await open({ path: dir });
    const p = await ks.partition('b');
    await p.insertBatch([
      { key: k('a'), value: k('1') },
      { key: k('b'), value: k('2') },
      { key: k('c'), value: k('3') },
    ]);
    expect(p.get(k('a'))!.toString()).toBe('1');
    expect(p.get(k('c'))!.toString()).toBe('3');
    await ks.close();
  });
  it('empty batch is a no-op', async () => {
    const ks = await open({ path: dir });
    const p = await ks.partition('b');
    await expect(p.insertBatch([])).resolves.toBeUndefined();
    await ks.close();
  });
});

describe('cacheSizeBytes (first-opener-wins)', () => {
  it('opens with a custom cache size', async () => {
    const ks = await open({ path: dir, cacheSizeBytes: 8 * 1024 * 1024 });
    const p = await ks.partition('p');
    await p.insert(k('a'), k('1'));
    expect(p.get(k('a'))!.toString()).toBe('1');
    await ks.close();
  });
  it('a later differing cache size attaches anyway (first wins)', async () => {
    const ks1 = await open({ path: dir, cacheSizeBytes: 8 * 1024 * 1024 });
    const ks2 = await open({ path: dir, cacheSizeBytes: 64 * 1024 * 1024 }); // warns; attaches
    const p = await ks2.partition('p');
    await p.insert(k('z'), k('9'));
    expect(p.get(k('z'))!.toString()).toBe('9');
    await ks2.close();
    await ks1.close();
  });
});
