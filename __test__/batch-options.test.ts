import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { open, type Keyspace, type Partition } from '../index.js';

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-batch-'));
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const k = (s: string) => Buffer.from(s, 'utf8');

describe('Partition.insertBatch', () => {
  let ks: Keyspace;
  let p: Partition;

  beforeEach(async () => {
    ks = await open(dir);
    p = await ks.partition('items');
  });

  afterEach(async () => {
    await ks.close();
  });

  it('writes every pair in the batch', async () => {
    await p.insertBatch([
      { key: k('a'), value: k('1') },
      { key: k('b'), value: k('2') },
      { key: k('c'), value: k('3') },
    ]);

    expect(p.get(k('a'))?.toString('utf8')).toBe('1');
    expect(p.get(k('b'))?.toString('utf8')).toBe('2');
    expect(p.get(k('c'))?.toString('utf8')).toBe('3');
  });

  it('is a no-op for an empty batch', async () => {
    await expect(p.insertBatch([])).resolves.toBeUndefined();
  });

  it('last write wins for a duplicate key within a batch', async () => {
    await p.insertBatch([
      { key: k('dup'), value: k('first') },
      { key: k('dup'), value: k('second') },
    ]);
    expect(p.get(k('dup'))?.toString('utf8')).toBe('second');
  });

  it('overwrites values written by a prior insert', async () => {
    await p.insert(k('x'), k('old'));
    await p.insertBatch([{ key: k('x'), value: k('new') }]);
    expect(p.get(k('x'))?.toString('utf8')).toBe('new');
  });
});

describe('open options', () => {
  it('opens with no options (backwards compatible)', async () => {
    const ks = await open(dir);
    const p = await ks.partition('p');
    await p.insert(k('a'), k('1'));
    expect(p.get(k('a'))?.toString('utf8')).toBe('1');
    await ks.close();
  });

  it('opens with an explicit empty options object', async () => {
    const ks = await open(dir, {});
    await ks.close();
  });

  it('honours a cache size', async () => {
    const ks = await open(dir, { cacheSizeBytes: 8 * 1024 * 1024 });
    const p = await ks.partition('p');
    await p.insert(k('a'), k('1'));
    expect(p.get(k('a'))?.toString('utf8')).toBe('1');
    await ks.close();
  });

  it('reads back writes from an ephemeral keyspace and closes without error', async () => {
    const ks = await open(dir, { ephemeral: true });
    const p = await ks.partition('p');
    await p.insertBatch([
      { key: k('a'), value: k('1') },
      { key: k('b'), value: k('2') },
    ]);
    expect(p.get(k('a'))?.toString('utf8')).toBe('1');
    expect(p.get(k('b'))?.toString('utf8')).toBe('2');
    // close() must skip the sync-all flush for ephemeral keyspaces, but still
    // resolve and release the handle.
    await expect(ks.close()).resolves.toBeUndefined();
  });
});
