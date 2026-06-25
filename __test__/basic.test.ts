import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';

import { open, Keyspace, Partition } from '../index.js';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-test-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});
const k = (s: string) => Buffer.from(s, 'utf8');

describe('open / close', () => {
  it('opens a keyspace at a fresh directory', async () => {
    const ks = await open({ path: dir });
    expect(ks).toBeDefined();
    await ks.close();
  });

  it('rejects opening on a regular file path', async () => {
    const filePath = join(dir, 'a-file');
    await writeFile(filePath, 'not a directory');
    await expect(open({ path: filePath })).rejects.toThrow();
  });

  it('persist + reopen sees the data', async () => {
    const ks1 = await open({ path: dir });
    const p1 = await ks1.partition('p');
    await p1.insert(k('a'), k('1'));
    await ks1.persist();
    await ks1.close();

    const ks2 = await open({ path: dir });
    const p2 = await ks2.partition('p');
    expect(p2.get(k('a'))?.toString('utf8')).toBe('1');
    await ks2.close();
  });
});

describe('partition CRUD', () => {
  let ks: Keyspace;
  let p: Partition;
  beforeEach(async () => {
    ks = await open({ path: dir });
    p = await ks.partition('items');
  });
  afterEach(async () => {
    await ks.close();
  });

  it('insert then get returns the value', async () => {
    await p.insert(k('hello'), k('world'));
    expect(p.get(k('hello'))!.toString('utf8')).toBe('world');
  });
  it('get of a missing key returns null', () => {
    expect(p.get(k('nope'))).toBeNull();
  });
  it('remove then get returns null', async () => {
    await p.insert(k('x'), k('y'));
    await p.remove(k('x'));
    expect(p.get(k('x'))).toBeNull();
  });
  it('round-trips random byte values up to ~1 MiB', async () => {
    for (const size of [0, 1, 7, 64, 4096, 65536, 1024 * 1024]) {
      const key = randomBytes(16);
      const value = randomBytes(size);
      await p.insert(key, value);
      expect(p.get(key)!.equals(value)).toBe(true);
    }
  });
  it('overwrites existing value on second insert', async () => {
    const ks = await open({ path: dir });
    const p = await ks.partition('p');
    await p.insert(k('k'), k('v1'));
    await p.insert(k('k'), k('v2'));
    expect(p.get(k('k'))!.toString('utf8')).toBe('v2');
    await ks.close();
  });
  it('remove of a missing key is a no-op', async () => {
    const ks = await open({ path: dir });
    const p = await ks.partition('p');
    await expect(p.remove(k('ghost'))).resolves.toBeUndefined();
    await ks.close();
  });
});

describe('multi-partition isolation', () => {
  it('writes to A do not appear in B', async () => {
    const ks = await open({ path: dir });
    const a = await ks.partition('A');
    const b = await ks.partition('B');
    await a.insert(k('shared-key'), k('from-A'));
    expect(a.get(k('shared-key'))!.toString('utf8')).toBe('from-A');
    expect(b.get(k('shared-key'))).toBeNull();
    await b.insert(k('shared-key'), k('from-B'));
    expect(a.get(k('shared-key'))!.toString('utf8')).toBe('from-A');
    expect(b.get(k('shared-key'))!.toString('utf8')).toBe('from-B');
    await ks.close();
  });

  it('opening the same partition twice returns the same data', async () => {
    const ks = await open({ path: dir });
    const a1 = await ks.partition('shared');
    await a1.insert(k('k'), k('v'));
    const a2 = await ks.partition('shared');
    expect(a2.get(k('k'))!.toString('utf8')).toBe('v');
    await ks.close();
  });
});

describe('persist', () => {
  it('accepts all three persist modes', async () => {
    const ks = await open({ path: dir });
    const p = await ks.partition('p');
    await p.insert(k('a'), k('1'));
    await ks.persist('buffer');
    await ks.persist('sync-data');
    await ks.persist('sync-all');
    await ks.close();
  });
  it('rejects an invalid persist mode', async () => {
    const ks = await open({ path: dir });
    await expect(ks.persist('nope' as never)).rejects.toThrow(/invalid persist mode/);
    await ks.close();
  });
});

describe('use after close', () => {
  it('operations on a closed keyspace reject', async () => {
    const ks = await open({ path: dir });
    const p = await ks.partition('p');
    await ks.close();
    await expect(ks.partition('q')).rejects.toThrow(/closed/);
    await expect(ks.persist()).rejects.toThrow(/closed/);
    await expect(p.insert(k('x'), k('y'))).rejects.toThrow(/closed/);
    await expect(p.remove(k('x'))).rejects.toThrow(/closed/);
    expect(() => p.get(k('x'))).toThrow(/closed/);
  });
  it('double close is a no-op', async () => {
    const ks = await open({ path: dir });
    await ks.close();
    await expect(ks.close()).resolves.toBeUndefined();
  });
  it('partial close keeps a second handle alive', async () => {
    const ks1 = await open({ path: dir });
    const p1 = await ks1.partition('p');
    await p1.insert(k('a'), k('1'));
    const ks2 = await open({ path: dir });
    const p2 = await ks2.partition('p');
    await ks1.close(); // engine survives for ks2
    expect(p2.get(k('a'))!.toString('utf8')).toBe('1');
    await ks2.close();
  });
});
