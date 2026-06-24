import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { open, openReadonly } from '../index.js';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-ro-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});
const k = (s: string) => Buffer.from(s, 'utf8');

describe('read-only surface', () => {
  it('a reader that opens first creates the engine; a later writer attaches', async () => {
    const ro = await openReadonly({ path: dir }); // first opener creates the engine
    const rp = await ro.partition('items');
    expect(rp.get(k('a'))).toBeNull();

    const rw = await open({ path: dir }); // attaches to the reader-created engine
    const wp = await rw.partition('items');
    await wp.insert(k('a'), k('1'));

    // Live visibility on the reader handle, no persist.
    expect(rp.get(k('a'))!.toString('utf8')).toBe('1');

    await rw.close();
    await ro.close();
  });

  it('ReadonlyPartition has no write methods at runtime', async () => {
    const ro = await openReadonly({ path: dir });
    const rp = await ro.partition('items');
    expect((rp as unknown as { insert?: unknown }).insert).toBeUndefined();
    await ro.close();
  });
});
