import { openReadonly } from '../index.js';

async function _assertNoWrites() {
  const ro = await openReadonly({ path: '/tmp/x' });
  const rp = await ro.partition('p');
  rp.get(Buffer.from('k'));
  // @ts-expect-error read-only partitions have no insert
  rp.insert(Buffer.from('k'), Buffer.from('v'));
  // @ts-expect-error read-only keyspaces have no persist
  ro.persist();
}
