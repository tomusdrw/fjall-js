import { parentPort, workerData } from 'node:worker_threads';
import { open, openReadonly } from '../index.js';

const { role, path, key, value } = workerData;

if (role === 'writer') {
  const ks = await open({ path });
  const p = await ks.partition('shared');
  await p.insert(Buffer.from(key), Buffer.from(value));
  await ks.persist();
  parentPort.postMessage({ ok: true });
  await ks.close();
} else {
  const ks = await openReadonly({ path });
  const p = await ks.partition('shared');
  // poll briefly for the writer's value to appear (live, same-process engine)
  let seen = null;
  for (let i = 0; i < 100 && seen === null; i++) {
    const v = p.get(Buffer.from(key));
    if (v) seen = v.toString('utf8');
    else await new Promise((r) => setTimeout(r, 10));
  }
  parentPort.postMessage({ ok: true, seen });
  await ks.close();
}
