// Worker entry. Each worker opens its OWN handle to the shared engine using the
// plain `config` it was handed — all handles for the same path share one
// in-process engine. In your own project, import from '@fjall-js/fjall':
//
//   import { open, openReadonly } from '@fjall-js/fjall';
//
import { parentPort, workerData } from 'node:worker_threads';
import { open, openReadonly } from '../../index.js';

const { role, config, key, value } = workerData;
const k = (s) => Buffer.from(s);

if (role === 'writer') {
  const ks = await open(config); // the single writer
  const items = await ks.partition('items');
  await items.insert(k(key), k(value));
  await ks.persist(); // durability barrier (close() does NOT flush)
  parentPort.postMessage({ wrote: value });
  await ks.close(); // REQUIRED — frees native memory deterministically
} else {
  const ks = await openReadonly(config); // a reader: no write methods on the type
  const items = await ks.partition('items');
  const got = items.get(k(key));
  parentPort.postMessage({ read: got ? got.toString() : null });
  await ks.close();
}
