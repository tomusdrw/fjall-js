// Command-driven worker for the lifecycle / shared-engine regression tests. The
// main thread drives a long-lived keyspace step by step (open, partition, insert,
// get, persist, close) so it can interleave two workers deterministically. Each
// request carries an `id`; the reply echoes it.
import { parentPort } from 'node:worker_threads';
import * as fjall from '../index.js';

let ks = null;
const parts = new Map();
const b = (s) => Buffer.from(String(s));

parentPort.on('message', async ({ id, cmd, args }) => {
  try {
    let result = null;
    switch (cmd) {
      case 'open':
        ks = await fjall.open(args);
        break;
      case 'openReadonly':
        ks = await fjall.openReadonly(args);
        break;
      case 'partition':
        parts.set(args.name, await ks.partition(args.name));
        break;
      case 'insert':
        await parts.get(args.name).insert(b(args.key), b(args.value));
        break;
      case 'remove':
        await parts.get(args.name).remove(b(args.key));
        break;
      case 'persist':
        await ks.persist(args && args.mode);
        break;
      case 'get': {
        const v = parts.get(args.name).get(b(args.key));
        result = v ? v.toString() : null;
        break;
      }
      case 'close':
        await ks.close();
        ks = null;
        parts.clear();
        break;
      default:
        throw new Error('unknown cmd: ' + cmd);
    }
    parentPort.postMessage({ id, ok: true, result });
  } catch (e) {
    parentPort.postMessage({ id, ok: false, error: String((e && e.stack) || e) });
  }
});

parentPort.postMessage({ ready: true });
