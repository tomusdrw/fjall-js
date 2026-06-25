// Runnable worker_threads example for @fjall-js/fjall.
//
//   node examples/worker-threads/main.mjs
//
// The cross-worker pattern: build ONE config, hand the same plain object to every
// worker, and let each worker open (and close) its OWN handle. Every handle for
// the same path shares a single in-process engine, so a writer worker and reader
// workers see a coherent, live view of the database.
//
// Contract reminders (see the README "Concurrency, sharing & safety"):
//   - Pass the IDENTICAL path/config from every worker. Never transfer a live
//     Keyspace/Partition handle — only the plain config object.
//   - There is NO built-in double-open protection: one process per directory, and
//     bring your own lockfile if you need a hard guarantee.
//   - Every open() must be matched by a close().
import { Worker } from 'node:worker_threads';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = fileURLToPath(new URL('.', import.meta.url));
const workerEntry = resolve(here, 'db-worker.mjs');

function runWorker(workerData) {
  return new Promise((res, rej) => {
    const w = new Worker(workerEntry, { workerData });
    w.once('message', res);
    w.once('error', rej);
  });
}

const dir = await mkdtemp(join(tmpdir(), 'fjall-example-'));
try {
  // ONE config, structured-clone-transferred to each worker.
  const config = { path: dir, cacheSizeBytes: 64 * 1024 * 1024 };

  const w = await runWorker({
    role: 'writer',
    config,
    key: 'greeting',
    value: 'hello from the writer',
  });
  console.log('writer wrote: ', w.wrote);

  const r = await runWorker({ role: 'reader', config, key: 'greeting' });
  console.log('reader saw:   ', r.read); // -> "hello from the writer"
} finally {
  await rm(dir, { recursive: true, force: true });
}
