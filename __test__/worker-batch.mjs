// Self-contained worker for the worker_threads regression suite. Performs a full
// open → insert-loop → persist → (optional read-back) → close lifecycle entirely
// INSIDE the worker, so concurrent workers exercise the native async path the way
// Typeberry's importer does. Posts exactly one result message, then exits.
import { parentPort, workerData } from 'node:worker_threads';
import { open, openReadonly } from '../index.js';

const { role, path, count, partition = 'data' } = workerData;
const KEY = (i) => Buffer.from('key-' + i);
const VAL = (i) => Buffer.from('val-' + i);

async function churn() {
  const ks = await open({ path });
  const p = await ks.partition(partition);
  for (let i = 0; i < count; i++) await p.insert(KEY(i), VAL(i));
  await ks.persist();
  let verified = 0;
  for (let i = 0; i < count; i++) {
    const v = p.get(KEY(i));
    if (v && v.toString() === 'val-' + i) verified++;
  }
  await ks.close();
  return { verified };
}

async function writer() {
  const ks = await open({ path });
  const p = await ks.partition(partition);
  for (let i = 0; i < count; i++) await p.insert(KEY(i), VAL(i));
  await ks.persist();
  await ks.close();
  return { wrote: count };
}

// Exercises insert_batch + remove + insert Tasks together under concurrency.
// Uses its own partition (passed via workerData) so concurrent workers on the
// shared engine don't race on the same keys — verification stays deterministic.
async function mixed() {
  const ks = await open({ path });
  const p = await ks.partition(partition);
  const entries = [];
  for (let i = 0; i < count; i++) entries.push({ key: KEY(i), value: VAL(i) });
  await p.insertBatch(entries); // batch write
  for (let i = 1; i < count; i += 2) await p.remove(KEY(i)); // remove odd keys
  for (let i = 1; i < count; i += 2) await p.insert(KEY(i), VAL(i)); // single-insert them back
  await ks.persist();
  let verified = 0;
  for (let i = 0; i < count; i++) {
    const v = p.get(KEY(i));
    if (v && v.toString() === 'val-' + i) verified++;
  }
  await ks.close();
  return { verified };
}

async function reader() {
  const ks = await openReadonly({ path });
  const p = await ks.partition(partition);
  // Poll briefly: a concurrent writer may still be flushing.
  let verified = 0;
  for (let attempt = 0; attempt < 500 && verified < count; attempt++) {
    verified = 0;
    for (let i = 0; i < count; i++) {
      const v = p.get(KEY(i));
      if (v && v.toString() === 'val-' + i) verified++;
    }
    if (verified < count) await new Promise((r) => setTimeout(r, 10));
  }
  await ks.close();
  return { verified };
}

// Opens, writes, reads, but deliberately does NOT call ks.close(). The N-API
// finalizer's auto-release must clean up the handle without crashing.
async function noClose() {
  const ks = await open({ path });
  const p = await ks.partition(partition);
  for (let i = 0; i < count; i++) await p.insert(KEY(i), VAL(i));
  await ks.persist();
  let verified = 0;
  for (let i = 0; i < count; i++) {
    const v = p.get(KEY(i));
    if (v && v.toString() === 'val-' + i) verified++;
  }
  return { verified };
}

const fn =
  role === 'writer'
    ? writer
    : role === 'reader'
      ? reader
      : role === 'mixed'
        ? mixed
        : role === 'no-close'
          ? noClose
          : churn;
fn()
  .then((r) => parentPort.postMessage({ ok: true, ...r }))
  .catch((e) => parentPort.postMessage({ ok: false, error: String((e && e.stack) || e) }));
