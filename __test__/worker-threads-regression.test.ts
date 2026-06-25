// Regression suite for the native worker_threads crash (EXC_BAD_ACCESS in the
// napi async/deferred path) that 0.3.1 hit when many concurrent worker_threads
// Envs issued async ops. Before the fix (async fn -> global tokio runtime +
// cross-thread ThreadSafeFunction deferred) the concurrent cases below crashed
// the whole process; with the napi `Task` (libuv per-Env async work) path they
// pass. See src/lib.rs and .context/ROOT_CAUSE.md.
import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Worker } from 'node:worker_threads';

let dir: string;
beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'fjall-wtr-'));
});
afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

const batchEntry = resolve(__dirname, 'worker-batch.mjs');
const cmdEntry = resolve(__dirname, 'worker-cmd.mjs');

// Run one self-contained batch worker. Rejects if the worker errors, exits
// non-zero, or reports a failure — and (critically) if the native code crashes
// the process, the whole test run dies, which is itself the regression signal.
function runBatch(workerData: Record<string, unknown>): Promise<any> {
  return new Promise((res, rej) => {
    const w = new Worker(batchEntry, { workerData });
    let msg: any;
    w.once('message', (m) => {
      msg = m;
    });
    w.once('error', rej);
    w.once('exit', (code) => {
      if (code !== 0) rej(new Error(`worker exited with code ${code}`));
      else if (!msg) rej(new Error('worker exited without a result message'));
      else if (!msg.ok) rej(new Error('worker reported failure: ' + msg.error));
      else res(msg);
    });
  });
}

// Thin RPC wrapper around the command-driven worker for step-by-step lifecycle
// orchestration across two real worker threads.
class WorkerClient {
  private w: Worker;
  private seq = 0;
  private pending = new Map<number, { res: (v: any) => void; rej: (e: any) => void }>();
  ready: Promise<void>;

  constructor() {
    this.w = new Worker(cmdEntry);
    this.ready = new Promise<void>((resolveReady) => {
      this.w.on('message', (m: any) => {
        if (m.ready) {
          resolveReady();
          return;
        }
        const p = this.pending.get(m.id);
        if (!p) return;
        this.pending.delete(m.id);
        if (m.ok) p.res(m.result);
        else p.rej(new Error(m.error));
      });
    });
    this.w.on('error', (e) => {
      for (const p of this.pending.values()) p.rej(e);
      this.pending.clear();
    });
  }

  req(cmd: string, args?: any): Promise<any> {
    const id = ++this.seq;
    return new Promise((res, rej) => {
      this.pending.set(id, { res, rej });
      this.w.postMessage({ id, cmd, args });
    });
  }

  async terminate(): Promise<void> {
    await this.w.terminate();
  }
}

describe('worker_threads regression', () => {
  // (1) Shared registry / concurrent open — the core crash regression. Many
  // worker threads open the SAME path and each does a full write/persist/close
  // lifecycle, repeated for several rounds. Pre-fix this reliably SIGSEGVs.
  it('many concurrent workers churn the same path without crashing', async () => {
    const ROUNDS = 8;
    const CONC = 6;
    const COUNT = 150;
    for (let round = 0; round < ROUNDS; round++) {
      const results = await Promise.all(
        Array.from({ length: CONC }, () => runBatch({ role: 'churn', path: dir, count: COUNT })),
      );
      for (const r of results) expect(r.verified).toBe(COUNT);
    }
  }, 90_000);

  // (1b) Shared-engine proof across two worker threads with NO main-thread
  // handle: an UN-persisted write by worker A is visible to worker B. Only one
  // shared in-process engine can make that true (a per-worker engine reading
  // disk could not, since nothing was persisted).
  it('an un-persisted write in one worker is visible to another worker', async () => {
    const A = new WorkerClient();
    const B = new WorkerClient();
    await Promise.all([A.ready, B.ready]);
    try {
      await A.req('open', { path: dir }); // A creates the engine
      await A.req('partition', { name: 'data' });
      await B.req('openReadonly', { path: dir }); // B attaches to the SAME engine
      await B.req('partition', { name: 'data' });

      await A.req('insert', { name: 'data', key: 'live', value: 'yes' }); // no persist
      expect(await B.req('get', { name: 'data', key: 'live' })).toBe('yes');

      await A.req('close');
      await B.req('close');
    } finally {
      await A.terminate();
      await B.terminate();
    }
  }, 20_000);

  // (2) Concurrent access: one worker writes+persists many keys, then two
  // readers verify all of them concurrently.
  it('one writer then two concurrent readers see every persisted key', async () => {
    const COUNT = 300;
    const w = await runBatch({ role: 'writer', path: dir, count: COUNT });
    expect(w.wrote).toBe(COUNT);
    const [r1, r2] = await Promise.all([
      runBatch({ role: 'reader', path: dir, count: COUNT }),
      runBatch({ role: 'reader', path: dir, count: COUNT }),
    ]);
    expect(r1.verified).toBe(COUNT);
    expect(r2.verified).toBe(COUNT);
  }, 30_000);

  // (2b) Mixed Task types under concurrency: several workers each run
  // insert_batch + remove + insert + get on the same shared engine (distinct
  // partitions to keep verification deterministic). Exercises every async Task
  // variant concurrently across worker threads, not just insert.
  it('concurrent workers mixing insert_batch/remove/insert do not crash', async () => {
    const CONC = 5;
    const COUNT = 120;
    const results = await Promise.all(
      Array.from({ length: CONC }, (_, i) =>
        runBatch({ role: 'mixed', path: dir, count: COUNT, partition: `w${i}` }),
      ),
    );
    for (const r of results) expect(r.verified).toBe(COUNT);
  }, 30_000);

  // (3) Lifecycle: two workers open the same path; A closes but B keeps reading;
  // B closes (last ref → engine torn down); reopening creates a fresh engine and
  // the previously-persisted data is still durable.
  it('refcounted close across two workers; reopen stays durable', async () => {
    const A = new WorkerClient();
    const B = new WorkerClient();
    await Promise.all([A.ready, B.ready]);
    try {
      await A.req('open', { path: dir });
      await A.req('partition', { name: 'data' });
      await A.req('insert', { name: 'data', key: 'k', value: 'v' });
      await A.req('persist');

      await B.req('openReadonly', { path: dir });
      await B.req('partition', { name: 'data' });
      expect(await B.req('get', { name: 'data', key: 'k' })).toBe('v');

      // A closes; engine survives because B still holds a reference.
      await A.req('close');
      expect(await B.req('get', { name: 'data', key: 'k' })).toBe('v');

      // B closes; that is the last reference → engine is dropped.
      await B.req('close');

      // Reopen: a fresh engine reads the durable, previously-persisted data.
      await A.req('open', { path: dir });
      await A.req('partition', { name: 'data' });
      expect(await A.req('get', { name: 'data', key: 'k' })).toBe('v');
      await A.req('close');
    } finally {
      await A.terminate();
      await B.terminate();
    }
  }, 20_000);

  // (4) Stress: repeated generations of concurrent worker churn. The engine is
  // process-global, so its native memory lives in this (main) process; a leaked
  // engine (close not freeing) would balloon RSS far past the bound.
  it('repeated concurrent worker churn keeps RSS bounded', async () => {
    const sample = () => process.memoryUsage().rss;
    for (let i = 0; i < 2; i++) await runBatch({ role: 'churn', path: dir, count: 100 }); // warmup
    const baseline = sample();
    let peak = baseline;
    for (let gen = 0; gen < 15; gen++) {
      await Promise.all(
        Array.from({ length: 4 }, () => runBatch({ role: 'churn', path: dir, count: 100 })),
      );
      peak = Math.max(peak, sample());
    }
    // 15 generations × 4 workers × 100 keys all close deterministically; a real
    // engine leak would blow well past this. Generous slack for worker-thread
    // isolate churn noise.
    expect(peak - baseline).toBeLessThan(200 * 1024 * 1024);
  }, 120_000);

  // (5) Env isolation: workers open/use/close and then EXIT, repeatedly. Each
  // spawn creates and tears down a worker N-API Env. A reference bound to the
  // wrong/exited Env (the original bug class) would surface as a use-after-free.
  it('workers open/use/close then exit repeatedly without a napi crash', async () => {
    for (let i = 0; i < 25; i++) {
      const r = await runBatch({ role: 'churn', path: dir, count: 50 });
      expect(r.verified).toBe(50);
    }
    const burst = await Promise.all(
      Array.from({ length: 8 }, () => runBatch({ role: 'churn', path: dir, count: 50 })),
    );
    for (const r of burst) expect(r.verified).toBe(50);
  }, 60_000);
});
