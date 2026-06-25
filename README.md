# @fjall-js/fjall

TypeScript / Node.js bindings for the [fjall](https://github.com/fjall-rs/fjall) Rust LSM-tree storage engine.

[![npm version](https://img.shields.io/npm/v/@fjall-js/fjall.svg)](https://www.npmjs.com/package/@fjall-js/fjall)
[![CI](https://github.com/tomusdrw/fjall-js/actions/workflows/CI.yml/badge.svg)](https://github.com/tomusdrw/fjall-js/actions/workflows/CI.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

> **Status:** v0.3 — one shared engine across Node `worker_threads`, a read-only handle type, and deterministic refcounted `close()`. **Read [Concurrency, sharing & safety](#concurrency-sharing--safety) before using it across threads or processes** — there is an operational contract you must honor (notably: matched `close()`s, and your own double-open protection).

## Install

```sh
npm install @fjall-js/fjall
```

The package ships pre-built native binaries via `optionalDependencies`. npm picks the right one for the host automatically; there is no postinstall script and no download server. It covers **macOS arm64** and **Linux x64 (glibc)** — see [Supported platforms](#supported-platforms).

## Quickstart

```ts
import { open } from '@fjall-js/fjall';

const ks = await open({ path: './mydata' });
const users = await ks.partition('users');

await users.insert(Buffer.from('alice'), Buffer.from('{"age":30}'));
await users.insert(Buffer.from('bob'), Buffer.from('{"age":42}'));

const alice = users.get(Buffer.from('alice')); // Buffer
const carol = users.get(Buffer.from('carol')); // null

console.log(alice?.toString(), carol);

await ks.persist(); // durability barrier — see Persistence
await ks.close(); // REQUIRED — frees native memory deterministically
```

The example uses top-level `await`, so it must run as ESM (`"type": "module"` or a `.mjs` file). For CommonJS, wrap in an async IIFE:

```js
(async () => {
  const { open } = require('@fjall-js/fjall');
  // ...
})();
```

## Concurrency, sharing & safety

This is the operational contract. Read it before sharing a database across threads or processes.

**One shared engine per path.** Every `open` / `openReadonly` of the **same directory path** within a process attaches to **one** shared in-process engine — one LSM tree, one bounded block cache. This is what lets Node `worker_threads` share a coherent, live view: a write committed on any handle is immediately visible to `get` on any other handle of the same engine, with **no `persist()` required** (`persist` is for on-disk durability, not cross-handle visibility).

**Pass the identical path from every worker.** The sharing key is the canonical filesystem path. Build your `DatabaseConfig` **once** and hand the same plain object to every worker — it is structured-clone-able data. **Never transfer a live `Keyspace`/`Partition` handle** between workers (it wraps a thread-bound native pointer); transfer the config and let each worker open its own handle:

```ts
// main thread
const config = { path: '/data/db', cacheSizeBytes: 256 * 1024 * 1024 };
for (const w of workers) w.postMessage({ config });

// each worker
const ks = await open(config); // the one writer
const ro = await openReadonly(config); // readers
```

A complete, runnable version of this pattern lives in [`examples/worker-threads/`](./examples/worker-threads/) — run it with `node examples/worker-threads/main.mjs`.

**⚠️ No double-open protection — you must provide your own.** fjall — and therefore this wrapper — takes **no** OS directory lock. Opening the **same directory twice via different path spellings**, or from a **second process**, is **unguarded** and **can corrupt the database**. The wrapper deliberately mirrors fjall's own behavior. **If you need a hard guarantee against double-opening a directory, set up your own mechanism — e.g. an OS advisory lockfile on the data directory — before calling `open`/`openReadonly`.** Operating rules: **one process per directory; one path spelling everywhere.**

**At most one writer per path.** Keeping to a single writable handle (`open`) per path is the consumer's responsibility. A second writable handle is **warned** (to stderr), not prevented — open every reader with `openReadonly`.

**Every `open` must be matched by exactly one `close`.** `close()` frees the shared engine's native memory (memtables, write buffer, block cache, partition handles) **deterministically on the last handle**. A handle dropped **without** `close()` **leaks its native memory for the process lifetime** — a warning is printed to stderr, and GC does **not** reclaim it. Call `close()` on every teardown path, including error and shutdown paths.

**Async ops run on libuv's thread pool.** Every async method (`open`, `partition`, `insert`, `insertBatch`, `remove`, `persist`, `close`) does its blocking work on Node's libuv thread pool — `UV_THREADPOOL_SIZE`, **default 4**, shared process-wide with `fs`/`dns`/`crypto`. This is what makes it safe across `worker_threads` (each promise resolves on its own thread, with no shared async runtime). For **write-heavy or highly concurrent** workloads, raise `UV_THREADPOOL_SIZE` (set it before the process starts; max 1024) so DB work doesn't starve — or get starved by — other libuv I/O. `get` is synchronous and runs on the calling thread regardless.

## API

### `open(config: DatabaseConfig): Promise<Keyspace>`

Open (or create, if missing) the **writable** handle to the shared engine for `config.path`. The directory is created if it does not exist. Throws if `config.path` points to a regular file.

### `openReadonly(config: DatabaseConfig): Promise<ReadonlyKeyspace>`

Open a **read-only** handle. `ReadonlyKeyspace` / `ReadonlyPartition` have **no** write methods (`insert` / `remove` / `insertBatch`) and **no** `persist` — their absence is enforced by the type system at compile time, not by throwing at runtime. A read-only opener may legitimately be the **first** opener and create the engine (fjall always opens read-write underneath; "read-only" is a wrapper-surface restriction).

`DatabaseConfig`:

| Field             | Meaning                                                                                                                                                                                                |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `path`            | Data directory. **This is the sharing key** — pass the identical path from every worker.                                                                                                               |
| `cacheSizeBytes?` | Block-cache capacity in bytes, shared across the engine's partitions; bounds the resident set. **Engine-level: fixed by the _first_ opener** of a path; a later differing value is warned and ignored. |

All engine-level config is fixed by whoever opens first (which may be a reader), so pass the same full config from every worker.

### `Keyspace.partition(name)` / `ReadonlyKeyspace.partition(name)`

Open or create a named partition (the fjall equivalent of a column family / bucket) within the engine. Partitions are isolated from each other — writes to one are not visible from another. A partition is opened once per engine and shared by every handle.

### `Keyspace.persist(mode?: PersistMode): Promise<void>`

Flush in-memory writes to disk — a durability barrier. See [Persistence](#persistence). Read-only handles have no `persist` (readers don't drive the shared journal).

### `Keyspace.close()` / `ReadonlyKeyspace.close()`

Drop **this** handle. The shared engine is freed **deterministically on the last `close()`** — its native memory is released eagerly, rather than lingering until the JS objects are garbage-collected, which keeps RSS flat across repeated open/close cycles. **`close()` does not flush to disk** — call `persist()` first if you need the latest writes durable (see [Persistence](#persistence)). After `close()`, this handle and any partition opened from it are no longer usable.

### `Partition.get(key: Uint8Array): Buffer | null`

**Synchronous.** Returns the value bytes for `key`, or `null` if absent. Reflects writes committed by **any** handle of the engine, immediately. `null` (not `undefined`) makes a miss explicit.

`get` is sync because LSM point reads are typically cheap (memtable / block-cache hits) and the JS-side cost of an async hop tends to dominate. Cold disk reads still block the calling thread.

### `Partition.insert(key, value): Promise<void>`

Insert or overwrite a key/value pair. Runs on a worker thread. (Writable handles only — absent from `ReadonlyPartition`.)

### `Partition.remove(key): Promise<void>`

Remove a key. A subsequent `get` returns `null`. Idempotent — removing a missing key is not an error.

### `Partition.insertBatch(entries: BatchEntry[]): Promise<void>`

Atomically insert many key/value pairs in a single fjall write batch — one journal write and one worker-thread round-trip instead of one per pair. Like `insert`, durability is deferred: call `persist()` to flush. An empty list is a no-op.

```ts
await partition.insertBatch([
  { key: Buffer.from('a'), value: Buffer.from('1') },
  { key: Buffer.from('b'), value: Buffer.from('2') },
]);
```

### Types

```ts
type Key = Uint8Array;
type Value = Uint8Array;
type PersistMode = 'buffer' | 'sync-data' | 'sync-all';

export interface DatabaseConfig {
  path: string;
  cacheSizeBytes?: number;
}

export interface BatchEntry {
  key: Key;
  value: Value;
}

export function open(config: DatabaseConfig): Promise<Keyspace>;
export function openReadonly(config: DatabaseConfig): Promise<ReadonlyKeyspace>;

export interface Keyspace {
  partition(name: string): Promise<Partition>;
  persist(mode?: PersistMode): Promise<void>;
  close(): Promise<void>;
}

export interface Partition {
  get(key: Key): Buffer | null;
  insert(key: Key, value: Value): Promise<void>;
  remove(key: Key): Promise<void>;
  insertBatch(entries: BatchEntry[]): Promise<void>;
}

export interface ReadonlyKeyspace {
  partition(name: string): Promise<ReadonlyPartition>;
  close(): Promise<void>;
}

export interface ReadonlyPartition {
  get(key: Key): Buffer | null;
}
```

`Buffer` is a `Uint8Array` subclass — anywhere a `Uint8Array` is accepted, a `Buffer` works too, and reads return `Buffer` for convenience (`.toString()`, slicing, etc.). Because the read-only types are a structural subset of the writable ones, a `Keyspace` is usable wherever a `ReadonlyKeyspace` is expected.

## Persistence

`insert`, `remove`, and `insertBatch` write through fjall's journal and memtable but do **not** by themselves guarantee the bytes are durable on disk, and **`close()` does not flush**. `persist()` is the **only** durability barrier — call it when you need one.

`persist(mode)` accepts three values, matching fjall's own:

| Mode          | Meaning                                                                         |
| ------------- | ------------------------------------------------------------------------------- |
| `'buffer'`    | Flush in-memory buffers to the OS. Survives process crash, not OS / power loss. |
| `'sync-data'` | `fdatasync` the journal — survives OS / power loss. **Default.**                |
| `'sync-all'`  | `fsync` the journal — also syncs file metadata. Strongest, slowest.             |

The default (no argument) is `'sync-data'`, matching the upstream fjall default. Cross-handle **visibility** needs no `persist` (it's in-memory via the shared engine); only on-disk **durability against power loss** does.

Typical usage: write a number of values (ideally via a single `insertBatch`), then call `persist()` once at the end. The writer is responsible for persisting after a logically-complete unit of work.

## Migrating from 0.2.x

0.3 is a breaking change. The mechanical edits:

| 0.2.x                               | 0.3.0                                                         |
| ----------------------------------- | ------------------------------------------------------------- |
| `open('./db')`                      | `open({ path: './db' })`                                      |
| `open('./db', { cacheSizeBytes })`  | `open({ path: './db', cacheSizeBytes })`                      |
| `open('./db', { ephemeral: true })` | `open({ path: './db' })` — `ephemeral` is removed (see below) |

Behavioral changes to check:

- **`close()` no longer flushes to disk.** Previously a non-`ephemeral` `close()` did a `sync-all`. Durability is now **persist-only**: call `await ks.persist()` before `close()` wherever you relied on close-time durability.
- **`ephemeral` is gone.** It only controlled that close-time flush, which no longer happens — just drop the option. (Cross-handle visibility never needed it.)
- **Readers should switch to `openReadonly(config)`.** It returns a type with no write methods and lets multiple workers share one engine safely. Keep at most one writable `open()` per path.
- **Multiple opens of one path now share a single engine** instead of building independent ones — read [Concurrency, sharing & safety](#concurrency-sharing--safety): pass the identical path from every worker, and provide your own double-open lock if you run more than one process.

See the [CHANGELOG](./CHANGELOG.md) for the full list.

## Supported platforms

Ships pre-built binaries for:

- **macOS arm64** (`darwin-arm64`)
- **Linux x64, glibc** (`linux-x64-gnu`)

Other targets (Linux musl, Linux arm64, macOS x64, Windows) are not yet built — there is no source-build fallback, so `npm install` on an unsupported platform succeeds but `require('@fjall-js/fjall')` throws a clear error at load time. If you need another platform, please open an issue.

## Naming

We mirror the upstream [fjall](https://github.com/fjall-rs/fjall) vocabulary on purpose:

- **Keyspace** — the top-level on-disk store (a directory).
- **Partition** — a named bucket within a keyspace (analogous to a column family).

If you've read the fjall docs, the concepts and method names (`insert`, `remove`, `persist`) should be familiar.

## Roadmap / out of scope

The following are **intentionally not included** yet. They are not bugs; they are deferred.

- Range queries, iterators, cursors
- Transactions (`TxKeyspace` / `WriteTx`) — though `insertBatch` provides atomic multi-key writes
- Snapshots
- Compression and compactor configuration (the block-cache _size_ is configurable via `cacheSizeBytes`)
- String / JSON convenience helpers (callers pass raw bytes)
- Built-in cross-process / double-open protection (use your own lockfile — see [Concurrency, sharing & safety](#concurrency-sharing--safety))
- Linux musl, Linux arm64, macOS x64, Windows binaries
- Source-build fallback when no prebuilt binary is available

If your use case depends on one of these, please open an issue — prioritization is driven by demand.

## A note on the unscoped `fjall-js` package

The unscoped [`fjall-js`](https://www.npmjs.com/package/fjall-js) package on npm is a deliberate **squat** owned by this project. It does nothing useful: it logs an error and throws on `require`. This is to prevent confusion and typosquatting. **Always install `@fjall-js/fjall`.**

The unscoped [`fjall`](https://www.npmjs.com/package/fjall) package on npm is **unrelated** to this project and is owned by someone else — do not assume it is this wrapper.

## License

Dual-licensed under either of

- MIT license ([LICENSE-MIT](./LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))

at your option.
