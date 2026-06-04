# @fjall-js/fjall

TypeScript / Node.js bindings for the [fjall](https://github.com/fjall-rs/fjall) Rust LSM-tree storage engine.

[![npm version](https://img.shields.io/npm/v/@fjall-js/fjall.svg)](https://www.npmjs.com/package/@fjall-js/fjall)
[![CI](https://github.com/tomusdrw/fjall-js/actions/workflows/CI.yml/badge.svg)](https://github.com/tomusdrw/fjall-js/actions/workflows/CI.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

> **Status:** v0.1, minimal API surface — see [Roadmap](#roadmap--out-of-scope) for what's not yet included.

## Install

```sh
npm install @fjall-js/fjall
```

The package ships pre-built native binaries via `optionalDependencies`. npm picks the right one for the host automatically; there is no postinstall script and no download server. v0.1 covers **macOS arm64** and **Linux x64 (glibc)** — see [Supported platforms](#supported-platforms).

## Quickstart

```ts
import { open } from '@fjall-js/fjall';

const ks = await open('./mydata');
const users = await ks.partition('users');

await users.insert(Buffer.from('alice'), Buffer.from('{"age":30}'));
await users.insert(Buffer.from('bob'), Buffer.from('{"age":42}'));

const alice = users.get(Buffer.from('alice'));
const bob = users.get(Buffer.from('bob'));
const carol = users.get(Buffer.from('carol')); // null

console.log(alice?.toString(), bob?.toString(), carol);

await ks.persist();
await ks.close();
```

The example uses top-level `await`, so it must run as ESM (`"type": "module"` or a `.mjs` file). For CommonJS, wrap in an async IIFE:

```js
(async () => {
  const { open } = require('@fjall-js/fjall');
  // ...
})();
```

## API

### `open(path: string): Promise<Keyspace>`

Open (or create) a keyspace at the given directory path. The directory is created if it does not exist. Throws if `path` points to a regular file or to a directory already locked by another keyspace.

### `Keyspace.partition(name: string): Promise<Partition>`

Open or create a named partition (the fjall equivalent of a column family / bucket) within the keyspace. Partitions are isolated from each other — writes to one are not visible from another.

### `Keyspace.persist(mode?: PersistMode): Promise<void>`

Flush in-memory writes to disk. See [Persistence](#persistence) below.

### `Keyspace.close(): Promise<void>`

Close the keyspace and release the directory lock. After `close()`, the keyspace and all of its partitions are no longer usable.

### `Partition.get(key: Uint8Array): Buffer | null`

**Synchronous.** Returns the value bytes for `key`, or `null` if the key is not present. `null` (not `undefined`) is used to make a miss explicit.

`get` is sync because LSM point reads are typically cheap (memtable / block-cache hits), and the JS-side overhead of scheduling an async hop tends to dominate. Cold disk reads still block the calling thread — keep that in mind if you read large keys under load.

### `Partition.insert(key: Uint8Array, value: Uint8Array): Promise<void>`

Insert or overwrite a key/value pair. Runs on a tokio worker thread.

### `Partition.remove(key: Uint8Array): Promise<void>`

Remove a key. A subsequent `get` returns `null`. Idempotent — removing a missing key is not an error.

### Types

```ts
type Key = Uint8Array;
type Value = Uint8Array;
type PersistMode = 'buffer' | 'sync-data' | 'sync-all';

export function open(path: string): Promise<Keyspace>;

export interface Keyspace {
  partition(name: string): Promise<Partition>;
  persist(mode?: PersistMode): Promise<void>;
  close(): Promise<void>;
}

export interface Partition {
  get(key: Key): Buffer | null;
  insert(key: Key, value: Value): Promise<void>;
  remove(key: Key): Promise<void>;
}
```

`Buffer` is a `Uint8Array` subclass — anywhere a `Uint8Array` is accepted, a `Buffer` works too, and reads return `Buffer` for convenience (`.toString()`, slicing, etc.).

## Persistence

`insert` and `remove` write through fjall's journal and memtable but do **not** by themselves guarantee the bytes are durable on disk. Call `persist()` when you need a durability barrier.

`persist(mode)` accepts three values, matching fjall's own:

| Mode          | Meaning                                                                         |
| ------------- | ------------------------------------------------------------------------------- |
| `'buffer'`    | Flush in-memory buffers to the OS. Survives process crash, not OS / power loss. |
| `'sync-data'` | `fdatasync` the journal — survives OS / power loss. **Default.**                |
| `'sync-all'`  | `fsync` the journal — also syncs file metadata. Strongest, slowest.             |

The default (no argument) is `'sync-data'`, matching the upstream fjall default.

Typical usage: batch a number of `insert`/`remove` calls, then call `persist()` once at the end of the batch. Calling `persist()` per write is correct but slow.

## Supported platforms

v0.1 ships pre-built binaries for:

- **macOS arm64** (`darwin-arm64`)
- **Linux x64, glibc** (`linux-x64-gnu`)

Other targets (Linux musl, Linux arm64, macOS x64, Windows) are intentionally not built in v0.1 — there is no source-build fallback, so `npm install` on an unsupported platform succeeds but `require('@fjall-js/fjall')` throws a clear error at load time. If you need another platform, please open an issue.

## Naming

We mirror the upstream [fjall](https://github.com/fjall-rs/fjall) vocabulary on purpose:

- **Keyspace** — the top-level on-disk store (a directory).
- **Partition** — a named bucket within a keyspace (analogous to a column family).

If you've read the fjall docs, the concepts and method names (`insert`, `remove`, `persist`) should be familiar.

## Roadmap / out of scope

The following are **intentionally not in v0.1**. They are not bugs; they are deferred. Most are plausible candidates for a later release.

- Range queries, iterators, cursors
- Transactions (`TxKeyspace` / `WriteTx`)
- Snapshots
- Compression, block cache, and compactor configuration
- String / JSON convenience helpers (callers pass raw bytes)
- Linux musl, Linux arm64, macOS x64, Windows binaries
- Source-build fallback when no prebuilt binary is available
- Streaming reads / writes

If your use case depends on one of these, please open an issue — prioritization is driven by demand.

## A note on the unscoped `fjall` package

The unscoped [`fjall`](https://www.npmjs.com/package/fjall) package on npm is a deliberate **squat** owned by this project. It does nothing useful: it logs an error and throws on `require`. This is to prevent confusion and typosquatting. **Always install `@fjall-js/fjall`.**

## License

Dual-licensed under either of

- MIT license ([LICENSE-MIT](./LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE))

at your option.

## Acknowledgments

This is a thin binding layer. All of the actual storage engine work lives in [fjall-rs/fjall](https://github.com/fjall-rs/fjall) — thanks to its authors and maintainers.
