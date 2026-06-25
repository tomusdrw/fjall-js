# Changelog

## 0.3.2

Fixes a native crash (`EXC_BAD_ACCESS` / `SIGSEGV`) when the database is used from
multiple Node `worker_threads` concurrently.

### Fixed

- **Worker-thread crash.** Operations were exposed as napi-rs `async fn`s, which run
  on a process-global tokio runtime and resolve their JS promise through a
  cross-thread ThreadSafeFunction. Under many concurrent `worker_threads` Envs this
  raced against the per-Env V8 isolate and crashed the whole process (faulting inside
  `napi_create_reference` / `GlobalHandles::Create` / promise resolution). Every
  operation now runs as a napi `Task` on libuv's async-work pool and resolves on its
  owning Env's thread — no shared runtime, no cross-thread deferred. The public
  `Promise`-based API is unchanged.
- A panic inside a background operation is now surfaced as a rejected promise instead
  of aborting the process.

### Operational note

- Async operations now run on **libuv's thread pool** (`UV_THREADPOOL_SIZE`, default
  **4**, shared process-wide with `fs`/`dns`/`crypto`) rather than a dedicated pool.
  For write-heavy or highly concurrent workloads, raise `UV_THREADPOOL_SIZE`.

## 0.3.0

One shared engine across Node `worker_threads`, a read-only handle type, and deterministic refcounted `close()`.

### Breaking

- `open(path, options)` → **`open(config: DatabaseConfig)`** — the path moves inside the config object: `open({ path, cacheSizeBytes? })`.
- Removed the `ephemeral` option. **`close()` no longer flushes to disk** — durability now requires an explicit `persist()`. (Cross-handle _visibility_ still needs no `persist`.)
- Multiple opens of the same path now **share one engine** instead of building independent ones.

### Added

- **`openReadonly(config): Promise<ReadonlyKeyspace>`** — a read-only surface whose `ReadonlyKeyspace`/`ReadonlyPartition` have no write methods and no `persist` (enforced at compile time, not by throwing). A reader may be the first opener and create the engine.
- **Shared engine across `worker_threads`** — every `open`/`openReadonly` of the same canonical path attaches to one process-global engine with one bounded block cache; a write on any handle is live-visible to `get` on any other, no `persist` required.
- **Deterministic refcounted `close()`** — the engine (keyspace + partition handles) is freed on the last `close()`, keeping RSS flat across open/close cycles.

### Operational contract (please read)

- **Databases are shared by filesystem path — pass the identical path from every worker.** Transfer the plain `DatabaseConfig` between workers (never a live handle).
- **No double-open protection — provide your own.** fjall takes no OS directory lock, so opening one directory via different path spellings, or from a second process, is unguarded and can corrupt the database. **Callers that need a hard guarantee must set up their own mechanism — e.g. an OS advisory lockfile on the data directory.** One process per directory; one path spelling.
- **Every successful `open`/`openReadonly` MUST be matched by exactly one `close`** on every teardown path. Relying on GC is unsupported and leaks native memory for the process lifetime (a stderr warning is emitted).
- **Durability is guaranteed only after `persist()` returns.** `close()` does not flush.
- **At most one writable handle per path** is the consumer's responsibility; a second is warned, not prevented.
