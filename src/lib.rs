//! Thin napi surface over the [`engine`] module (the shared-engine registry).
//!
//! Each `Keyspace`/`ReadonlyKeyspace`/`Partition`/`ReadonlyPartition` is a handle
//! holding an `Arc<engine::HandleState>`. Every blocking engine call runs on the
//! libuv thread-pool via a napi [`Task`] (returned as `AsyncTask`); only `get` is
//! synchronous. `EngineError` is mapped to `napi::Error`. A keyspace `Drop`
//! auto-releases the engine handle (via `engine::release`, wrapped in
//! `catch_unwind`) so the engine is reclaimed even if `close()` is never called.
//! `close()` is still recommended for deterministic cleanup — auto-release runs
//! inside the N-API destructor during GC / environment teardown.
//!
//! # worker_threads safety — why `Task`, not `async fn`
//! We deliberately do NOT expose operations as napi-rs `#[napi] async fn`. In
//! napi-rs 2.x an `async fn` is driven by `execute_tokio_future`, which (1) spawns
//! the future on a **process-global tokio runtime** shared by every Node
//! `worker_threads` N-API Env, and (2) resolves the JS promise through a per-call
//! **ThreadSafeFunction** (`napi_resolve_deferred` via `ThreadSafeFunction::AsyncCb`).
//! Under many concurrent worker Envs issuing async ops, that global-runtime +
//! cross-thread tsfn-deferred resolution races against the per-Env v8 isolate and
//! crashes (EXC_BAD_ACCESS in `GlobalHandles::Create` / `ConcludeDeferred`).
//!
//! The napi [`Task`] path instead runs `compute()` on a libuv thread-pool thread
//! and resolves the deferred **directly on the owning Env's thread** (libuv
//! async-work `complete`) — no global runtime, no ThreadSafeFunction — so it is
//! isolate-safe across worker_threads. See `.context/ROOT_CAUSE.md` for the full
//! investigation and the bisection that pinned this down.

mod engine;

use std::io::Write;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use engine::{
    attach_or_create, open_partition, release, resolve_partition, warn_if_unclosed, with_keyspace,
    EngineConfig, EngineError, HandleState, Writability,
};

fn map_err(e: EngineError) -> Error {
    Error::from_reason(e.to_string())
}

fn parse_persist_mode(s: Option<&str>) -> Result<fjall::PersistMode> {
    Ok(match s {
        None | Some("sync-data") => fjall::PersistMode::SyncData,
        Some("buffer") => fjall::PersistMode::Buffer,
        Some("sync-all") => fjall::PersistMode::SyncAll,
        Some(other) => {
            return Err(Error::from_reason(format!(
                "invalid persist mode: '{other}' (expected 'buffer', 'sync-data', or 'sync-all')"
            )))
        }
    })
}

/// Engine-level configuration. All fields are fixed by the FIRST opener of a
/// path (first-opener-wins); pass the same config from every worker.
#[napi(object)]
pub struct DatabaseConfig {
    pub path: String,
    /// Shared block-cache capacity in bytes. Caps resident set. Engine-level.
    pub cache_size_bytes: Option<f64>,
}

fn to_engine_config(c: &DatabaseConfig) -> EngineConfig {
    EngineConfig {
        path: std::path::PathBuf::from(&c.path),
        cache_size_bytes: c.cache_size_bytes.and_then(|b| {
            if b.is_finite() && b >= 1.0 {
                Some(b as u64)
            } else {
                None
            }
        }),
    }
}

/// Extract a human-readable message from a caught panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// A [`Task`] wrapper that catches a panic in the inner `compute()` and turns it
/// into a rejected promise instead of letting it unwind across the `extern "C"`
/// boundary and abort the whole process.
///
/// This matters more here than usual: the engine is process-global and shared by
/// every Node `worker_threads` Env, so a single aborting panic would take down
/// **all** workers. It also restores the behaviour we had under napi-rs `async fn`
/// (tokio's `spawn_blocking` caught panics and rejected). Our shared mutexes are
/// poison-tolerant (see `engine::lock`), so recovering from a panic does not wedge
/// the engine for the rest of the process. `resolve`/`reject`/`finally` run on the
/// JS thread and only build a return value, so they are forwarded unwrapped.
pub struct PanicSafe<T>(T);

impl<T: Task> Task for PanicSafe<T> {
    type Output = T::Output;
    type JsValue = T::JsValue;

    fn compute(&mut self) -> Result<Self::Output> {
        let inner = &mut self.0;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.compute())).unwrap_or_else(
            |payload| {
                let msg = panic_message(&*payload);
                let _ = writeln!(
                    std::io::stderr(),
                    "fjall: caught panic in {} background task: {msg}",
                    std::any::type_name::<T>()
                );
                Err(Error::from_reason(format!(
                    "fjall: internal error in background task: {msg}"
                )))
            },
        )
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        self.0.resolve(env, output)
    }

    fn reject(&mut self, env: Env, err: Error) -> Result<Self::JsValue> {
        self.0.reject(env, err)
    }

    fn finally(&mut self, env: Env) -> Result<()> {
        self.0.finally(env)
    }
}

// ---------------------------------------------------------------------------
// Tasks. Each runs `compute()` on a libuv thread-pool thread and `resolve()` on
// the owning Env's JS thread (see the module-level worker_threads note). They are
// always wrapped in [`PanicSafe`] at the call site before being handed to napi.
// ---------------------------------------------------------------------------

/// `open` / `open_readonly`: attach to (or create) the shared engine.
pub struct OpenTask {
    cfg: EngineConfig,
    writability: Writability,
}

impl Task for OpenTask {
    type Output = Arc<HandleState>;
    type JsValue = Keyspace;

    fn compute(&mut self) -> Result<Arc<HandleState>> {
        attach_or_create(self.cfg.clone(), self.writability).map_err(map_err)
    }

    fn resolve(&mut self, _env: Env, state: Arc<HandleState>) -> Result<Keyspace> {
        Ok(Keyspace { state })
    }
}

/// Read-only variant of [`OpenTask`] (distinct `JsValue`).
pub struct OpenReadonlyTask {
    cfg: EngineConfig,
}

impl Task for OpenReadonlyTask {
    type Output = Arc<HandleState>;
    type JsValue = ReadonlyKeyspace;

    fn compute(&mut self) -> Result<Arc<HandleState>> {
        attach_or_create(self.cfg.clone(), Writability::ReadOnly).map_err(map_err)
    }

    fn resolve(&mut self, _env: Env, state: Arc<HandleState>) -> Result<ReadonlyKeyspace> {
        Ok(ReadonlyKeyspace { state })
    }
}

/// `Keyspace::partition`: open (idempotently) a partition, return its handle.
pub struct PartitionTask {
    state: Arc<HandleState>,
    name: String,
}

impl Task for PartitionTask {
    type Output = ();
    type JsValue = Partition;

    fn compute(&mut self) -> Result<()> {
        open_partition(&self.state, &self.name).map_err(map_err)
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<Partition> {
        Ok(Partition {
            state: self.state.clone(),
            name: std::mem::take(&mut self.name),
        })
    }
}

/// Read-only variant of [`PartitionTask`].
pub struct ReadonlyPartitionTask {
    state: Arc<HandleState>,
    name: String,
}

impl Task for ReadonlyPartitionTask {
    type Output = ();
    type JsValue = ReadonlyPartition;

    fn compute(&mut self) -> Result<()> {
        open_partition(&self.state, &self.name).map_err(map_err)
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<ReadonlyPartition> {
        Ok(ReadonlyPartition {
            state: self.state.clone(),
            name: std::mem::take(&mut self.name),
        })
    }
}

/// `Keyspace::persist`.
pub struct PersistTask {
    state: Arc<HandleState>,
    mode: Option<String>,
}

impl Task for PersistTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        // Parse inside compute so a bad mode rejects the promise (rather than
        // throwing synchronously from the napi entrypoint).
        let mode = parse_persist_mode(self.mode.as_deref())?;
        with_keyspace(&self.state, |ks| ks.persist(mode))
            .map_err(map_err)?
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<()> {
        Ok(())
    }
}

/// `Keyspace::close` / `ReadonlyKeyspace::close`: drop this handle's engine ref.
pub struct CloseTask {
    state: Arc<HandleState>,
}

impl Task for CloseTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        release(&self.state);
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<()> {
        Ok(())
    }
}

/// `Partition::insert`.
pub struct InsertTask {
    state: Arc<HandleState>,
    name: String,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Task for InsertTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        let part = resolve_partition(&self.state, &self.name).map_err(map_err)?;
        part.insert(
            std::mem::take(&mut self.key),
            std::mem::take(&mut self.value),
        )
        .map_err(|e| Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<()> {
        Ok(())
    }
}

/// `Partition::remove`.
pub struct RemoveTask {
    state: Arc<HandleState>,
    name: String,
    key: Vec<u8>,
}

impl Task for RemoveTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        let part = resolve_partition(&self.state, &self.name).map_err(map_err)?;
        part.remove(std::mem::take(&mut self.key))
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<()> {
        Ok(())
    }
}

/// `Partition::insert_batch`.
pub struct InsertBatchTask {
    state: Arc<HandleState>,
    name: String,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Task for InsertBatchTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }
        let part = resolve_partition(&self.state, &self.name).map_err(map_err)?;
        let entries = std::mem::take(&mut self.entries);
        with_keyspace(&self.state, |ks| {
            let mut batch = ks.batch();
            for (key, value) in entries {
                batch.insert(&part, key, value);
            }
            batch.commit()
        })
        .map_err(map_err)?
        .map_err(|e| Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// napi surface.
// ---------------------------------------------------------------------------

/// Open (or attach to) the shared writable engine for `config.path`.
#[napi(ts_return_type = "Promise<Keyspace>")]
pub fn open(config: DatabaseConfig) -> AsyncTask<PanicSafe<OpenTask>> {
    AsyncTask::new(PanicSafe(OpenTask {
        cfg: to_engine_config(&config),
        writability: Writability::Writable,
    }))
}

#[napi]
pub struct Keyspace {
    state: Arc<HandleState>,
}

#[napi]
impl Keyspace {
    #[napi(ts_return_type = "Promise<Partition>")]
    pub fn partition(&self, name: String) -> AsyncTask<PanicSafe<PartitionTask>> {
        AsyncTask::new(PanicSafe(PartitionTask {
            state: self.state.clone(),
            name,
        }))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn persist(&self, mode: Option<String>) -> AsyncTask<PanicSafe<PersistTask>> {
        AsyncTask::new(PanicSafe(PersistTask {
            state: self.state.clone(),
            mode,
        }))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn close(&self) -> AsyncTask<PanicSafe<CloseTask>> {
        AsyncTask::new(PanicSafe(CloseTask {
            state: self.state.clone(),
        }))
    }
}

impl Drop for Keyspace {
    fn drop(&mut self) {
        // Auto-release the engine handle if the consumer forgot to call close().
        // Wrapped in catch_unwind: during worker-thread shutdown the N-API
        // destructor runs as an extern "C" callback — a panic here would cross
        // the FFI boundary and abort the process (SIGABRT / exit 134).
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            warn_if_unclosed(&self.state, "Keyspace");
            release(&self.state);
        }));
    }
}

/// A key/value pair for [`Partition::insert_batch`].
#[napi(object)]
pub struct BatchEntry {
    pub key: Uint8Array,
    pub value: Uint8Array,
}

#[napi]
pub struct Partition {
    state: Arc<HandleState>,
    name: String,
}

#[napi]
impl Partition {
    /// Sync read. Reflects writes committed by any handle of the engine.
    #[napi]
    pub fn get(&self, key: Uint8Array) -> Result<Option<Buffer>> {
        let part = resolve_partition(&self.state, &self.name).map_err(map_err)?;
        match part.get(key.as_ref()) {
            Ok(Some(slice)) => Ok(Some(Buffer::from(slice.as_ref().to_vec()))),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn insert(&self, key: Uint8Array, value: Uint8Array) -> AsyncTask<PanicSafe<InsertTask>> {
        AsyncTask::new(PanicSafe(InsertTask {
            state: self.state.clone(),
            name: self.name.clone(),
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        }))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn remove(&self, key: Uint8Array) -> AsyncTask<PanicSafe<RemoveTask>> {
        AsyncTask::new(PanicSafe(RemoveTask {
            state: self.state.clone(),
            name: self.name.clone(),
            key: key.as_ref().to_vec(),
        }))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn insert_batch(&self, entries: Vec<BatchEntry>) -> AsyncTask<PanicSafe<InsertBatchTask>> {
        let owned: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|e| (e.key.as_ref().to_vec(), e.value.as_ref().to_vec()))
            .collect();
        AsyncTask::new(PanicSafe(InsertBatchTask {
            state: self.state.clone(),
            name: self.name.clone(),
            entries: owned,
        }))
    }
}

/// Open (or attach to) the shared engine for `config.path` with a read-only
/// surface. May be the first opener (fjall always opens read-write underneath).
#[napi(ts_return_type = "Promise<ReadonlyKeyspace>")]
pub fn open_readonly(config: DatabaseConfig) -> AsyncTask<PanicSafe<OpenReadonlyTask>> {
    AsyncTask::new(PanicSafe(OpenReadonlyTask {
        cfg: to_engine_config(&config),
    }))
}

#[napi]
pub struct ReadonlyKeyspace {
    state: Arc<HandleState>,
}

#[napi]
impl ReadonlyKeyspace {
    #[napi(ts_return_type = "Promise<ReadonlyPartition>")]
    pub fn partition(&self, name: String) -> AsyncTask<PanicSafe<ReadonlyPartitionTask>> {
        AsyncTask::new(PanicSafe(ReadonlyPartitionTask {
            state: self.state.clone(),
            name,
        }))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn close(&self) -> AsyncTask<PanicSafe<CloseTask>> {
        AsyncTask::new(PanicSafe(CloseTask {
            state: self.state.clone(),
        }))
    }
}

impl Drop for ReadonlyKeyspace {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            warn_if_unclosed(&self.state, "ReadonlyKeyspace");
            release(&self.state);
        }));
    }
}

#[napi]
pub struct ReadonlyPartition {
    state: Arc<HandleState>,
    name: String,
}

#[napi]
impl ReadonlyPartition {
    /// Sync read. Reflects writes committed by any handle of the engine.
    #[napi]
    pub fn get(&self, key: Uint8Array) -> Result<Option<Buffer>> {
        let part = resolve_partition(&self.state, &self.name).map_err(map_err)?;
        match part.get(key.as_ref()) {
            Ok(Some(slice)) => Ok(Some(Buffer::from(slice.as_ref().to_vec()))),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::from_reason(e.to_string())),
        }
    }
}
