//! Thin napi surface over the [`engine`] module (the shared-engine registry).
//!
//! Each `Keyspace`/`ReadonlyKeyspace`/`Partition`/`ReadonlyPartition` is a handle
//! holding an `Arc<engine::HandleState>`. Blocking engine calls run inside
//! `spawn_blocking`; only `get` is synchronous. `EngineError` is mapped to
//! `napi::Error`. A keyspace `Drop` calls `engine::warn_if_unclosed` — it never
//! tears the engine down (that is `close()`'s job; see the engine module docs for
//! the leak-on-forgotten-close contract).

mod engine;

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
fn join_err(e: napi::tokio::task::JoinError) -> Error {
    Error::from_reason(format!("worker task failed: {e}"))
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

/// Open (or attach to) the shared writable engine for `config.path`.
#[napi]
pub async fn open(config: DatabaseConfig) -> Result<Keyspace> {
    let cfg = to_engine_config(&config);
    let state =
        napi::tokio::task::spawn_blocking(move || attach_or_create(cfg, Writability::Writable))
            .await
            .map_err(join_err)?
            .map_err(map_err)?;
    Ok(Keyspace { state })
}

#[napi]
pub struct Keyspace {
    state: Arc<HandleState>,
}

#[napi]
impl Keyspace {
    #[napi]
    pub async fn partition(&self, name: String) -> Result<Partition> {
        let state = self.state.clone();
        let n = name.clone();
        napi::tokio::task::spawn_blocking(move || open_partition(&state, &n))
            .await
            .map_err(join_err)?
            .map_err(map_err)?;
        Ok(Partition {
            state: self.state.clone(),
            name,
        })
    }

    #[napi]
    pub async fn persist(&self, mode: Option<String>) -> Result<()> {
        let mode = parse_persist_mode(mode.as_deref())?;
        let state = self.state.clone();
        napi::tokio::task::spawn_blocking(move || with_keyspace(&state, |ks| ks.persist(mode)))
            .await
            .map_err(join_err)?
            .map_err(map_err)?
            .map_err(|e| Error::from_reason(e.to_string()))?;
        Ok(())
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        let state = self.state.clone();
        napi::tokio::task::spawn_blocking(move || release(&state))
            .await
            .map_err(join_err)?;
        Ok(())
    }
}

impl Drop for Keyspace {
    fn drop(&mut self) {
        warn_if_unclosed(&self.state, "Keyspace");
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

    #[napi]
    pub async fn insert(&self, key: Uint8Array, value: Uint8Array) -> Result<()> {
        let state = self.state.clone();
        let name = self.name.clone();
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        napi::tokio::task::spawn_blocking(move || -> Result<()> {
            let part = resolve_partition(&state, &name).map_err(map_err)?;
            part.insert(key, value)
                .map_err(|e| Error::from_reason(e.to_string()))
        })
        .await
        .map_err(join_err)??;
        Ok(())
    }

    #[napi]
    pub async fn remove(&self, key: Uint8Array) -> Result<()> {
        let state = self.state.clone();
        let name = self.name.clone();
        let key = key.as_ref().to_vec();
        napi::tokio::task::spawn_blocking(move || -> Result<()> {
            let part = resolve_partition(&state, &name).map_err(map_err)?;
            part.remove(key)
                .map_err(|e| Error::from_reason(e.to_string()))
        })
        .await
        .map_err(join_err)??;
        Ok(())
    }

    #[napi]
    pub async fn insert_batch(&self, entries: Vec<BatchEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let state = self.state.clone();
        let name = self.name.clone();
        let owned: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|e| (e.key.as_ref().to_vec(), e.value.as_ref().to_vec()))
            .collect();
        napi::tokio::task::spawn_blocking(move || -> Result<()> {
            let part = resolve_partition(&state, &name).map_err(map_err)?;
            with_keyspace(&state, |ks| {
                let mut batch = ks.batch();
                for (key, value) in owned {
                    batch.insert(&part, key, value);
                }
                batch.commit()
            })
            .map_err(map_err)?
            .map_err(|e| Error::from_reason(e.to_string()))
        })
        .await
        .map_err(join_err)??;
        Ok(())
    }
}

/// Open (or attach to) the shared engine for `config.path` with a read-only
/// surface. May be the first opener (fjall always opens read-write underneath).
#[napi]
pub async fn open_readonly(config: DatabaseConfig) -> Result<ReadonlyKeyspace> {
    let cfg = to_engine_config(&config);
    let state =
        napi::tokio::task::spawn_blocking(move || attach_or_create(cfg, Writability::ReadOnly))
            .await
            .map_err(join_err)?
            .map_err(map_err)?;
    Ok(ReadonlyKeyspace { state })
}

#[napi]
pub struct ReadonlyKeyspace {
    state: Arc<HandleState>,
}

#[napi]
impl ReadonlyKeyspace {
    #[napi]
    pub async fn partition(&self, name: String) -> Result<ReadonlyPartition> {
        let state = self.state.clone();
        let n = name.clone();
        napi::tokio::task::spawn_blocking(move || open_partition(&state, &n))
            .await
            .map_err(join_err)?
            .map_err(map_err)?;
        Ok(ReadonlyPartition {
            state: self.state.clone(),
            name,
        })
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        let state = self.state.clone();
        napi::tokio::task::spawn_blocking(move || release(&state))
            .await
            .map_err(join_err)?;
        Ok(())
    }
}

impl Drop for ReadonlyKeyspace {
    fn drop(&mut self) {
        warn_if_unclosed(&self.state, "ReadonlyKeyspace");
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
