mod engine;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};

use napi::bindgen_prelude::*;
use napi_derive::napi;

type SharedKeyspace = Arc<Mutex<Option<fjall::Keyspace>>>;
type SharedPartition = Arc<Mutex<Option<fjall::PartitionHandle>>>;

/// Weak references to the partition handles opened from a keyspace, so
/// `close()` can drop them eagerly. Weak (not Arc) so a keyspace never keeps a
/// partition's native memory alive on its own — only the JS `Partition` wrapper
/// holds the strong reference.
type PartitionRegistry = Arc<Mutex<Vec<Weak<Mutex<Option<fjall::PartitionHandle>>>>>>;

fn err(e: impl std::fmt::Display) -> Error {
    Error::from_reason(e.to_string())
}

fn join_err(e: napi::tokio::task::JoinError) -> Error {
    Error::from_reason(format!("worker task failed: {e}"))
}

fn lock<'a, T>(m: &'a Mutex<T>) -> Result<std::sync::MutexGuard<'a, T>> {
    m.lock()
        .map_err(|_| Error::from_reason("internal lock poisoned"))
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

/// Options for [`open`].
#[napi(object)]
pub struct OpenOptions {
    /// Skip the durability flush (`sync-all`) on `close()`. Only safe for
    /// throwaway databases — e.g. a fuzz target that wipes on every reset.
    /// Mirrors LMDB's `noSync`. Defaults to `false`.
    pub ephemeral: Option<bool>,
    /// Cache capacity in bytes, shared across the keyspace's partitions. fjall
    /// reads through this bounded cache instead of mmapping the whole store, so
    /// this caps the resident set. Defaults to fjall's built-in cache size.
    pub cache_size_bytes: Option<f64>,
}

/// Open (or create) a fjall keyspace at the given filesystem path.
#[napi]
pub async fn open(path: String, options: Option<OpenOptions>) -> Result<Keyspace> {
    let path = PathBuf::from(path);
    let ephemeral = options.as_ref().and_then(|o| o.ephemeral).unwrap_or(false);
    let cache_size_bytes = options.as_ref().and_then(|o| o.cache_size_bytes);

    let ks = napi::tokio::task::spawn_blocking(move || {
        let mut config = fjall::Config::new(path);
        // Only override the cache when a sane, positive size is given; otherwise
        // keep fjall's default.
        if let Some(bytes) = cache_size_bytes {
            if bytes.is_finite() && bytes >= 1.0 {
                config = config.cache_size(bytes as u64);
            }
        }
        config.open()
    })
    .await
    .map_err(join_err)?
    .map_err(err)?;

    Ok(Keyspace {
        inner: Arc::new(Mutex::new(Some(ks))),
        partitions: Arc::new(Mutex::new(Vec::new())),
        ephemeral,
    })
}

#[napi]
pub struct Keyspace {
    inner: SharedKeyspace,
    partitions: PartitionRegistry,
    /// When true, `close()` skips the sync-all flush (see [`OpenOptions`]).
    ephemeral: bool,
}

#[napi]
impl Keyspace {
    /// Open (or create, if missing) a partition.
    #[napi]
    pub async fn partition(&self, name: String) -> Result<Partition> {
        let ks_clone = {
            let g = lock(&self.inner)?;
            g.as_ref()
                .ok_or_else(|| Error::from_reason("Keyspace is closed"))?
                .clone()
        };
        let state = self.inner.clone();

        // Allocate the shared handle slot and register its weak reference now,
        // *before* the await, so close() can reach it; the slot is filled once
        // open_partition returns below. (Like the rest of this file, we read
        // `&self` only before awaiting and carry owned data across the await.)
        // Prune dead weaks first so the registry can't grow unbounded across
        // many partition() calls.
        let inner: SharedPartition = Arc::new(Mutex::new(None));
        {
            let mut registry = lock(&self.partitions)?;
            registry.retain(|w| w.strong_count() > 0);
            registry.push(Arc::downgrade(&inner));
        }

        let handle = napi::tokio::task::spawn_blocking(move || {
            ks_clone.open_partition(&name, fjall::PartitionCreateOptions::default())
        })
        .await
        .map_err(join_err)?
        .map_err(err)?;

        *lock(&inner)? = Some(handle);

        Ok(Partition {
            ks_state: state,
            inner,
        })
    }

    /// Flush the journal to disk. Defaults to fjall's `sync-data` mode.
    #[napi]
    pub async fn persist(&self, mode: Option<String>) -> Result<()> {
        let mode = parse_persist_mode(mode.as_deref())?;
        let ks_clone = {
            let g = lock(&self.inner)?;
            g.as_ref()
                .ok_or_else(|| Error::from_reason("Keyspace is closed"))?
                .clone()
        };
        napi::tokio::task::spawn_blocking(move || ks_clone.persist(mode))
            .await
            .map_err(join_err)?
            .map_err(err)?;
        Ok(())
    }

    /// Release the keyspace handle, persisting with `sync-all` first unless the
    /// keyspace was opened with `ephemeral: true`. Subsequent operations on this
    /// Keyspace and any Partition opened from it will fail.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        let ephemeral = self.ephemeral;
        let taken = {
            let mut g = lock(&self.inner)?;
            g.take()
        };

        // Pull every still-live partition handle out of its slot so it can be
        // dropped now, rather than whenever V8 happens to GC the JS Partition
        // wrappers — which, because the heavy memory lives in the native heap
        // and is invisible to V8, can be effectively never. A live handle keeps
        // the partition's memtables, journal, write buffer and block cache
        // alive, so deferring this drop is the leak. Dead weaks were already
        // freed; draining also empties the registry so a second close() is a
        // no-op.
        let parts: Vec<fjall::PartitionHandle> = {
            let mut registry = lock(&self.partitions)?;
            let mut handles = Vec::with_capacity(registry.len());
            for weak in registry.drain(..) {
                if let Some(arc) = weak.upgrade() {
                    if let Some(handle) = lock(&arc)?.take() {
                        handles.push(handle);
                    }
                }
            }
            handles
        };

        if taken.is_some() || !parts.is_empty() {
            napi::tokio::task::spawn_blocking(move || {
                if let Some(ks) = taken {
                    // Ephemeral keyspaces are throwaway, so skip the sync-all
                    // fsync — it is the dominant teardown cost when a fuzz target
                    // wipes and reopens the keyspace on every reset.
                    if !ephemeral {
                        let _ = ks.persist(fjall::PersistMode::SyncAll);
                    }
                    drop(ks);
                }
                drop(parts);
            })
            .await
            .map_err(join_err)?;
        }
        Ok(())
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
    ks_state: SharedKeyspace,
    inner: SharedPartition,
}

#[napi]
impl Partition {
    /// Sync read. Returns the value as a Buffer, or null if the key is missing.
    #[napi]
    pub fn get(&self, key: Uint8Array) -> Result<Option<Buffer>> {
        {
            let g = lock(&self.ks_state)?;
            if g.is_none() {
                return Err(Error::from_reason("Keyspace is closed"));
            }
        }
        let g = lock(&self.inner)?;
        let part = g
            .as_ref()
            .ok_or_else(|| Error::from_reason("Partition is closed"))?;

        match part.get(key.as_ref()) {
            Ok(Some(slice)) => Ok(Some(Buffer::from(slice.as_ref().to_vec()))),
            Ok(None) => Ok(None),
            Err(e) => Err(err(e)),
        }
    }

    /// Async write.
    #[napi]
    pub async fn insert(&self, key: Uint8Array, value: Uint8Array) -> Result<()> {
        let part = self.clone_part()?;
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        napi::tokio::task::spawn_blocking(move || part.insert(key, value))
            .await
            .map_err(join_err)?
            .map_err(err)?;
        Ok(())
    }

    /// Async delete. Idempotent — removing a missing key is not an error.
    #[napi]
    pub async fn remove(&self, key: Uint8Array) -> Result<()> {
        let part = self.clone_part()?;
        let key = key.as_ref().to_vec();
        napi::tokio::task::spawn_blocking(move || part.remove(key))
            .await
            .map_err(join_err)?
            .map_err(err)?;
        Ok(())
    }

    /// Atomically insert many key/value pairs in a single write batch.
    ///
    /// Equivalent to a sequence of `insert` calls, but commits them as one fjall
    /// batch: a single journal write and one worker-thread round-trip, instead of
    /// one per pair. Like `insert`, durability is deferred — call `persist` (or
    /// `close` on a non-ephemeral keyspace) to flush to disk. An empty list is a
    /// no-op.
    #[napi]
    pub async fn insert_batch(&self, entries: Vec<BatchEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let ks = self.clone_ks()?;
        let part = self.clone_part()?;
        // Copy the bytes out of the JS-owned Uint8Arrays before leaving the JS
        // thread; `into_iter` drops each entry here so no Uint8Array is held
        // across the await.
        let owned: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|e| (e.key.as_ref().to_vec(), e.value.as_ref().to_vec()))
            .collect();
        napi::tokio::task::spawn_blocking(move || {
            let mut batch = ks.batch();
            for (key, value) in owned {
                batch.insert(&part, key, value);
            }
            batch.commit()
        })
        .await
        .map_err(join_err)?
        .map_err(err)?;
        Ok(())
    }
}

impl Partition {
    fn clone_part(&self) -> Result<fjall::PartitionHandle> {
        {
            let g = lock(&self.ks_state)?;
            if g.is_none() {
                return Err(Error::from_reason("Keyspace is closed"));
            }
        }
        let g = lock(&self.inner)?;
        let part = g
            .as_ref()
            .ok_or_else(|| Error::from_reason("Partition is closed"))?
            .clone();
        Ok(part)
    }

    fn clone_ks(&self) -> Result<fjall::Keyspace> {
        let g = lock(&self.ks_state)?;
        let ks = g
            .as_ref()
            .ok_or_else(|| Error::from_reason("Keyspace is closed"))?
            .clone();
        Ok(ks)
    }
}
