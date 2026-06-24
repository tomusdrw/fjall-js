use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

#[derive(Clone)]
pub struct EngineConfig {
    pub path: PathBuf,
    pub cache_size_bytes: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Writability {
    Writable,
    ReadOnly,
}

#[derive(Debug)]
pub enum EngineError {
    #[allow(dead_code)]
    Closed,
    Fjall(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Closed => write!(f, "Keyspace is closed"),
            EngineError::Fjall(e) => write!(f, "{e}"),
        }
    }
}

#[cfg(test)]
static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct Engine {
    #[allow(dead_code)]
    keyspace: fjall::Keyspace,
    #[allow(dead_code)]
    partitions: Mutex<HashMap<String, fjall::PartitionHandle>>,
    #[allow(dead_code)]
    key: CanonicalKey,
    cache_size_bytes: Option<u64>,
}

impl Engine {
    #[cfg(test)]
    pub fn ptr_id(self: &Arc<Self>) -> usize {
        Arc::as_ptr(self) as usize
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Frees keyspace + all partition handles together (subsumes the old
        // partition-leak fix). May briefly block joining fjall background
        // threads; releases the fjall directory lock. Does NOT persist.
        #[cfg(test)]
        DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct HandleState {
    pub(crate) engine: Weak<Engine>,
    pub(crate) key: CanonicalKey,
    #[allow(dead_code)]
    pub(crate) closed: AtomicBool,
    #[allow(dead_code)]
    pub(crate) writable: bool,
}

pub enum Slot {
    Creating,
    Live {
        engine: Arc<Engine>,
        refs: usize,
        writable: usize,
    },
    #[allow(dead_code)]
    TearingDown,
}

pub(crate) static REGISTRY: OnceLock<Mutex<HashMap<CanonicalKey, Slot>>> = OnceLock::new();
static CV: OnceLock<Condvar> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<CanonicalKey, Slot>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}
fn cv() -> &'static Condvar {
    CV.get_or_init(Condvar::new)
}

fn open_fjall(cfg: &EngineConfig) -> Result<fjall::Keyspace, EngineError> {
    let mut config = fjall::Config::new(&cfg.path);
    if let Some(bytes) = cfg.cache_size_bytes {
        if bytes >= 1 {
            config = config.cache_size(bytes);
        }
    }
    // NOTE: fjall 2.x takes no OS directory lock, so a second open of the same
    // directory does not error here. Engine sharing is enforced ONLY by the
    // canonical-path registry above. Divergent path spellings of the same dir,
    // or a second process, are unguarded and can corrupt — a documented
    // limitation: pass an identical path from every worker, one process per dir.
    config.open().map_err(|e| EngineError::Fjall(e.to_string()))
}

/// Attach to the shared engine for `cfg.path`, creating it if none exists.
/// Blocking; the napi layer calls this inside `spawn_blocking`.
pub fn attach_or_create(
    cfg: EngineConfig,
    w: Writability,
) -> Result<Arc<HandleState>, EngineError> {
    loop {
        // Re-derive the key every iteration: after a concurrent creator finishes,
        // the directory exists and a best-effort key resolves to the canonical one.
        let key = canonical_key(&cfg.path);
        let mut reg = registry().lock().unwrap();
        let mut should_wait = false;

        match reg.get_mut(&key) {
            Some(Slot::Live {
                engine,
                refs,
                writable,
            }) => {
                if let Some(prev) = engine.cache_size_bytes {
                    if let Some(req) = cfg.cache_size_bytes {
                        if req != prev {
                            eprintln!(
                                "fjall: cacheSizeBytes {req} ignored; engine already created with {prev} (first-opener wins)"
                            );
                        }
                    }
                }
                *refs += 1;
                if w == Writability::Writable {
                    *writable += 1;
                    if *writable > 1 {
                        eprintln!(
                            "fjall: a second writable handle was opened for {key:?}; single-writer is the consumer's responsibility"
                        );
                    }
                }
                let state = Arc::new(HandleState {
                    engine: Arc::downgrade(engine),
                    key: key.clone(),
                    closed: AtomicBool::new(false),
                    writable: w == Writability::Writable,
                });
                return Ok(state);
            }
            Some(Slot::Creating) | Some(Slot::TearingDown) => {
                should_wait = true;
            }
            None => {
                reg.insert(key.clone(), Slot::Creating);
            }
        }
        if should_wait {
            // Park until the in-flight create/teardown for this key finishes,
            // then loop to re-derive the (now canonical) key and re-check.
            let _guard = cv()
                .wait_while(reg, |r| {
                    matches!(r.get(&key), Some(Slot::Creating) | Some(Slot::TearingDown))
                })
                .unwrap();
            continue;
        }
        drop(reg);

        // Create outside the registry lock.
        match open_fjall(&cfg) {
            Ok(keyspace) => {
                let canon = canonical_key(&cfg.path); // dir now exists
                let mut reg = registry().lock().unwrap();
                reg.remove(&key);
                let result = match reg.get_mut(&canon) {
                    Some(Slot::Live {
                        engine,
                        refs,
                        writable,
                    }) => {
                        *refs += 1;
                        if w == Writability::Writable {
                            *writable += 1;
                        }
                        Arc::downgrade(engine)
                    }
                    _ => {
                        let engine = Arc::new(Engine {
                            keyspace,
                            partitions: Mutex::new(HashMap::new()),
                            key: canon.clone(),
                            cache_size_bytes: cfg.cache_size_bytes,
                        });
                        let weak = Arc::downgrade(&engine);
                        reg.insert(
                            canon.clone(),
                            Slot::Live {
                                engine,
                                refs: 1,
                                writable: if w == Writability::Writable { 1 } else { 0 },
                            },
                        );
                        weak
                    }
                };
                cv().notify_all();
                return Ok(Arc::new(HandleState {
                    engine: result,
                    key: canon,
                    closed: AtomicBool::new(false),
                    writable: w == Writability::Writable,
                }));
            }
            Err(e) => {
                let mut reg = registry().lock().unwrap();
                reg.remove(&key);
                cv().notify_all();
                return Err(e);
            }
        }
    }
}

pub type CanonicalKey = PathBuf;

/// Best-effort canonical registry key. Resolves symlinks/case/relative parts
/// when `path` exists; otherwise canonicalizes the nearest existing ancestor and
/// re-appends the remaining components. Never fails.
pub fn canonical_key(path: &Path) -> CanonicalKey {
    if let Ok(c) = std::fs::canonicalize(path) {
        return c;
    }
    // Walk up to the nearest existing ancestor, canonicalize it, re-append the rest.
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|c| c.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut ancestor = abs.as_path();
    // Own each component so the tail can't dangle into `abs` as the loop reassigns
    // `ancestor`; the path is short, so the per-component clone is negligible.
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    loop {
        if let Ok(c) = std::fs::canonicalize(ancestor) {
            let mut key = c;
            for part in tail.iter().rev() {
                key.push(part);
            }
            return key;
        }
        match (ancestor.file_name(), ancestor.parent()) {
            (Some(name), Some(parent)) => {
                tail.push(name.to_os_string());
                ancestor = parent;
            }
            _ => return abs, // hit the root with nothing canonicalizable; lexical absolute
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // Serializes tests that mutate process-global cwd so they can't race the
    // (parallel-by-default) test harness as more tests are added to this module.
    static CWD_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn cfg(path: &Path) -> EngineConfig {
        EngineConfig { path: path.to_path_buf(), cache_size_bytes: None }
    }

    fn refs_for(key: &CanonicalKey) -> usize {
        let reg = registry().lock().unwrap();
        match reg.get(key) {
            Some(Slot::Live { refs, .. }) => *refs,
            _ => 0,
        }
    }

    #[test]
    fn first_open_creates_second_attaches_same_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");

        let h1 = attach_or_create(cfg(&p), Writability::Writable).unwrap();
        let h2 = attach_or_create(cfg(&p), Writability::ReadOnly).unwrap();

        let e1 = h1.engine.upgrade().unwrap();
        let e2 = h2.engine.upgrade().unwrap();
        assert_eq!(e1.ptr_id(), e2.ptr_id(), "both handles share one engine");
        assert_eq!(refs_for(&h1.key), 2);
    }

    #[test]
    fn distinct_paths_get_distinct_engines() {
        let tmp = tempfile::tempdir().unwrap();
        let a = attach_or_create(cfg(&tmp.path().join("a")), Writability::Writable).unwrap();
        let b = attach_or_create(cfg(&tmp.path().join("b")), Writability::Writable).unwrap();
        let ea = a.engine.upgrade().unwrap();
        let eb = b.engine.upgrade().unwrap();
        assert_ne!(ea.ptr_id(), eb.ptr_id());
    }

    #[test]
    fn existing_dir_canonicalizes_to_same_key_for_relative_and_absolute() {
        let _serial = CWD_GUARD.lock().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let abs = tmp.path().join("db");
        fs::create_dir(&abs).unwrap();

        // Restore cwd on scope exit even if an assertion panics.
        struct RestoreCwd(PathBuf);
        impl Drop for RestoreCwd {
            fn drop(&mut self) {
                let _ = std::env::set_current_dir(&self.0);
            }
        }
        let _restore = RestoreCwd(std::env::current_dir().unwrap());

        std::env::set_current_dir(tmp.path()).unwrap();
        let via_rel = canonical_key(Path::new("db"));
        let via_abs = canonical_key(&abs); // absolute spelling — unaffected by cwd
        assert_eq!(via_rel, via_abs, "relative and absolute spellings must match");
    }

    #[test]
    fn nonexistent_path_uses_nearest_existing_ancestor() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("not-yet").join("db");
        let key = canonical_key(&missing);
        // The existing tmp prefix is canonicalized; the missing tail is preserved.
        let canon_tmp = fs::canonicalize(tmp.path()).unwrap();
        assert_eq!(key, canon_tmp.join("not-yet").join("db"));
    }
}
