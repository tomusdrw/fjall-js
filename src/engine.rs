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
        // threads. Does NOT persist (fjall holds no dir lock to release).
        #[cfg(test)]
        DROP_COUNT.fetch_add(1, Ordering::SeqCst);
    }
}

pub struct HandleState {
    pub(crate) engine: Weak<Engine>,
    pub(crate) key: CanonicalKey,
    pub(crate) closed: AtomicBool,
    pub(crate) writable: bool,
}

pub enum Slot {
    Creating,
    Live {
        engine: Arc<Engine>,
        refs: usize,
        writable: usize,
    },
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
    // Callers that need hard double-open protection must provide their own
    // mechanism (e.g. an OS advisory lockfile on the data directory).
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

/// Drop this handle's reference. Idempotent. On the last handle for an engine,
/// tears the engine down deterministically (outside the registry lock).
/// Blocking; the napi layer calls this inside `spawn_blocking`.
pub fn release(state: &Arc<HandleState>) {
    // First close wins; later calls (incl. GC after an explicit close) are no-ops.
    if state
        .closed
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    let engine_to_drop: Option<Arc<Engine>> = {
        let mut reg = registry().lock().unwrap();
        // Decrement first and compute is_last; this ends the get_mut borrow
        // before we re-borrow reg to insert the TearingDown marker.
        let is_last = match reg.get_mut(&state.key) {
            Some(Slot::Live {
                refs,
                writable,
                ..
            }) => {
                debug_assert!(*refs > 0, "release: refs underflow (double-decrement?)");
                *refs -= 1;
                if state.writable && *writable > 0 {
                    *writable -= 1;
                }
                *refs == 0
            }
            // A handle is only handed out after its slot is Live, and the slot
            // stays Live while the handle is open (it counts in `refs`); the CAS
            // above guarantees this is the first release. So a first release always
            // finds Live. Any other state is a broken invariant: loud in debug,
            // graceful in release (don't crash a live node — at worst leak one ref).
            _ => {
                debug_assert!(false, "release: handle's slot is not Live");
                return;
            }
        };
        if is_last {
            // Take the engine out under a TearingDown marker so a concurrent
            // attach_or_create parks instead of racing the dir-lock release.
            match reg.insert(state.key.clone(), Slot::TearingDown) {
                Some(Slot::Live { engine, .. }) => Some(engine),
                // We just read Live under this same lock and never released it, so
                // insert must return that Live. Anything else is a broken invariant:
                // assert in debug; in release, undo our TearingDown so the key isn't
                // wedged for future opens.
                _ => {
                    debug_assert!(false, "release: Live slot changed under the registry lock");
                    reg.remove(&state.key);
                    None
                }
            }
        } else {
            None
        }
    };

    if let Some(engine) = engine_to_drop {
        drop(engine); // joins fjall background threads (fjall holds no dir lock)
        let mut reg = registry().lock().unwrap();
        reg.remove(&state.key);
        cv().notify_all();
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

    // Serializes tests that make relative-delta assertions on the process-global
    // DROP_COUNT so they cannot race each other.
    static DROP_COUNT_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn drop_count() -> usize {
        DROP_COUNT.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn lock_drop_count_guard() -> std::sync::MutexGuard<'static, ()> {
        DROP_COUNT_GUARD.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn last_release_drops_engine_deterministically() {
        let _g = lock_drop_count_guard();
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        let before = drop_count();

        let h1 = attach_or_create(cfg(&p), Writability::Writable).unwrap();
        let h2 = attach_or_create(cfg(&p), Writability::ReadOnly).unwrap();

        release(&h1);
        assert_eq!(drop_count(), before, "engine alive while a handle remains");
        assert!(h2.engine.upgrade().is_some());

        release(&h2);
        assert_eq!(drop_count(), before + 1, "engine dropped on the last release");
        assert_eq!(refs_for(&h1.key), 0, "slot is gone");
    }

    #[test]
    fn release_is_idempotent() {
        let _g = lock_drop_count_guard();
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        let before = drop_count();
        let h = attach_or_create(cfg(&p), Writability::Writable).unwrap();
        release(&h);
        release(&h); // double close: must be a no-op
        assert_eq!(drop_count(), before + 1);
    }

    #[test]
    fn reopen_after_last_release_creates_fresh_engine() {
        let _g = lock_drop_count_guard();
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        let h1 = attach_or_create(cfg(&p), Writability::Writable).unwrap();
        let id1 = h1.engine.upgrade().unwrap().ptr_id();
        release(&h1);
        let h2 = attach_or_create(cfg(&p), Writability::Writable).unwrap();
        let id2 = h2.engine.upgrade().unwrap().ptr_id();
        assert_ne!(id1, id2, "a fresh engine is created after teardown");
        release(&h2);
    }

    #[test]
    fn symlink_alias_attaches_to_one_engine() {
        let _drop_guard = DROP_COUNT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = tmp.path().join("link");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &link).unwrap();
        #[cfg(not(unix))]
        return; // symlink test is unix-only

        let h_real = attach_or_create(cfg(&real), Writability::Writable).unwrap();
        let h_link = attach_or_create(cfg(&link), Writability::ReadOnly).unwrap();
        assert_eq!(
            h_real.engine.upgrade().unwrap().ptr_id(),
            h_link.engine.upgrade().unwrap().ptr_id()
        );
        release(&h_real);
        release(&h_link);
    }

    #[test]
    fn case_variant_attaches_to_one_engine_on_case_insensitive_fs() {
        let _drop_guard = DROP_COUNT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().unwrap();
        let lower = tmp.path().join("casedb");
        std::fs::create_dir(&lower).unwrap();
        let upper = tmp.path().join("CASEDB");
        // Only meaningful where the FS is case-insensitive (macOS APFS default).
        if std::fs::canonicalize(&upper).is_err() {
            return; // case-sensitive FS: skip
        }
        let h_lower = attach_or_create(cfg(&lower), Writability::Writable).unwrap();
        let h_upper = attach_or_create(cfg(&upper), Writability::ReadOnly).unwrap();
        assert_eq!(
            h_lower.engine.upgrade().unwrap().ptr_id(),
            h_upper.engine.upgrade().unwrap().ptr_id()
        );
        release(&h_lower);
        release(&h_upper);
    }

    #[test]
    fn concurrent_first_open_creates_exactly_one_engine() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let p = p.clone();
            let b = barrier.clone();
            threads.push(std::thread::spawn(move || {
                b.wait();
                let h = attach_or_create(
                    EngineConfig { path: p, cache_size_bytes: None },
                    Writability::ReadOnly,
                )
                .unwrap();
                h.engine.upgrade().unwrap().ptr_id()
            }));
        }
        let ids: Vec<usize> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(ids.iter().all(|id| *id == ids[0]), "all share one engine: {ids:?}");
        assert_eq!(refs_for(&canonical_key(&p)), 8);
    }

    #[test]
    fn teardown_reopen_race_never_errors() {
        let _drop_guard = DROP_COUNT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("db");
        let mut threads = Vec::new();
        for _ in 0..6 {
            let p = p.clone();
            threads.push(std::thread::spawn(move || {
                // Each cycle is a real fjall open + teardown (joins background
                // threads, ~0.2s). The teardown/reopen race is hit within the
                // first few cycles, so keep the per-thread count small — 6 threads
                // hammering one path still overlaps teardown with reopen many times.
                for _ in 0..12 {
                    let h = attach_or_create(
                        EngineConfig { path: p.clone(), cache_size_bytes: None },
                        Writability::Writable,
                    )
                    .expect("open must not error under teardown/reopen race");
                    release(&h);
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        // All handles released → no live slot remains for this path.
        assert_eq!(refs_for(&canonical_key(&p)), 0);
    }
}
