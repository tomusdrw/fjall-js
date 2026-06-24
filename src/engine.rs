use std::path::{Path, PathBuf};

#[allow(dead_code)]
pub type CanonicalKey = PathBuf;

/// Best-effort canonical registry key. Resolves symlinks/case/relative parts
/// when `path` exists; otherwise canonicalizes the nearest existing ancestor and
/// re-appends the remaining components. Never fails.
#[allow(dead_code)]
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
