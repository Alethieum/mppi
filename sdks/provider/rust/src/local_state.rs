//! Locked, crash-safe local records for wallet operation journals.
//!
//! A [`TransactionWallet`](crate::capabilities::TransactionWallet) must durably
//! bind each operation reference to its exact call before submitting it. The
//! Circle adapter and the CLI keep their records with [`LocalState`]: one JSON
//! object, exclusively locked while a handle is open and replaced atomically on
//! every write. Never provider session storage.
//!
//! The caller owns the directory and its backups. Locks serialize cooperating
//! local invocations; this is not a distributed lock. No credentials belong here.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use serde_json::Value;

/// How long `open` waits for a lock another descriptor still holds. A lock
/// belongs to its open file description (flock(2)), and a child process spawned
/// by any thread holds a copy of every open descriptor until it execs, so a lock
/// this process has just released can stay held for that moment. A second live
/// holder is still refused once the wait ends.
pub(crate) const LOCK_WAIT: Duration = Duration::from_secs(1);

/// Unit tests only: records whose next `write` fails after writing its
/// replacement, and whether after renaming it over the record. Before the
/// rename, as a full disk or a crash would, the record is unchanged; after it,
/// as a failed directory sync would, the replacement is in place although the
/// write reports failure. Tests may run as root, which no directory
/// permission refuses.
#[cfg(test)]
static FAILING_WRITES: std::sync::Mutex<Vec<(PathBuf, bool)>> = std::sync::Mutex::new(Vec::new());
#[cfg(test)]
pub(crate) fn fail_next_write(path: &Path) {
    let path = path.canonicalize().expect("an existing record");
    FAILING_WRITES.lock().unwrap().push((path, false));
}
#[cfg(test)]
pub(crate) fn fail_next_write_after_rename(path: &Path) {
    let path = path.canonicalize().expect("an existing record");
    FAILING_WRITES.lock().unwrap().push((path, true));
}
#[cfg(test)]
fn failing(path: &Path, renamed: bool) -> bool {
    let mut failing = FAILING_WRITES.lock().unwrap();
    let index = failing
        .iter()
        .position(|(failing, after)| failing == path && *after == renamed);
    index.map(|index| failing.swap_remove(index)).is_some()
}

/// The record is unavailable, unsafe, invalid or held by another handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateError;
impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("local operation record unavailable, invalid or already in use")
    }
}
impl std::error::Error for StateError {}

/// One bounded record, exclusively owned while this handle lives.
#[derive(Debug)]
pub struct LocalState {
    path: PathBuf,
    parent: PathBuf,
    _lock: File,
    limit: usize,
    poisoned: bool,
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn check_file(file: &File, path: &Path) -> Result<(), StateError> {
    let actual = file.metadata().map_err(|_| StateError)?;
    let linked = fs::symlink_metadata(path).map_err(|_| StateError)?;
    if !actual.is_file() || !linked.is_file() {
        return Err(StateError);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if actual.dev() != linked.dev()
            || actual.ino() != linked.ino()
            || actual.nlink() != 1
            || actual.mode() & 0o077 != 0
        {
            return Err(StateError);
        }
    }
    Ok(())
}

impl LocalState {
    /// The record's path, in its canonical directory.
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Opens the record at `path`, at most `limit` bytes (1 byte to 16 MiB),
    /// and locks it exclusively until this handle is dropped. The directory must
    /// exist and must not be group- or world-writable. The lock is the sibling
    /// `<file>.lock`, created mode 0600 and never removed. Waits up to one second
    /// for a lock another descriptor holds, then refuses the record as in use.
    pub fn open(path: impl AsRef<Path>, limit: usize) -> Result<Self, StateError> {
        Self::open_within(path, limit, LOCK_WAIT)
    }
    /// [`Self::open`], waiting up to `wait` for a lock another descriptor
    /// holds. It sleeps the calling thread, so async callers run it on a
    /// blocking thread.
    pub(crate) fn open_within(
        path: impl AsRef<Path>,
        limit: usize,
        wait: Duration,
    ) -> Result<Self, StateError> {
        if limit == 0 || limit > 16 * 1024 * 1024 {
            return Err(StateError);
        }
        let path = path.as_ref();
        let filename = path
            .file_name()
            .filter(|n| !n.is_empty())
            .ok_or(StateError)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = parent.canonicalize().map_err(|_| StateError)?;
        let metadata = fs::metadata(&parent).map_err(|_| StateError)?;
        if !metadata.is_dir() {
            return Err(StateError);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.mode() & 0o022 != 0 {
                return Err(StateError);
            }
        }
        let path = parent.join(filename);
        let mut lock_name = filename.to_os_string();
        lock_name.push(".lock");
        let lock_path = parent.join(lock_name);
        let lock = private_options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|_| StateError)?;
        check_file(&lock, &lock_path)?;
        let deadline = Instant::now() + wait;
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return Err(StateError),
            }
        }
        // Never unlink a lock inode: waiters must continue to name this same lock.
        Ok(Self {
            path,
            parent,
            _lock: lock,
            limit,
            poisoned: false,
        })
    }

    /// The record, or `None` before its first write. Refuses a record that is
    /// not a regular, single-link file without group or other permissions, that
    /// exceeds the limit, or that is not one strict JSON object (UTF-8, no
    /// duplicate keys, nesting at most 32).
    pub fn read(&self) -> Result<Option<Value>, StateError> {
        if self.poisoned {
            return Err(StateError);
        }
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Ok(metadata) if metadata.is_file() && metadata.len() <= self.limit as u64 => {}
            _ => return Err(StateError),
        }
        let file = File::open(&self.path).map_err(|_| StateError)?;
        check_file(&file, &self.path)?;
        let mut bytes = Vec::new();
        file.take(self.limit as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| StateError)?;
        let value = crate::json::parse_document(&bytes, self.limit, 32).map_err(|_| StateError)?;
        if !value.is_object() {
            return Err(StateError);
        }
        Ok(Some(value))
    }

    /// Replaces the record with `value`, a JSON object within the limit. It is
    /// durable when this returns: written to a new mode-0600 file, synced,
    /// renamed over the record, and the directory synced. After any failure the
    /// handle refuses every later read and write, since the rename may already
    /// have happened; reopen the record to see what is durable.
    pub fn write(&mut self, value: &Value) -> Result<(), StateError> {
        if self.poisoned || !value.is_object() {
            return Err(StateError);
        }
        let bytes = serde_json::to_vec(value).map_err(|_| StateError)?;
        crate::json::parse_document(&bytes, self.limit, 32).map_err(|_| StateError)?;
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| StateError)?;
        let name = format!(".mppi-{}.tmp", alloy_primitives::hex::encode(nonce));
        let temp = self.parent.join(name);
        let result = (|| {
            let mut file = private_options()
                .write(true)
                .create_new(true)
                .open(&temp)
                .map_err(|_| StateError)?;
            file.write_all(&bytes).map_err(|_| StateError)?;
            file.sync_all().map_err(|_| StateError)?;
            #[cfg(test)]
            if failing(&self.path, false) {
                return Err(StateError);
            }
            fs::rename(&temp, &self.path).map_err(|_| StateError)?;
            #[cfg(test)]
            if failing(&self.path, true) {
                return Err(StateError);
            }
            File::open(&self.parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| StateError)?;
            Ok(())
        })();
        if result.is_err() {
            // A rename can succeed before directory fsync fails. Do not continue
            // on either the old in-memory state or a guessed durable outcome.
            self.poisoned = true;
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn directory() -> PathBuf {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "mppi-intent-{}",
            alloy_primitives::hex::encode(nonce)
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn intent_is_locked_durable_bounded_and_strict() {
        let directory = directory();
        let path = directory.join("intent.json");
        let mut record = LocalState::open(&path, 128).unwrap();
        assert_eq!(record.read().unwrap(), None);
        assert!(LocalState::open(&path, 128).is_err());
        record.write(&json!({"operation":"original"})).unwrap();
        assert!(record.write(&json!({"oversized":"a".repeat(128)})).is_err());
        assert_eq!(record.read().unwrap().unwrap()["operation"], "original");
        drop(record);
        let record = LocalState::open(&path, 128).unwrap();
        assert_eq!(record.read().unwrap().unwrap()["operation"], "original");
        fs::write(&path, br#"{"a":1,"a":2}"#).unwrap();
        assert!(record.read().is_err());
        drop(record);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_lock_held_only_by_an_inherited_copy_is_awaited() {
        let directory = directory();
        let path = directory.join("intent.json");
        let record = LocalState::open(&path, 128).unwrap();
        // A child process spawned meanwhile holds a copy until it execs.
        let inherited = record._lock.try_clone().unwrap();
        drop(record);
        let exec = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(inherited);
        });
        LocalState::open(&path, 128).unwrap();
        exec.join().unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn linked_and_permissive_records_are_rejected() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = directory();
        let path = directory.join("intent.json");
        let record = LocalState::open(&path, 128).unwrap();
        let target = directory.join("other.json");
        fs::write(&target, b"{}").unwrap();
        symlink(&target, &path).unwrap();
        assert!(record.read().is_err());
        fs::remove_file(&path).unwrap();
        fs::hard_link(&target, &path).unwrap();
        assert!(record.read().is_err());
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"{}").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(record.read().is_err());
        drop(record);
        fs::remove_dir_all(directory).unwrap();
    }
}
