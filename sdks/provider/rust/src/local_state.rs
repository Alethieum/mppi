//! Private, bounded command/wallet intent files. Never provider session storage.
//!
//! The caller owns the directory and its backups. Locks serialize cooperating
//! local invocations; this is not a distributed lock. No credentials belong here.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StateError;
impl std::fmt::Display for StateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("local operation record unavailable, invalid or already in use")
    }
}
impl std::error::Error for StateError {}

pub(crate) struct LocalState {
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
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn open(path: impl AsRef<Path>, limit: usize) -> Result<Self, StateError> {
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
        lock.try_lock().map_err(|_| StateError)?;
        // Never unlink a lock inode: waiters must continue to name this same lock.
        Ok(Self {
            path,
            parent,
            _lock: lock,
            limit,
            poisoned: false,
        })
    }

    pub(crate) fn read(&self) -> Result<Option<Value>, StateError> {
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

    pub(crate) fn write(&mut self, value: &Value) -> Result<(), StateError> {
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
            fs::rename(&temp, &self.path).map_err(|_| StateError)?;
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
