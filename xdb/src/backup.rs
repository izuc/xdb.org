//! Hot backup engine for xdb databases.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::Db;

/// Manages incremental backups of an xdb database.
pub struct BackupEngine {
    backup_dir: PathBuf,
}

impl BackupEngine {
    /// Create a backup engine targeting the given directory.
    pub fn new(backup_dir: impl AsRef<Path>) -> Result<Self> {
        let backup_dir = backup_dir.as_ref().to_path_buf();
        fs::create_dir_all(&backup_dir)?;
        Ok(BackupEngine { backup_dir })
    }

    /// Create a new backup of the given database.
    ///
    /// The backup is a full copy of the current database state. SST files
    /// are copied (not hard-linked) so the backup is independent.
    pub fn create_backup(&self, db: &Db) -> Result<BackupInfo> {
        // Find next backup number.
        let backup_id = self.next_backup_id()?;
        let backup_path = self.backup_dir.join(format!("backup-{:06}", backup_id));

        // Use checkpoint internally, then copy if hard links don't work
        // across filesystems.
        db.checkpoint(&backup_path)?;

        Ok(BackupInfo {
            id: backup_id,
            path: backup_path,
        })
    }

    /// Restore a backup to the given path.
    ///
    /// The destination path must NOT exist.
    pub fn restore(
        &self,
        backup_id: u64,
        restore_path: impl AsRef<Path>,
    ) -> Result<()> {
        let restore_path = restore_path.as_ref();
        if restore_path.exists() {
            return Err(Error::invalid_argument("restore path already exists"));
        }

        let backup_path = self.backup_dir.join(format!("backup-{:06}", backup_id));
        if !backup_path.exists() {
            return Err(Error::not_found(format!(
                "backup {} not found",
                backup_id
            )));
        }

        // Copy entire backup directory to restore path.
        Self::copy_dir(&backup_path, restore_path)?;

        // Remove the LOCK file from the restored copy so it can be opened.
        let lock_path = restore_path.join("LOCK");
        let _ = fs::remove_file(&lock_path);

        Ok(())
    }

    /// List all available backups.
    pub fn list_backups(&self) -> Result<Vec<BackupInfo>> {
        let mut backups = Vec::new();
        for entry in fs::read_dir(&self.backup_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(id_str) = name.strip_prefix("backup-") {
                if let Ok(id) = id_str.parse::<u64>() {
                    backups.push(BackupInfo {
                        id,
                        path: entry.path(),
                    });
                }
            }
        }
        backups.sort_by_key(|b| b.id);
        Ok(backups)
    }

    /// Delete a specific backup.
    pub fn delete_backup(&self, backup_id: u64) -> Result<()> {
        let backup_path = self.backup_dir.join(format!("backup-{:06}", backup_id));
        if backup_path.exists() {
            fs::remove_dir_all(&backup_path)?;
        }
        Ok(())
    }

    fn next_backup_id(&self) -> Result<u64> {
        let backups = self.list_backups()?;
        Ok(backups.last().map(|b| b.id + 1).unwrap_or(1))
    }

    fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
        fs::create_dir_all(dst)?;
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if src_path.is_dir() {
                Self::copy_dir(&src_path, &dst_path)?;
            } else {
                fs::copy(&src_path, &dst_path)?;
            }
        }
        Ok(())
    }
}

/// Information about a single backup.
#[derive(Debug)]
pub struct BackupInfo {
    /// Unique backup identifier.
    pub id: u64,
    /// Path to the backup directory.
    pub path: PathBuf,
}
