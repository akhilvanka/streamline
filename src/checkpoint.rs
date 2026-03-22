use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type CheckpointId = u64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CheckpointManifest {
    pub id:          CheckpointId,
    pub timestamp:   u64,           // Unix ms
    pub operator_count: usize,
    pub operators:   HashMap<String, String>, // name → state JSON
}

impl CheckpointManifest {
    pub fn new(id: CheckpointId, states: HashMap<String, String>) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            id,
            timestamp: ts,
            operator_count: states.len(),
            operators: states,
        }
    }
}

/// Manages checkpointing for a pipeline.
pub struct CheckpointStore {
    dir:         PathBuf,
    last_id:     Arc<Mutex<CheckpointId>>,
    interval:    Duration,
}

impl CheckpointStore {
    pub fn new(dir: impl AsRef<Path>, interval: Duration) -> anyhow::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir, last_id: Arc::new(Mutex::new(0)), interval })
    }

    /// Atomically write a checkpoint (write to tmp, then rename for crash safety)
    pub fn save(&self, states: HashMap<String, String>) -> anyhow::Result<CheckpointId> {
        let mut id_guard = self.last_id.lock().unwrap();
        *id_guard += 1;
        let id = *id_guard;

        let manifest = CheckpointManifest::new(id, states);
        let json     = serde_json::to_string_pretty(&manifest)?;

        let tmp  = self.dir.join(format!("checkpoint_{:08}.json.tmp", id));
        let dest = self.dir.join(format!("checkpoint_{:08}.json", id));

        fs::write(&tmp, &json)?;
        fs::rename(&tmp, &dest)?; // atomic on POSIX

        Ok(id)
    }

    /// Load the most recent valid checkpoint, or None if none exists.
    pub fn load_latest(&self) -> anyhow::Result<Option<CheckpointManifest>> {
        let mut entries: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .collect();

        entries.sort();

        for path in entries.iter().rev() {
            match fs::read_to_string(path) {
                Ok(json) => match serde_json::from_str::<CheckpointManifest>(&json) {
                    Ok(m) => return Ok(Some(m)),
                    Err(_) => { let _ = fs::remove_file(path); } // corrupt — delete
                },
                Err(_) => {}
            }
        }
        Ok(None)
    }

    /// Clean up old checkpoints, keeping only the last `keep` ones.
    pub fn gc(&self, keep: usize) -> anyhow::Result<()> {
        let mut entries: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .collect();
        entries.sort();
        if entries.len() > keep {
            for old in &entries[..entries.len() - keep] {
                let _ = fs::remove_file(old);
            }
        }
        Ok(())
    }

    pub fn interval(&self) -> Duration { self.interval }
}

/// Trait that operators implement to save/restore their state.
pub trait Checkpointable {
    fn save_state(&self) -> serde_json::Value;
    fn restore_state(&mut self, state: &serde_json::Value);
}
