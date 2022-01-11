use crate::database::DataAttributes;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use lazy_static::lazy_static;

use std::collections::BTreeMap;
use std::fs::{create_dir_all, remove_file, File};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use tokio::sync::MutexGuard;

pub type RecordsData = BTreeMap<i64, DataAttributes>;

#[derive(Default, Debug)]
pub struct DataAbstraction {
    data: RecordsData,
    filename: Option<String>,
}

// Abstraction over data in database, represents data both in memory and on disk
impl DataAbstraction {
    pub fn load_from_disk(&mut self) {
        if let Some(path) = &self.filename {
            let file = std::fs::File::open(path).unwrap();
            self.data = serde_json::from_reader(file).unwrap();
        }
    }

    pub fn save_on_disk(&mut self) {
        if self.filename.is_none() {
            self.filename = Some(get_unique_filename());
        }
        let path = self.filename.as_ref().unwrap();
        let file = File::create(path).unwrap();
        serde_json::to_writer_pretty(file, &self.data).unwrap();
        // Clear data from memory
        self.data.clear();
    }

    pub fn clear_from_disk(&mut self) {
        if let Some(path) = &self.filename {
            remove_file(path).unwrap();
            self.filename = None;
        }
    }

    pub fn in_memory(&self) -> bool {
        // If filename is None data is only present in memory
        self.filename.is_none()
    }
}

impl Drop for DataAbstraction {
    fn drop(&mut self) {
        // Delete file from disk at the end of lifetime
        self.clear_from_disk();
    }
}

pub struct DataAbstractionLock<'a> {
    guard: MutexGuard<'a, DataAbstraction>,
}

impl<'a> DataAbstractionLock<'a> {
    pub fn new(guard: MutexGuard<'a, DataAbstraction>) -> Self {
        let mut lock = DataAbstractionLock { guard };
        if !lock.guard.in_memory() {
            // If data is on disk, load it to memory
            lock.guard.load_from_disk();
        }
        lock
    }
}

impl<'a> Drop for DataAbstractionLock<'a> {
    fn drop(&mut self) {
        match (crate::allocator::out_of_memory(), self.guard.in_memory()) {
            (true, _) => {
                // Memory limit exceeded, move data from memory to disk
                self.guard.save_on_disk();
            }
            (false, false) => {
                // Memory available, clear data on disk and keep it in memory
                self.guard.clear_from_disk();
            }
            (false, true) => {
                // Memory available, keep data in memory
            }
        }
    }
}

impl<'a> Deref for DataAbstractionLock<'a> {
    type Target = RecordsData;
    fn deref(&self) -> &Self::Target {
        &self.guard.data
    }
}

impl<'a> DerefMut for DataAbstractionLock<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard.data
    }
}

const DEFAULT_DATA_PATH: &str = "./.db_data";

lazy_static! {
    static ref FILE_COUNTER: RelaxedCounter = RelaxedCounter::new(0);
    static ref DATA_PATH: String =
        std::env::var("DATA_PATH").unwrap_or_else(|_| DEFAULT_DATA_PATH.to_string());
}

fn get_unique_filename() -> String {
    if !Path::new(&*DATA_PATH).exists() {
        create_dir_all(&*DATA_PATH).ok();
    }
    format!("{}/tabledata_{}", *DATA_PATH, FILE_COUNTER.inc())
}
