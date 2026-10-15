use std::{collections::HashMap, path::PathBuf, sync::{Arc, atomic::AtomicU64}};

use crate::message::Message;

#[derive(Debug, Clone)]
pub struct PersistentJournal {
    file_path: PathBuf, // file containing journal entries
    max_file_in_kb: u64, // prevent file growing forever - in KB
    flush_on_write: bool, // write to file immediately upon receiving entry
}

impl PersistentJournal {
    pub fn new(file_path: PathBuf, max_file_in_kb: u64, flush_on_write: bool) -> Self {
        Self { file_path, max_file_in_kb, flush_on_write }
    }
}

#[derive(Debug, Clone)]
pub struct JournalConfig {
    capacity: usize, // max entries in-memory journal should take
    persistent: Option<PersistentJournal>, // whether to save to a file
}

impl JournalConfig {
    pub fn new(capacity: usize, persistent: Option<PersistentJournal>) -> Self {
        assert!(capacity > 0);
        Self { 
            capacity, 
            persistent,
        }
    }
}

#[derive(Debug)]
pub struct JournalEntry {
    ticket: u64,
    message: Message,
    pending_readers: AtomicU64,
}

impl JournalEntry {
    pub fn new(ticket: u64, message: Message, pending_readers: u64) -> Self {
        Self { 
            ticket, 
            message, 
            pending_readers: AtomicU64::new(pending_readers) 
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum JournalError {
    JournalFull,
    NoRecipientForMessage, // drop any message with 0 pending readers
    InvalidMetaData,
}

#[derive(Debug)]
pub struct Journal {
    config: JournalConfig,
    store: HashMap<u64, Arc<JournalEntry>>,
    persistent_store: Option<PersistentJournal>,
}

impl Journal {
    pub fn new(config: JournalConfig) -> Self {
        let persist_store = if config.persistent.is_some() {
            config.persistent.clone()
        } else {
            None
        };

        Self { 
            config,
            store: HashMap::new(),
            persistent_store: persist_store,
        }
    }
}