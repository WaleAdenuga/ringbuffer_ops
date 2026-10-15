use std::{collections::HashMap, fs::OpenOptions, io::Write, path::PathBuf, println, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use serde::Serialize;

use crate::{buffer::MigrationLease, message::{Message, MessageKind, TopicId}, registry::AgentDetails};

#[derive(Debug)]
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

#[derive(Debug, Serialize)]
struct PersistedMessage {
    topic_id: TopicId,
    kind: MessageKind,
    message_id: u64,
    version: u8,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct PersistentJournalEntry {
    ticket: u64,
    message: PersistedMessage,
    pending_readers: u64,
}

impl From<&Message> for PersistedMessage {
    fn from(value: &Message) -> Self {
        assert!(value.payload_len <= value.payload.len());
        Self { 
            topic_id: value.topic_id, 
            kind: value.kind, 
            message_id: value.message_id, 
            version: value.version, 
            payload: value.payload[..value.payload_len].to_vec() 
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum JournalError {
    JournalFull,
    DualEntry,
    NoRecipientForMessage, // drop any message with 0 pending readers
    InvalidEntry,
    NothingtoDrain,
    Serialization(serde_json::Error),
    Persistence,
    PersistentJournalFull,
    LeaseGenerationError,
}

pub enum JournalReadResult {
    NotFound, // no entry exists for given ticket
    Skip, // message not for this agent
    Delivered(Message), // delivered message to agent and decremented pending_readers
}

#[derive(Debug)]
pub struct Journal {
    config: JournalConfig,
    store: HashMap<u64, Arc<JournalEntry>>, // ticket (agent_read_cursor) to journaled entry
}

impl Journal {
    pub fn new(config: JournalConfig) -> Self {
        Self { 
            config,
            store: HashMap::new(),
        }
    }

    /// Only used by the safeguard - If the journal was set up with an active persistent store,
    /// this would also write to that store.
    /// Returns the provided lease on error, matching buffer behavior
    pub fn insert_lease(&mut self, lease: MigrationLease) -> Result<(), (JournalError, MigrationLease)> {
        // verify lease validity
        if lease.pending_readers == 0 {
            return Err((JournalError::NoRecipientForMessage, lease));
        }

        if self.store.len() >= self.config.capacity {
            return Err((JournalError::JournalFull, lease));
        }

        // Verify that this entry is not already recorded
        if self.store.contains_key(&lease.ticket) {
            return Err((JournalError::DualEntry, lease));
        }

        let entry = Arc::new(
            JournalEntry::new(
                lease.ticket, 
                lease.message, 
                lease.pending_readers
            )
        );

        // To think about: write to file before saving internally
        self.store.insert(lease.ticket, entry.clone());

        // Write to store if part of config
        if self.config.persistent.is_some() {
            if let Err(error) = self.write_to_persistent_store(&entry) {
                println!("Error while persisting entry, non-fatal: {:?}", error);
            }
        }

        Ok(())
    }

    /// So broker can read from the journal (TBD)
    /// 
    /// Returns a JournalReadResults that states whether the message was delivered, 
    /// whether this ticket doesn't exist or whether this ticket can be skipped
    /// 
    /// This cannot logically advance the agent cursor, that behavior is deferred to
    /// the agent or to the broker
    pub fn try_read(&mut self, agent: &AgentDetails) -> JournalReadResult {
        let Some(entry) = self.store.get(&agent.read_cursor.load(Ordering::Acquire)).cloned() else {
            return JournalReadResult::NotFound;
        };

        if entry.pending_readers.load(Ordering::Acquire) & agent.agent_bit != 0 {
            // This message is for this agent, decrement pending readers and remove if this is the last agent
            // recall that fetch_and would return the previous value
            let message = entry.message.clone();
            let previous = entry.pending_readers.fetch_and(!agent.agent_bit, Ordering::AcqRel);
            if previous == agent.agent_bit {
                self.remove_journal_entry(&entry.ticket);
            }
            return JournalReadResult::Delivered(message);
        } else {
            // message not for it, broker/agent should advance its own cursor
            return JournalReadResult::Skip;
        }
    }

    fn remove_journal_entry(&mut self, entry_ticket: &u64) {
        self.store.remove(entry_ticket);
    }

    /// Returns all messages in journal for the given agent.
    pub fn replay_from_journal(&mut self, agent: &AgentDetails) -> Vec<Message> {
        let mut affected_entries: Vec<Arc<JournalEntry>> = self.store.values()
            .filter(|entry| entry.pending_readers.load(Ordering::Acquire) & agent.agent_bit != 0)
            .cloned()
            .collect();

        affected_entries.iter().for_each(|entry| {
            let previous = entry.pending_readers.fetch_and(!agent.agent_bit, Ordering::AcqRel);
            //
            if previous == agent.agent_bit {
                self.store.remove(&entry.ticket);
            }
        });

        // Sort to make the return determinstic
        affected_entries.sort_by_key(|entry| entry.ticket);

        affected_entries.iter()
            .map(|entry| entry.message.clone())
            .collect()
    }

    pub fn drain_unregistering_agent(&mut self, agent: &AgentDetails) -> Result<(), JournalError> {
        let affected_entries: Vec<Arc<JournalEntry>> = self.store.values()
            .filter(|entry| entry.pending_readers.load(Ordering::Acquire) & agent.agent_bit != 0)
            .cloned()
            .collect();

        affected_entries.iter().for_each(|entry| {
            let previous = entry.pending_readers.fetch_and(!agent.agent_bit, Ordering::AcqRel);
            if previous == agent.agent_bit {
                self.store.remove(&entry.ticket);
            }
        });
        Ok(())
    }

    pub fn has_pending_reader(&self, agent_bit: u64) -> bool {
        self.store.values().any(|entry| entry.pending_readers.load(Ordering::Acquire) & agent_bit != 0)
    }

    fn write_to_persistent_store(&self, entry: &Arc<JournalEntry>) -> Result<(), JournalError> {
        let persisted_message = PersistedMessage::from(&entry.message);
        let persisted_entry = PersistentJournalEntry {
            ticket: entry.ticket, 
            message: persisted_message, 
            pending_readers: entry.pending_readers.load(Ordering::Acquire)
        };
        // earlier checks if it's some - so this should be safe
        let persistent_journal = self.config.persistent.as_ref().unwrap();
        
        let mut plain_json = serde_json::to_vec(&persisted_entry).map_err(JournalError::Serialization)?;
        plain_json.push(b'\n');

        // Creates a file at the given path if it didn't already exist
        let mut file = OpenOptions::new().create(true).append(true).open(&persistent_journal.file_path).map_err(|_| JournalError::Persistence)?;
        let current_size = file.metadata().map_err(|_| JournalError::Persistence)?.len();
        let projected_size = current_size.checked_add(plain_json.len() as u64).ok_or(JournalError::PersistentJournalFull)?;

        if projected_size > (persistent_journal.max_file_in_kb.checked_mul(1024).ok_or(JournalError::PersistentJournalFull)?) {
            return Err(JournalError::PersistentJournalFull);
        }

        file.write_all(&plain_json).map_err(|_| JournalError::Persistence)?;
        if persistent_journal.flush_on_write {
            file.sync_data().map_err(|_| JournalError::Persistence)?;
        }
        
        Ok(())
    }

    fn capacity(&self) -> usize {
        return self.config.capacity;
    }

    /// Returns the current pressure state in percentage format (function of live/capacity)
    fn pressure_percent(&self) -> usize {
        (self.store.len() * 100) / self.capacity()
    }

    /// What to recover the journal to when it's full
    /// 
    /// i.e. when the journal is at 100% capacity, remove entries until its pressure
    /// percentage is less than what's given in recovery_threshold
    pub fn discard_oldest_entries(&mut self, recovery_threshold: usize) -> bool {
        assert!(recovery_threshold <= 100);
        while self.pressure_percent() > recovery_threshold {
            let Some(ticket) = self.oldest_entry().cloned() else {
                return false;
            };
            self.store.remove_entry(&ticket);
        }
        true
    }

    pub fn discard_single_oldest_entry(&mut self) -> bool {
        let Some(ticket) = self.oldest_entry().cloned() else {
            return false;
        };
        self.store.remove_entry(&ticket);
        true
    }

    /// Returns the ticket of the oldest entry in the store
    fn oldest_entry(&self) -> Option<&u64> {
        // the store can't be empty so this shouldn't return None right
        // if it's empty, then pressure percent would be 0 and the while loop wouldn't run
        self.store.keys().min()
    }


}

#[cfg(test)]
mod tests {
    use std::{fs, sync::{atomic::AtomicU64, Arc}};

    use event_listener::Event;
    use uuid::Uuid;

    use crate::{buffer::MigrationLease, message::{MessageKind, TopicId}, registry::AgentDetails};

    use super::*;

    fn test_agent(agent_bit: u64) -> AgentDetails {
        AgentDetails::new(
            AtomicU64::new(0),
            Arc::new(Event::new()),
            Arc::new(Event::new()),
            agent_bit,
        )
    }

    fn lease(ticket: u64, message_id: u64, pending_readers: u64) -> MigrationLease {
        MigrationLease::new(
            (ticket % 2) as usize,
            ticket,
            Message::new(TopicId(1), MessageKind::Data, message_id, 1).unwrap(),
            pending_readers,
        )
    }

    #[test]
    fn replay_is_ticket_ordered_and_preserves_other_recipients() {
        let mut journal = Journal::new(JournalConfig::new(4, None));
        let first_agent = test_agent(0b01);
        let second_agent = test_agent(0b10);

        journal.insert_lease(lease(4, 4, 0b11)).unwrap();
        journal.insert_lease(lease(2, 2, 0b11)).unwrap();
        journal.insert_lease(lease(3, 3, 0b10)).unwrap();

        let first_messages = journal.replay_from_journal(&first_agent);
        assert_eq!(first_messages.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![2, 4]);
        assert!(journal.has_pending_reader(second_agent.agent_bit));

        let second_messages = journal.replay_from_journal(&second_agent);
        assert_eq!(second_messages.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![2, 3, 4]);
        assert!(!journal.has_pending_reader(second_agent.agent_bit));
        assert!(journal.store.is_empty());
    }

    #[test]
    fn try_read_delivers_the_current_ticket_and_removes_the_last_copy() {
        let mut journal = Journal::new(JournalConfig::new(1, None));
        let agent = test_agent(0b1);
        journal.insert_lease(lease(0, 11, agent.agent_bit)).unwrap();

        assert!(matches!(journal.try_read(&agent), JournalReadResult::Delivered(message) if message.message_id == 11));
        assert!(journal.store.is_empty());
        assert!(matches!(journal.try_read(&agent), JournalReadResult::NotFound));
    }

    #[test]
    fn try_read_skips_entries_for_another_agent_without_removing_them() {
        let mut journal = Journal::new(JournalConfig::new(1, None));
        let skipped_agent = test_agent(0b01);
        let recipient = test_agent(0b10);
        journal.insert_lease(lease(0, 12, recipient.agent_bit)).unwrap();

        assert!(matches!(journal.try_read(&skipped_agent), JournalReadResult::Skip));
        assert!(journal.has_pending_reader(recipient.agent_bit));
        assert!(matches!(journal.try_read(&recipient), JournalReadResult::Delivered(message) if message.message_id == 12));
        assert!(journal.store.is_empty());
    }

    #[test]
    fn duplicate_ticket_is_rejected_without_replacing_the_original_entry() {
        let mut journal = Journal::new(JournalConfig::new(2, None));
        journal.insert_lease(lease(7, 1, 0b1)).unwrap();
        let (error, returned_lease) = journal.insert_lease(lease(7, 2, 0b1)).unwrap_err();

        assert!(matches!(error, JournalError::DualEntry));
        assert_eq!(returned_lease.ticket, 7);
        assert_eq!(journal.store.len(), 1);
        assert_eq!(journal.replay_from_journal(&test_agent(0b1))[0].message_id, 1);
    }

    #[test]
    fn draining_one_agent_keeps_the_other_agents_recovery_copy() {
        let mut journal = Journal::new(JournalConfig::new(2, None));
        let departing_agent = test_agent(0b01);
        let remaining_agent = test_agent(0b10);

        journal.insert_lease(lease(0, 1, 0b11)).unwrap();
        journal.drain_unregistering_agent(&departing_agent).unwrap();

        assert!(!journal.has_pending_reader(departing_agent.agent_bit));
        assert!(journal.has_pending_reader(remaining_agent.agent_bit));
        assert_eq!(journal.replay_from_journal(&remaining_agent).len(), 1);
        assert!(journal.store.is_empty());
    }

    #[test]
    fn zero_recipient_lease_is_rejected_without_insertion() {
        let mut journal = Journal::new(JournalConfig::new(1, None));
        let (error, returned_lease) = journal.insert_lease(lease(7, 7, 0)).unwrap_err();

        assert!(matches!(error, JournalError::NoRecipientForMessage));
        assert_eq!(returned_lease.ticket, 7);
        assert!(journal.store.is_empty());
    }

    #[test]
    fn full_journal_returns_the_uninserted_lease() {
        let mut journal = Journal::new(JournalConfig::new(1, None));
        journal.insert_lease(lease(1, 1, 0b1)).unwrap();
        let (error, returned_lease) = journal.insert_lease(lease(2, 2, 0b1)).unwrap_err();

        assert!(matches!(error, JournalError::JournalFull));
        assert_eq!(returned_lease.ticket, 2);
        assert_eq!(journal.store.len(), 1);
    }

    #[test]
    fn discarding_an_empty_journal_is_a_noop() {
        let mut journal = Journal::new(JournalConfig::new(1, None));

        assert!(!journal.discard_single_oldest_entry());
        assert!(journal.store.is_empty());
    }

    #[test]
    fn discarding_one_entry_removes_the_lowest_ticket_only() {
        let mut journal = Journal::new(JournalConfig::new(3, None));
        let agent = test_agent(0b1);
        journal.insert_lease(lease(7, 7, agent.agent_bit)).unwrap();
        journal.insert_lease(lease(3, 3, agent.agent_bit)).unwrap();
        journal.insert_lease(lease(5, 5, agent.agent_bit)).unwrap();

        assert!(journal.discard_single_oldest_entry());

        let remaining = journal.replay_from_journal(&agent);
        assert_eq!(remaining.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![5, 7]);
    }

    #[test]
    fn persistent_journal_appends_a_serialized_entry() {
        let path = std::env::temp_dir().join(format!("ringbuffer-journal-{}.jsonl", Uuid::new_v4()));
        let persistent = PersistentJournal::new(path.clone(), 4, true);
        let mut journal = Journal::new(JournalConfig::new(2, Some(persistent)));
        let mut entry = lease(8, 44, 0b1);
        entry.message.set_payload(vec![3, 4, 5]).unwrap();

        journal.insert_lease(entry).unwrap();

        let contents = fs::read_to_string(&path).expect("persistent journal file was not created");
        assert_eq!(contents.lines().count(), 1);
        assert!(contents.contains("\"ticket\":8"));
        assert!(contents.contains("\"message_id\":44"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_persistence_does_not_prevent_in_memory_recovery() {
        let path = std::env::temp_dir().join(format!("ringbuffer-journal-dir-{}", Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        let persistent = PersistentJournal::new(path.clone(), 4, true);
        let mut journal = Journal::new(JournalConfig::new(2, Some(persistent)));
        let agent = test_agent(0b1);

        journal.insert_lease(lease(8, 44, agent.agent_bit)).unwrap();

        assert_eq!(journal.replay_from_journal(&agent)[0].message_id, 44);
        fs::remove_dir(path).unwrap();
    }

    #[test]
    fn draining_an_agent_with_no_recovery_messages_is_successful() {
        let mut journal = Journal::new(JournalConfig::new(1, None));
        assert!(journal.drain_unregistering_agent(&test_agent(0b1)).is_ok());
    }
}
