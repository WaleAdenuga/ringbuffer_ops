use std::{sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, thread::{self, JoinHandle}};

use event_listener::{Event, Listener};
use crate::{buffer::{RingBuffer, RingBufferError}, journal::{Journal, JournalError}, registry::Registry};

const SPINS_BEFORE_YIELD: usize = 64;

#[derive(Debug, PartialEq, Eq)]
enum StrikeOutcome { // for monitoring journal/buffer movement 
    UnmovableMessage,
    JournalFull,
    Recovered,
    SafeguardStopped,
    JournalError,
    UnexpectedBufferError,
    UnmovableSlot,
}

#[derive(Debug)]
enum SafeguardMode {
    Normal, // sleep
    Warning, // notification to all pending readers
    FirstStrike, // move messages to journal
}

#[derive(Debug)]
pub struct SafeguardConfig {
    warning_threshold: usize, // when to send final notification to buffer's pending readers
    journal_threshold: usize, // when to move things to the journal
    recover_threshold: usize, // when to reset safeguard to normal
}

impl SafeguardConfig {
    pub fn new(warning_threshold: usize, journal_threshold: usize, recover_threshold: usize) -> Self  {
        
        assert!(recover_threshold < warning_threshold);
        assert!(warning_threshold < journal_threshold);
        assert!(journal_threshold <= 100);
        
        Self { 
            warning_threshold, 
            journal_threshold, 
            recover_threshold, 
        }
    }
}

// Safeguard properties - shared from broker
#[derive(Debug)]
pub struct Safeguard {
    buffer: Arc<RingBuffer>,
    registry: Arc<Mutex<Registry>>,
    journal: Arc<Mutex<Journal>>,
    wake_event: Arc<Event>,
    config: SafeguardConfig,
    check_requested: AtomicBool,
    stop_requested: AtomicBool,
    //safeguard: AtomicBool, // journal is getting full, purge
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Safeguard {
    pub fn new(buffer: Arc<RingBuffer>, registry: Arc<Mutex<Registry>>, journal: Arc<Mutex<Journal>>, config: SafeguardConfig) -> Self {
        Self { 
            buffer, 
            registry, 
            journal, 
            wake_event: Arc::new(Event::new()), 
            config,
            check_requested: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false), 
            //safeguard: AtomicBool::new(false), 
            worker: Mutex::new(None), 
        }
    }

    pub fn start(self: &Arc<Self>) {
        let mut worker = self.worker.lock().unwrap();
        
        // prevent duplicate starts
        if worker.is_some() || self.stop_requested.load(Ordering::Acquire) {
            return;
        }
        let thread_safeguard = Arc::clone(self);
        let handle = thread::spawn(move || {
            thread_safeguard.worker_loop();
        });

        *worker = Some(handle);
    }

    fn worker_loop(&self) {
        let mut mode = SafeguardMode::Normal;
        loop {
            let listener = self.wake_event.listen();

            if self.stop_requested.load(Ordering::Acquire) {
                break;
            }

            // puts false into check_requested (clearing it), returning the previous value
            if self.check_requested.swap(false, Ordering::AcqRel) {
                self.check_buffer(&mut mode);
                continue;
            } else {
                listener.wait(); // blocks until this listener is notified
            }
        }
    }

    pub fn request_check(&self) {
        // puts true into requested, returning the previous value
        let requested = self.check_requested.swap(true, Ordering::Release);
        if !requested {
            // unblock the worker loop
            self.wake_event.notify(1);
        }
    }

    /// Helps unblock slot due to slow reader hogging a slot
    /// 
    /// First we check the position of this supposed hole (head % capacity)
    /// If it's fully ready (published msg, proper readers, seqno updated etc), 
    /// then it's guaranteed to be the oldest slot active. We then proceed on the normal
    /// migration path and return true on success, false on failure
    /// 
    /// If it's not fully ready, then a different writer has claimed this slot but hasn't written 
    /// the message, we return false on that
    pub fn request_check_write(&self) -> bool {
        let (hole_index, ticket) = self.buffer.get_slow_reader_plug();
        if self.buffer.confirm_slot_can_migrate(hole_index) {
            loop {
                match self.migrate_one_safe_message(Some((hole_index, ticket))) {
                    Ok(_) => return true,
                    Err(StrikeOutcome::JournalFull) => {
                        let mut journal = self.journal.lock().unwrap();
                        if !journal.discard_single_oldest_entry() {
                            drop(journal);
                            return false;
                        }
                        drop(journal);
                        continue;
                    }
                    Err(_) => return false
                }   
            }
        }
        false
    }

    fn check_buffer(&self, mode: &mut SafeguardMode) {
        let buffer_pressure = self.buffer.pressure_percent();

        if buffer_pressure <= self.config.recover_threshold {
            *mode = SafeguardMode::Normal;
            return;
        }

        match *mode {
            SafeguardMode::Normal => {
                if buffer_pressure >= self.config.journal_threshold {
                    // write to journal, but warn for final time
                    self.warn_pending_agents(None, false);
                    self.strike_until_recovered(mode);
                } else if buffer_pressure >= self.config.warning_threshold {
                    // warn agents that there's a message
                    *mode = SafeguardMode::Warning;
                    self.warn_pending_agents(None, false);
                }
            },

            SafeguardMode::Warning => {
                if buffer_pressure >= self.config.journal_threshold {
                    // write to journal
                    self.strike_until_recovered(mode);
                } 
            },

            SafeguardMode::FirstStrike => {
                self.strike_until_recovered(mode);
            },
        }
    }

    fn warn_pending_agents(&self, pending_readers: Option<u64>, journal_message: bool) {
        // Get all agents with pending messages on the ring
        let pending_agents = pending_readers.unwrap_or(self.buffer.all_pending_agents());
        let registry = self.registry.lock().unwrap(); // same registry lock used by the broker
        let listeners = registry.notify_from_agent_bits(pending_agents, journal_message);
        drop(registry); // drop asap

        listeners.iter().for_each(|listener| {
            listener.notify(1);
        });
    }

    fn strike_until_recovered(&self, mode: &mut SafeguardMode) {
        *mode = SafeguardMode::FirstStrike;

        let outcome = self.write_to_journal();

        if outcome == StrikeOutcome::Recovered {
            *mode = SafeguardMode::Normal;
        }
    }

    fn write_to_journal(&self) -> StrikeOutcome {
        let mut spins = 0;
        while self.buffer.pressure_percent() > self.config.recover_threshold {
            if self.stop_requested.load(Ordering::Acquire) {
                return StrikeOutcome::SafeguardStopped;
            }

            if let Err(error) = self.migrate_one_safe_message(None) {
                // In this scenario, try another slot
                if error == StrikeOutcome::UnmovableSlot || error == StrikeOutcome::UnmovableMessage {
                    // don't burn a cpu core while waiting for an unmovable message
                    spins += 1;
                    if spins < SPINS_BEFORE_YIELD {
                        std::hint::spin_loop();
                    } else {
                        spins = 0;
                        std::thread::yield_now();
                    }
                    continue;
                } else if error == StrikeOutcome::JournalFull {
                    // clear oldest journal entry
                    let mut journal = self.journal.lock().unwrap();
                    if !journal.discard_single_oldest_entry() {
                        continue;
                    }
                    //journal.discard_oldest_entries(self.config.recover_threshold);
                    drop(journal);
                    continue;
                }
                return error;
            }
        }

        StrikeOutcome::Recovered
    }

    fn migrate_one_safe_message(&self, target_info: Option<(usize, u64)>) -> Result<(), StrikeOutcome> {

        let lease = match self.buffer.begin_migration(target_info) {
            Ok(lease) => lease,
            Err(error) => return Err(self.map_buffer_error(error)),
        };

        let slot_index = lease.slot_index;
        let ticket = lease.ticket;
        let pending_readers = lease.pending_readers;

        let mut journal = self.journal.lock().unwrap();

        match journal.insert_lease(lease) {
            Ok(_) => {
                // free the slot on the ring and alert pending readers
                drop(journal);
                self.buffer.confirm_migration(slot_index, ticket);
                self.warn_pending_agents(Some(pending_readers), true);
            }, 
            Err((error, lease)) => {
                self.buffer.abort_migration(lease);
                return Err(self.map_journal_error(error));
            }
        }
        
        Ok(())
    }

    fn map_buffer_error(&self, error: RingBufferError) -> StrikeOutcome {
        match error {
            RingBufferError::SlotMismatch | RingBufferError::SlotUnavailableForMigration | RingBufferError::NoSlotToMigrate => StrikeOutcome::UnmovableSlot,
            _ => StrikeOutcome::UnexpectedBufferError,
        }
    }

    fn map_journal_error(&self, error: JournalError) -> StrikeOutcome {
        match error {
            JournalError::JournalFull => StrikeOutcome::JournalFull,
            JournalError::InvalidEntry | JournalError::NoRecipientForMessage => StrikeOutcome::UnmovableMessage,
            _ => StrikeOutcome::JournalError,
        }
    }

    pub fn shutdown(&self) {
        self.stop_requested.store(true, Ordering::Release);
        self.wake_event.notify(1); //notify to break the worker_loop
        // take() takes the value out of the worker, puts None in worker and puts it into handle
        let handle = self.worker.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }

}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicU64, Arc, Mutex};

    use event_listener::Event;

    use crate::{buffer::MigrationLease, journal::JournalConfig, message::{Message, MessageKind, TopicId}, registry::AgentDetails};

    use super::*;

    fn message(message_id: u64) -> Message {
        Message::new(TopicId(1), MessageKind::Data, message_id, 1).unwrap()
    }

    fn test_agent(agent_bit: u64) -> AgentDetails {
        AgentDetails::new(
            AtomicU64::new(0),
            Arc::new(Event::new()),
            Arc::new(Event::new()),
            agent_bit,
        )
    }

    fn test_safeguard(journal_capacity: usize) -> (Arc<RingBuffer>, Arc<Mutex<Journal>>, Safeguard) {
        let buffer = Arc::new(RingBuffer::new());
        let registry = Arc::new(Mutex::new(Registry::new()));
        let journal = Arc::new(Mutex::new(Journal::new(JournalConfig::new(journal_capacity, None))));
        let safeguard = Safeguard::new(
            Arc::clone(&buffer),
            registry,
            Arc::clone(&journal),
            SafeguardConfig::new(80, 90, 70),
        );
        (buffer, journal, safeguard)
    }

    fn fill(buffer: &RingBuffer, count: u64, reader_mask: u64) {
        for message_id in 1..=count {
            buffer.try_write(message(message_id), reader_mask).unwrap();
        }
    }

    #[test]
    fn empty_buffer_resets_warning_mode_to_normal() {
        let (_, _, safeguard) = test_safeguard(200);
        let mut mode = SafeguardMode::Warning;

        safeguard.check_buffer(&mut mode);

        assert!(matches!(mode, SafeguardMode::Normal));
    }

    #[test]
    fn below_warning_threshold_does_not_warn_or_spill() {
        let (buffer, journal, safeguard) = test_safeguard(200);
        fill(&buffer, 399, 0b1);

        let mut mode = SafeguardMode::Normal;
        safeguard.check_buffer(&mut mode);

        assert!(matches!(mode, SafeguardMode::Normal));
        assert_eq!(buffer.pressure_percent(), 79);
        assert!(journal.lock().unwrap().replay_from_journal(&test_agent(0b1)).is_empty());
    }

    #[test]
    fn warning_threshold_does_not_spill_before_first_strike() {
        let (buffer, journal, safeguard) = test_safeguard(200);
        fill(&buffer, 400, 0b1);

        let mut mode = SafeguardMode::Normal;
        safeguard.check_buffer(&mut mode);

        assert!(matches!(mode, SafeguardMode::Warning));
        assert_eq!(buffer.pressure_percent(), 80);
        assert!(journal.lock().unwrap().replay_from_journal(&test_agent(0b1)).is_empty());
    }

    #[test]
    fn warning_mode_recovers_after_readers_reduce_pressure() {
        let (buffer, journal, safeguard) = test_safeguard(200);
        let agent = test_agent(0b1);
        fill(&buffer, 400, agent.agent_bit);
        let mut mode = SafeguardMode::Normal;
        safeguard.check_buffer(&mut mode);
        assert!(matches!(mode, SafeguardMode::Warning));

        for _ in 0..50 {
            buffer.try_read(&agent).unwrap();
        }
        safeguard.check_buffer(&mut mode);

        assert!(buffer.pressure_percent() <= 70);
        assert!(matches!(mode, SafeguardMode::Normal));
        assert!(journal.lock().unwrap().replay_from_journal(&agent).is_empty());
    }

    #[test]
    fn first_strike_spills_oldest_messages_until_recovery_threshold() {
        let (buffer, journal, safeguard) = test_safeguard(200);
        fill(&buffer, 450, 0b1);

        let mut mode = SafeguardMode::Normal;
        safeguard.check_buffer(&mut mode);

        assert!(matches!(mode, SafeguardMode::Normal));
        assert!(buffer.pressure_percent() <= 70);
        let recovered = journal.lock().unwrap().replay_from_journal(&test_agent(0b1));
        assert_eq!(recovered.len(), 96);
        assert_eq!(recovered.first().unwrap().message_id, 1);
        assert_eq!(recovered.last().unwrap().message_id, 96);
    }

    #[test]
    fn direct_migration_moves_the_oldest_live_message_to_the_journal() {
        let (buffer, journal, safeguard) = test_safeguard(2);
        let agent = test_agent(0b1);
        fill(&buffer, 2, agent.agent_bit);

        safeguard.migrate_one_safe_message(None).unwrap();

        assert_eq!(buffer.pressure_percent(), 0);
        let recovered = journal.lock().unwrap().replay_from_journal(&agent);
        assert_eq!(recovered.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![1]);
        assert_eq!(buffer.try_read(&agent).unwrap().message_id, 2);
    }

    #[test]
    fn targeted_migration_does_not_choose_the_global_oldest_slot() {
        let (buffer, journal, safeguard) = test_safeguard(2);
        let agent = test_agent(0b1);
        fill(&buffer, 2, agent.agent_bit);

        safeguard.migrate_one_safe_message(Some((1, 1))).unwrap();

        let recovered = journal.lock().unwrap().replay_from_journal(&agent);
        assert_eq!(recovered.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![2]);
        assert_eq!(buffer.try_read(&agent).unwrap().message_id, 1);
    }

    #[test]
    fn write_relief_migrates_a_low_pressure_head_blocker() {
        let (buffer, journal, safeguard) = test_safeguard(2);
        let slow_reader = test_agent(0b1);
        let fast_reader = test_agent(0b10);

        buffer.try_write(message(1), slow_reader.agent_bit).unwrap();
        for message_id in 2..=500 {
            buffer.try_write(message(message_id), fast_reader.agent_bit).unwrap();
        }
        for message_id in 2..=500 {
            assert_eq!(buffer.try_read(&fast_reader).unwrap().message_id, message_id);
        }

        assert_eq!(buffer.pressure_percent(), 0);
        assert!(matches!(
            buffer.try_write(message(501), fast_reader.agent_bit),
            Err((RingBufferError::BufferFull, _))
        ));

        assert!(safeguard.request_check_write());
        let recovered = journal.lock().unwrap().replay_from_journal(&slow_reader);
        assert_eq!(recovered.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![1]);
        assert!(buffer.try_write(message(501), fast_reader.agent_bit).is_ok());
        assert_eq!(buffer.try_read(&fast_reader).unwrap().message_id, 501);
    }

    #[test]
    fn write_relief_discards_one_journal_entry_then_retries() {
        let (buffer, journal, safeguard) = test_safeguard(1);
        let agent = test_agent(0b1);
        journal.lock().unwrap().insert_lease(MigrationLease::new(999, 999, message(999), agent.agent_bit)).unwrap();
        fill(&buffer, 500, agent.agent_bit);

        assert!(safeguard.request_check_write());

        let recovered = journal.lock().unwrap().replay_from_journal(&agent);
        assert_eq!(recovered.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![1]);
        assert_eq!(buffer.pressure_percent(), 99);
    }

    #[test]
    fn migration_of_an_empty_buffer_reports_no_movable_slot() {
        let (_, _, safeguard) = test_safeguard(1);
        assert_eq!(safeguard.migrate_one_safe_message(None), Err(StrikeOutcome::UnmovableSlot));
    }

    #[test]
    fn failed_journal_insert_keeps_the_live_message_readable() {
        let (buffer, journal, safeguard) = test_safeguard(1);
        let agent = test_agent(0b1);
        journal.lock().unwrap().insert_lease(MigrationLease::new(999, 999, message(2), 0b1)).unwrap();
        buffer.try_write(message(1), agent.agent_bit).unwrap();

        assert_eq!(safeguard.migrate_one_safe_message(None), Err(StrikeOutcome::JournalFull));
        assert_eq!(buffer.try_read(&agent).unwrap().message_id, 1);
    }

    #[test]
    fn full_journal_discards_and_retries_until_ring_pressure_recovers() {
        let (buffer, journal, safeguard) = test_safeguard(1);
        let agent = test_agent(0b1);
        journal.lock().unwrap().insert_lease(MigrationLease::new(999, 999, message(999), agent.agent_bit)).unwrap();
        fill(&buffer, 450, agent.agent_bit);

        assert_eq!(safeguard.write_to_journal(), StrikeOutcome::Recovered);
        assert_eq!(buffer.pressure_percent(), 70);

        let recovered = journal.lock().unwrap().replay_from_journal(&agent);
        assert_eq!(recovered.iter().map(|message| message.message_id).collect::<Vec<_>>(), vec![96]);
        assert_eq!(buffer.try_read(&agent).unwrap().message_id, 97);
    }

    #[test]
    fn full_journal_is_not_discarded_without_live_ring_pressure() {
        let (_, journal, safeguard) = test_safeguard(1);
        let agent = test_agent(0b1);
        journal.lock().unwrap().insert_lease(MigrationLease::new(999, 999, message(999), agent.agent_bit)).unwrap();
        let mut mode = SafeguardMode::FirstStrike;

        safeguard.check_buffer(&mut mode);

        assert!(matches!(mode, SafeguardMode::Normal));
        assert_eq!(journal.lock().unwrap().replay_from_journal(&agent)[0].message_id, 999);
    }

    #[test]
    fn request_check_is_coalesced_until_the_worker_consumes_it() {
        let (_, _, safeguard) = test_safeguard(1);
        assert!(!safeguard.check_requested.load(Ordering::Acquire));

        safeguard.request_check();
        safeguard.request_check();

        assert!(safeguard.check_requested.load(Ordering::Acquire));
        assert!(safeguard.check_requested.swap(false, Ordering::AcqRel));
        assert!(!safeguard.check_requested.load(Ordering::Acquire));
    }
}
