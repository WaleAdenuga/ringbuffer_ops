use std::{sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, thread::{self, JoinHandle}};

use event_listener::{Event, Listener};
use crate::{buffer::RingBuffer, journal::{Journal, JournalError}, registry::Registry};

pub enum SafeguardError {
    No
}

#[derive(Debug, PartialEq, Eq)]
enum StrikeOutcome { // for monitoring journal movement 
    UnmovableMessage,
    JournalFull,
    Recovered,
    SafeguardStopped,
}

#[derive(Debug)]
enum SafeguardMode {
    Normal, // sleep
    Warning, // notification to all pending readers
    FirstStrike, // move messages to journal
    //Nuclear // drain the journal, drain the ring
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
                    self.warn_pending_agents();
                    self.strike_until_recovered(mode);
                } else if buffer_pressure >= self.config.warning_threshold {
                    // warn agents that there's a message
                    *mode = SafeguardMode::Warning;
                    self.warn_pending_agents();
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

            // SafeguardMode::Nuclear => {
            //     // clear journal and ring
            // }
        }
    }

    // TODO - need a way for the agent to know to go to the journal and not the ring itself
    pub fn write_to_buffer(&self) -> bool {
        false
    }

    fn warn_pending_agents(&self) {
        // Get all agents with pending messages on the ring
        let pending_agents = self.buffer.all_pending_agents();
        let registry = self.registry.lock().unwrap(); // same registry lock used by the broker
        let listeners = registry.notify_from_agent_bits(pending_agents);
        drop(registry); // drop asap

        listeners.iter().for_each(|listener| {
            listener.notify(1);
        });
    }

    fn strike_until_recovered(&self, mode: &mut SafeguardMode) {
        *mode = SafeguardMode::FirstStrike;

        if self.write_to_journal() == StrikeOutcome::Recovered {
            *mode = SafeguardMode::Normal;
        }
    }

    fn write_to_journal(&self) -> StrikeOutcome {
        while self.buffer.pressure_percent() > self.config.recover_threshold {
            if self.stop_requested.load(Ordering::Acquire) {
                return StrikeOutcome::SafeguardStopped;
            }

            match self.migrate_one_safe_message() {
                Ok(_) => {
                    continue;
                }
                Err(error) => {
                    return self.map_journal_error(error);
                }
            }
        }

        StrikeOutcome::Recovered
    }

    fn migrate_one_safe_message(&self) -> Result<(), JournalError> {
        Ok(())
    }

    fn map_journal_error(&self, error: JournalError) -> StrikeOutcome {
        match error {
            JournalError::JournalFull => StrikeOutcome::JournalFull,
            JournalError::InvalidMetaData | JournalError::NoRecipientForMessage => StrikeOutcome::UnmovableMessage,
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