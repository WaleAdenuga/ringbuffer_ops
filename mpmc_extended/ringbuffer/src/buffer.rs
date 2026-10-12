use std::{cell::UnsafeCell, sync::atomic::{AtomicU64, Ordering}};
use crate::message::{Message};
use crate::registry::AgentDetails;

const BUFFER_SIZE: usize = 500;
pub type ReaderMask = AtomicU64; // 64bit - 64 possible readers

#[derive(Debug)] // AtomicU64 doesn't implement Copy
pub struct Slot {
    seqno: AtomicU64, // atomic for multiple consumers
    // use fixed Message type, since final plan is to have in SharedMemory
    // unsafecell - gives us control over mutability but atomic semantics still up to us
    // unsafecell - rust primitive for interior mutability
    value: UnsafeCell<Message>, 
    pending_readers: ReaderMask, // To know how many readers are left before slot is available (treated as bitmask)
}

#[derive(Debug)]
pub struct RingBuffer {
    buffer: [Slot; BUFFER_SIZE], 
    head: AtomicU64, // owned by producer
}

#[derive(Debug, PartialEq)]
pub enum RingBufferError {
    BufferFull,
    BufferEmpty,
    SlotAccessDenied,
}

// Producers keep writing sequentially
// But consumers have their own read_cursor, so we need to distinguish between
// when a slot is available to write, and when not. We can still use seqno but this time,
// when there are no more readers, then the last consumer will update the seqno to the right generation
// and the producer can write afterwards
// TODO: How will the safeguard interact with this?
impl RingBuffer {

    pub fn new() -> Self {
        Self { 
            buffer: std::array::from_fn(|i| Slot {
                seqno: AtomicU64::new(i as u64),
                value: UnsafeCell::new(Message::empty().unwrap()),
                pending_readers: AtomicU64::new(0),
            }), // each slot's initial seqno depends on its index
            head: AtomicU64::new(0),
        }
    }

    /// Writing requires a message and a reader mask signifying the pending readers for this slot
    /// Saved in the format of a bitmask
    /// i.e. 4 agents, message about sports, first and fourth agents are interested 
    /// This slot will get mask 1001 (or value 9) - When those agents read, we switch the bit to 0
    /// Message is passed in by value as slot should take ownership of it
    pub fn try_write(&self, value: Message, reader_mask: u64) -> Result<(), RingBufferError> {
        loop {
            let current_head = self.head.load(Ordering::Acquire); // producer owns head, 
            let slot_index = (current_head as usize) % BUFFER_SIZE;
            let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);

            // Use difference as splitting point rather than head/tail semantics
            let difference : i64 = current_seqno as i64 - current_head as i64;
            if difference == 0 {
                // I own this slot from the consumer, I can write
                // but first, let me make sure I own this from other producers
                // perform CAS on head (increments global head but we still keep current)
                let result = self.head.compare_exchange(
                    current_head, 
                    current_head + 1, 
                    Ordering::Relaxed, 
                    Ordering::Relaxed);

                match result {
                    Ok(claimed_head) => { // note that claimed_head will always equal current_head based on compare_exchange docs
                        let slot_to_write = (claimed_head as usize) % BUFFER_SIZE;
                        let slot_value = self.buffer[slot_to_write].value.get() as *mut Message;
                        unsafe {
                            (*slot_value) = value;
                        }

                        // Advance seqno and set the pending readers bitmask
                        self.buffer[slot_to_write].pending_readers.store(reader_mask, Ordering::Relaxed);
                        self.buffer[slot_to_write].seqno.store(current_seqno + 1, Ordering::Release);
                        return Ok(());
                    },
                    Err(_) => {
                        // retry to claim another position
                        continue;
                    }
                }
            } else if difference < 0 {
                // slot hasn't been released for this generation (buffer must be full)
                return Err(RingBufferError::BufferFull);
            } else {
                // some other producer moved this slot forward 
                // don't return an error (like in spmc since there was only one producer), try again
                continue;
            }
        }
    }

    pub fn try_read(&self, agent_details: &AgentDetails) -> Result<Message, RingBufferError> {
        loop {
            let current_tail = agent_details.read_cursor.load(Ordering::Acquire);

            let slot_index = (current_tail % (BUFFER_SIZE as u64)) as usize;
            let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);
            
            let difference = current_seqno as i64 - (current_tail + 1) as i64;
            if difference == 0 { // ready for consumers
                // Check if this is an eligible consumer for this slot
                // Don't send unnecessary payloads and let the broker handle it
                // pending readers is 110, bit is index 2, ==> 110 & 100 -> 100 -> message concerns you -> get copied messg
                // pending readers is 101, bit is index 1, ==> 101 & 010 -> 000 -> message not for you -> continue scanning
                let op = self.buffer[slot_index].pending_readers.load(Ordering::Acquire) & agent_details.agent_bit;
                if op == 0 {
                    self.advance_cursor(&agent_details.read_cursor);
                    continue;
                } else {
                    unsafe {
                        let message = *self.buffer[slot_index].value.get().clone();
                        self.advance_cursor(&agent_details.read_cursor);
                        
                        let pending_readers = self.buffer[slot_index].pending_readers.fetch_and(!agent_details.agent_bit, Ordering::AcqRel);
                        // If pending_readers is 1, advance seqno, and make slot available to producers
                        // recall that fetch_and returns the previous value
                        // if the previous value is the same as the agent_bit, then that guarantees this agent is the last reader.
                        if pending_readers == agent_details.agent_bit {
                            self.buffer[slot_index].seqno.store(agent_details.read_cursor.load(Ordering::Acquire) + (BUFFER_SIZE as u64), Ordering::Release);
                        }
                        return Ok(message);
                    };
                }
            } else if difference < 0 { // Value not published ==> empty
                return Err(RingBufferError::BufferEmpty);
            } else { // ==> slot is ahead, fast-forward read_cursor by one generation
                agent_details.read_cursor.fetch_add(BUFFER_SIZE as u64, Ordering::AcqRel);
                continue;
            }   
        }
    }

    pub fn advance_cursor(&self, cursor: &AtomicU64) {
        cursor.fetch_add(1, Ordering::Release);
    }

    pub fn current_write_cursor(&self) -> Result<u64, RingBufferError> {
        Ok(self.head.load(Ordering::Acquire))
    }
}