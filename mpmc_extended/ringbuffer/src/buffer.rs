use std::{cell::UnsafeCell, sync::atomic::{AtomicU64, Ordering}};
use crate::message::{Message};
use crate::registry::AgentDetails;

const DEFAULT_BUFFER_SIZE: usize = 500;
pub type DefaultRingBuffer = RingBuffer<DEFAULT_BUFFER_SIZE>;
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

// Make the buffer take an optional parameter for capacity - easier for testing
#[derive(Debug)]
pub struct RingBuffer<const CAPACITY: usize = DEFAULT_BUFFER_SIZE> {
    buffer: [Slot; CAPACITY], 
    head: AtomicU64,
}

#[derive(Debug, PartialEq)]
pub enum RingBufferError {
    BufferFull,
    BufferEmpty,
    NoRecipient,
    InvalidMetaData,
}

// Producers keep writing sequentially
// But consumers have their own read_cursor, so we need to distinguish between
// when a slot is available to write, and when not. We can still use seqno but this time,
// when there are no more readers, then the last consumer will update the seqno to the right generation
// and the producer can write afterwards
// TODO: How will the safeguard interact with this?
impl<const CAPACITY: usize> RingBuffer<CAPACITY> {

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
            if reader_mask == 0 { // no subscribers
                return Err(RingBufferError::NoRecipient);
            }
            
            let current_head = self.head.load(Ordering::Acquire);
            let slot_index = (current_head % (CAPACITY as u64)) as usize;
            let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);
            let difference = current_seqno.wrapping_sub(current_head) as i64;
            if difference == 0 {
                // I own this slot from the consumer, I can write
                // but first, let me make sure I own this from other producers
                // perform CAS on head (increments global head but we still keep current)
                let result = self.head.compare_exchange(
                    current_head, 
                    current_head.wrapping_add(1), 
                    Ordering::Relaxed, 
                    Ordering::Relaxed);

                match result {
                    Ok(claimed_head) => { // note that claimed_head will always equal current_head based on compare_exchange docs
                        let slot_to_write = (claimed_head % (CAPACITY as u64)) as usize;
                        let slot_value = self.buffer[slot_to_write].value.get() as *mut Message;
                        unsafe {
                            (*slot_value) = value;
                        }
                        // Advance seqno and set the pending readers bitmask
                        self.buffer[slot_to_write].pending_readers.store(reader_mask, Ordering::Relaxed);
                        self.buffer[slot_to_write].seqno.store(current_seqno.wrapping_add(1), Ordering::Release);
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
            // Only one agent bit is allowed to be set
            if agent_details.agent_bit.count_ones() != 1 {
                return Err(RingBufferError::InvalidMetaData);
            }

            let current_tail = agent_details.read_cursor.load(Ordering::Acquire);
            let slot_index = (current_tail % (CAPACITY as u64)) as usize;
            let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);
            
            let difference = current_seqno.wrapping_sub(current_tail.wrapping_add(1)) as i64;
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
                        let message = (*self.buffer[slot_index].value.get()).clone();   
                        let previous = self.buffer[slot_index].pending_readers.fetch_and(!agent_details.agent_bit, Ordering::AcqRel);
                        // If pending_readers is 1, advance seqno, and make slot available to producers
                        // recall that fetch_and returns the previous value
                        // if the previous value is the same as the agent_bit, then that guarantees this agent is the last reader.
                        if previous == agent_details.agent_bit {
                            self.buffer[slot_index].seqno.store(current_tail.wrapping_add(CAPACITY as u64), Ordering::Release);
                        }
                        self.advance_cursor(&agent_details.read_cursor);
                        return Ok(message);
                    };
                }
            } else if difference < 0 { // Value not published ==> empty
                return Err(RingBufferError::BufferEmpty);
            } else { // ==> slot is ahead, fast-forward read_cursor by one generation
                agent_details.read_cursor.fetch_add(CAPACITY as u64, Ordering::AcqRel);
                continue;
            }   
        }
    }

    fn advance_cursor(&self, cursor: &AtomicU64) {
        cursor.fetch_add(1, Ordering::Release);
    }

    pub fn current_write_cursor(&self) -> u64 {
        self.head.load(Ordering::Acquire)
    }
}

// Testing
mod tests {
    use core::panic;
use std::thread::JoinHandle;
use std::{assert_eq, assert_ne, matches, sync::Arc, thread};
use std::sync::atomic::AtomicUsize;

use event_listener::Event;

use crate::message::MessageTopic;

use super::*; // parent module

    fn test_agent(agent_bit: u64) -> AgentDetails {
        AgentDetails::new(
            AtomicU64::new(0), 
            Arc::new(Event::new()), 
            agent_bit
        )
    }
    
    #[test]
    fn new_buffer_is_empty() {
        let buffer = DefaultRingBuffer::new();
        assert_eq!(buffer.head.load(Ordering::Relaxed), 0);

        for i in 0..DEFAULT_BUFFER_SIZE {
            let seqno = buffer.buffer[i].seqno.load(Ordering::Relaxed) as usize;
            assert_eq!(i, seqno);
        }
    }

    #[test]
    fn empty_buffer() {
        let buffer = DefaultRingBuffer::new();
        let agent = test_agent(0b1);
        assert!(matches!(buffer.try_read(&agent), Err(RingBufferError::BufferEmpty)));
        assert_eq!(agent.read_cursor.load(Ordering::Acquire), 0);
    }

    #[test]
    fn zero_recipient_rejection_and_doesnt_advance_head() {
        let buffer = DefaultRingBuffer::new();
        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();

        assert!(matches!(buffer.try_write(message, 0), Err(RingBufferError::NoRecipient)));
        assert_eq!(buffer.current_write_cursor(), 0);
    }

    #[test]
    fn multi_agent_bit_rejection() {
        let buffer = DefaultRingBuffer::new();
        let agent = test_agent(0b011);
        assert!(matches!(buffer.try_read(&agent), Err(RingBufferError::InvalidMetaData)));
        assert_eq!(agent.read_cursor.load(Ordering::Acquire), 0);
    }

    #[test]
    fn single_recipient_round_trip() {
        let buffer = DefaultRingBuffer::new();
        let agent = test_agent(0b100);
        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b111).is_ok());
        let message_read = buffer.try_read(&agent).unwrap();
        assert_eq!(message, message_read);
    }

    #[test]
    fn read_returns_empty_after_message_is_drained() {
        let buffer = DefaultRingBuffer::new();
        let agent = test_agent(0b100);
        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b111).is_ok());
        let message_read = buffer.try_read(&agent).unwrap();
        assert_eq!(message, message_read);

        let try_read_again = buffer.try_read(&agent);
        assert!(matches!(try_read_again, Err(RingBufferError::BufferEmpty)));
    }

    #[test]
    fn read_cursor_advances_after_reading_and_when_skipping_messages() {
        let buffer = DefaultRingBuffer::new();
        let agent = test_agent(0b100);
        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        
        assert!(buffer.try_write(message, 0b111).is_ok());
        let message_read = buffer.try_read(&agent).unwrap();
        assert_eq!(message, message_read);
        assert_eq!(1, agent.read_cursor.load(Ordering::Acquire));

        assert!(buffer.try_write(message, 0b011).is_ok());
        let _ = buffer.try_read(&agent);
        assert_eq!(2, agent.read_cursor.load(Ordering::Acquire));
        
        assert!(buffer.try_write(message, 0b010).is_ok());
        assert!(buffer.try_write(message, 0b100).is_ok());
        // Keep trying to read until you find a message that concerns you
        let message_again = buffer.try_read(&agent).unwrap();
        assert_eq!(message, message_again);
        assert_eq!(4, agent.read_cursor.load(Ordering::Acquire));
    }

    #[test]
    fn two_recipients_receive_one_copy_each() {
        let buffer = RingBuffer::<5>::new();
        let agent_1 = test_agent(0b100);
        let agent_2 = test_agent(0b010);

        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b111).is_ok());

        let a_message = buffer.try_read(&agent_1).unwrap();
        let b_message = buffer.try_read(&agent_2).unwrap();

        assert_eq!(a_message, b_message);
        assert!(matches!(buffer.try_read(&agent_1), Err(RingBufferError::BufferEmpty)));
        assert!(matches!(buffer.try_read(&agent_2), Err(RingBufferError::BufferEmpty)));
    }

    #[test]
    fn first_reader_does_not_free_slot() {
        let buffer = RingBuffer::<5>::new();
        let agent_1 = test_agent(0b100);
        let agent_2 = test_agent(0b010);
        let agent_3 = test_agent(0b001);

        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b111).is_ok());

        let a_message = buffer.try_read(&agent_1).unwrap();
        assert_eq!(message, a_message);

        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 1); // seqno shouldn't have changed since there are pending readers
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b011);

        let b_message = buffer.try_read(&agent_2).unwrap();
        assert_eq!(b_message, a_message);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 1); // seqno shouldn't have changed since there are pending readers
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b001);

        let c_message = buffer.try_read(&agent_3).unwrap();
        assert_eq!(b_message, c_message);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 5); // seqno should've increased by the value of capacity which we set to 5
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b000);
    }

    #[test]
    fn last_reader_frees_slot() {
        let buffer = RingBuffer::<2>::new();
        let agent_1 = test_agent(0b100);
        let agent_2 = test_agent(0b010);
        let agent_3 = test_agent(0b001);

        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b110).is_ok());
        assert!(buffer.try_write(message, 0b001).is_ok()); 
        
        // buffer should be full
        assert!(matches!(buffer.try_write(message, 0b001), Err(RingBufferError::BufferFull)));
        let a_message = buffer.try_read(&agent_1).unwrap();
        assert_eq!(message, a_message);

        // it still shouldn't be possible to write
        assert!(matches!(buffer.try_write(message, 0b001), Err(RingBufferError::BufferFull)));
        let b_message = buffer.try_read(&agent_2).unwrap();
        assert_eq!(b_message, a_message);

        // buffer should be free now
        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 2);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b000);
        assert!(buffer.try_write(message, 0b110).is_ok());
        assert_eq!(slot.seqno.load(Ordering::Acquire), 3);
        // then full again
        assert!(matches!(buffer.try_write(message, 0b001), Err(RingBufferError::BufferFull)));
        

        let c_message = buffer.try_read(&agent_3).unwrap();
        assert_eq!(c_message, b_message);
        assert!(buffer.try_write(message, 0b110).is_ok());

    }
}