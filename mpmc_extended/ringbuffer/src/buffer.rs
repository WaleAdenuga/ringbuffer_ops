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

enum ReadMode {
    Deliver,
    Drain,
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
    /// On failure - return owned message as well - large payloads won't implement Copy for Message
    pub fn try_write(&self, value: Message, reader_mask: u64) -> Result<(), (RingBufferError, Message)> {
        loop {
            if reader_mask == 0 { // no subscribers
                return Err((RingBufferError::NoRecipient, value));
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
                return Err((RingBufferError::BufferFull, value));
            } else {
                // some other producer moved this slot forward 
                // don't return an error (like in spmc since there was only one producer), try again
                continue;
            }
        }
    }

    pub fn try_read(&self, agent_details: &AgentDetails) -> Result<Message, RingBufferError> {
        loop {
            match self.read_next(agent_details, ReadMode::Deliver) {
                Ok(value) => {
                    if value.is_none() {
                        continue;
                    } else {
                        return Ok(value.unwrap());
                    }
                },
                Err(value) => {
                    return Err(value); // error is either invalid metadata or empty buffer - propagate back to broker
                }
            }
        }
    }

    fn read_next(&self, agent_details: &AgentDetails, mode: ReadMode) -> Result<Option<Message>, RingBufferError> {
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
                return Ok(None);
            } else {
                let message = match mode {
                    ReadMode::Deliver => {
                        Some( unsafe { (*self.buffer[slot_index].value.get()).clone() })
                    }
                    ReadMode::Drain => None
                };
                let previous = self.buffer[slot_index].pending_readers.fetch_and(!agent_details.agent_bit, Ordering::AcqRel);
                // If pending_readers is 1, advance seqno, and make slot available to producers
                // recall that fetch_and returns the previous value
                // if the previous value is the same as the agent_bit, then that guarantees this agent is the last reader.
                if previous == agent_details.agent_bit {
                    self.buffer[slot_index].seqno.store(current_tail.wrapping_add(CAPACITY as u64), Ordering::Release);
                }
                self.advance_cursor(&agent_details.read_cursor);
                return Ok(message);
            }
        } else if difference < 0 { // Value not published ==> empty
            return Err(RingBufferError::BufferEmpty);
        } else { // ==> slot is ahead, advance cursor (not by one generation as it may miss message in other slot)
            // just skip one slot
            self.advance_cursor(&agent_details.read_cursor);
            return Ok(None);
        }   
    }

    fn advance_cursor(&self, cursor: &AtomicU64) {
        cursor.fetch_add(1, Ordering::Release);
    }

    pub fn current_write_cursor(&self) -> u64 {
        self.head.load(Ordering::Acquire)
    }

    /// Walks from the agent's read cursor to cutoff, clearing messages
    pub fn drain_to(&self, agent_details: &AgentDetails, cutoff: u64) -> Result<(), RingBufferError> {
        while agent_details.read_cursor.load(Ordering::Acquire) < cutoff {
            // update slot's pending readers - ignore message in slot
            self.read_next(agent_details, ReadMode::Drain)?;
        }
        Ok(())
    }

    /// Checks if slot still has messages for this agent
    pub fn has_pending_reader(&self, agent_bit: u64) -> bool {
        // returns true if the closure in any returns true
        self.buffer.iter().any(|slot| slot.pending_readers.load(Ordering::Acquire) & agent_bit != 0)
    }
}

// Sync means multiple threads can safely use references to this value at the same time
unsafe impl<const CAPACITY: usize> Sync for RingBuffer<CAPACITY> {}

// Testing
mod tests {
    use core::panic;
use std::assert_eq;
use std::thread::JoinHandle;
use std::{sync::Arc, thread};
use std::sync::atomic::AtomicUsize;
use crate::message::MessageTopic;

use event_listener::Event;
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

        assert!(matches!(buffer.try_write(message, 0), Err((RingBufferError::NoRecipient, _message)) ));
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
    fn last_reader_frees_slot_and_writer_can_only_use_after_that() {
        let buffer = RingBuffer::<2>::new();
        let agent_1 = test_agent(0b100);
        let agent_2 = test_agent(0b010);

        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b110).is_ok());
        assert!(buffer.try_write(message, 0b001).is_ok()); 
        
        // buffer should be full
        assert!(matches!(buffer.try_write(message, 0b001), Err((RingBufferError::BufferFull, _message))));
        let a_message = buffer.try_read(&agent_1).unwrap();
        assert_eq!(message, a_message);

        // it still shouldn't be possible to write
        assert!(matches!(buffer.try_write(message, 0b001), Err((RingBufferError::BufferFull, _message))));
        let b_message = buffer.try_read(&agent_2).unwrap();
        assert_eq!(b_message, a_message);

        // buffer should be free now
        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 2);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b000);
        assert!(buffer.try_write(message, 0b110).is_ok());
        assert_eq!(slot.seqno.load(Ordering::Acquire), 3);
        // then full again
        assert!(matches!(buffer.try_write(message, 0b001), Err((RingBufferError::BufferFull, _message))));
    }

    #[test]
    fn reclaimed_unowned_slot_advances_one_cursor_position() {
        let buffer = RingBuffer::<2>::new();
        let agent_1 = test_agent(0b100);
        let agent_2 = test_agent(0b010);
        let agent_3 = test_agent(0b001);

        let message = Message::new(MessageTopic::Pictures, 1, 1).unwrap();
        assert!(buffer.try_write(message, 0b110).is_ok()); // slot 0
        assert!(buffer.try_write(message, 0b001).is_ok()); // slot 1
        
        // buffer should be full
        assert!(matches!(buffer.try_write(message, 0b001), Err((RingBufferError::BufferFull, _message))));
        
        // drain slot 0 - advancing its seqno by a generation (CAPACITY)
        let a_message = buffer.try_read(&agent_1).unwrap();
        assert_eq!(message, a_message);
        let b_message = buffer.try_read(&agent_2).unwrap();
        assert_eq!(b_message, a_message);

        // slot 0 should be free now
        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 2);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b000);
        assert!(buffer.try_write(message, 0b110).is_ok());
        assert_eq!(slot.seqno.load(Ordering::Acquire), 3);
        
        // it should still be possible to read message in slot 1
        assert_eq!(0, agent_3.read_cursor.load(Ordering::Acquire));
        let c_message = buffer.try_read(&agent_3).unwrap();
        assert_eq!(c_message, b_message);
        // read cursor should've advanced by 2
        assert_eq!(2, agent_3.read_cursor.load(Ordering::Acquire));
        assert!(buffer.try_write(message, 0b110).is_ok());

    }

    #[test]
    fn reader_cannot_receive_same_message_twice() {
        let buffer = RingBuffer::<2>::new();
        let agent_1 = test_agent(0b100);

        let message = Message::new(MessageTopic::Shopping, 64, 1).unwrap();
        assert!(buffer.try_write(message, 0b100).is_ok()); // slot 0 - only for agent 1
        assert!(buffer.try_write(message, 0b001).is_ok()); // slot 1
        
        // drain slot 0 - advancing its seqno by a generation (CAPACITY)
        let a_message = buffer.try_read(&agent_1).unwrap();
        assert_eq!(message, a_message);
        assert_eq!(agent_1.read_cursor.load(Ordering::Acquire), 1);

        // agent should not be able to read again - even if that message hasn't been removed by the writer
        // since its read_cursor has been advanced and can't go back
        assert!(matches!(buffer.try_read(&agent_1), Err(RingBufferError::BufferEmpty)));

        // slot 0 should be free now
        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 2);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b000);
        assert!(buffer.try_write(message, 0b110).is_ok());
        assert_eq!(slot.seqno.load(Ordering::Acquire), 3);
    }

    // zero recipient write must fail before a head is claimed or slot modified
    #[test]
    fn zero_recipient_rejection_does_not_consume_capacity() {
        let buffer = RingBuffer::<2>::new();
        let message = Message::new(MessageTopic::Shopping, 64, 1).unwrap();
        assert!(matches!(buffer.try_write(message, 0b000), Err((RingBufferError::NoRecipient, _message))));
        // slot 0 should be free now
        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 0);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b000);

        assert!(buffer.try_write(message, 0b100).is_ok()); // should write to slot 0 now
        assert_eq!(slot.seqno.load(Ordering::Acquire), 1);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b100);
    }

    #[test]
    fn unowned_reader_does_not_clear_pending_bit() {
        let buffer = RingBuffer::<2>::new();
        let agent_1 = test_agent(0b100);

        let message = Message::new(MessageTopic::Shopping, 64, 1).unwrap();
        assert!(buffer.try_write(message, 0b011).is_ok()); // message not for agent 1

        let slot = &(buffer.buffer[0]);
        assert_eq!(slot.seqno.load(Ordering::Acquire), 1);
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b011);
        assert!(matches!(buffer.try_read(&agent_1), Err(RingBufferError::BufferEmpty)));
        assert_eq!(slot.pending_readers.load(Ordering::Acquire), 0b011);
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct ReaderResult {
        agent_bit: u64,
        received_messages: Vec<u64>,
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    struct ReaderExpectedMessage {
        agent_bit: u64,
        sent_messages: Vec<u64>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PlannedMessage {
        message_id: u64,
        reader_mask: u64,
    }

    // Helper function to spawn multiple consumer threads (saves code duplication)
    fn spawn_consumers<const CAPACITY: usize>(buffer: Arc<RingBuffer<CAPACITY>>, agents: Vec<ReaderExpectedMessage>) -> Vec<JoinHandle<ReaderResult>> {
        let mut handles = Vec::new();
        
        for agent in agents {
            let cons_buffer = Arc::clone(&buffer);
            let consumer = thread::spawn(move || {
                let mut read_messages: Vec<u64> = Vec::new();
                let dummy = test_agent(agent.agent_bit);
                loop {
                    // recreate agent details
                    match cons_buffer.try_read(&dummy) {
                        Ok(value) => {
                            read_messages.push(value.message_id);
                        },
                        Err(RingBufferError::BufferEmpty) => {
                            std::thread::yield_now();
                        },
                        _ => panic!("Unexpected ring buffer error"),
                    }
                    if read_messages.len() >= agent.sent_messages.len() {
                        break;
                    }
                }
                ReaderResult { agent_bit: agent.agent_bit, received_messages: read_messages }
            });
            handles.push(consumer);
        }
        handles
    }

    fn spawn_producers<const CAPACITY: usize>(buffer: Arc<RingBuffer<CAPACITY>>, messages: Vec<PlannedMessage>, no_writers: usize) -> Vec<JoinHandle<()>> {
        
        let mut handles = Vec::new();
        let next_value = Arc::new(AtomicUsize::new(0));
        let messages_to_write = Arc::new(messages);

        for _ in 0..no_writers {
            let prod_buffer = Arc::clone(&buffer);
            let prod_next_value = Arc::clone(&next_value);
            let prod_messages = Arc::clone(&messages_to_write);
            
            let producer = thread::spawn(move || {
                loop {
                    // prevents producers writing the same value --> like in a standard for loop
                    let i = prod_next_value.fetch_add(1, Ordering::Relaxed);
                    
                    if i >= prod_messages.len() {
                        break;
                    }
                    let planned = prod_messages.get(i).unwrap();
                    let message = Message::new(MessageTopic::Sports, planned.message_id, 1).unwrap();
                    loop { // actual retry mechanism
                        match prod_buffer.try_write(message, planned.reader_mask) {
                            Ok(_) => {
                                break;
                            },
                            Err((RingBufferError::BufferFull, _message)) => {
                                //println!("BufferFull");
                                std::thread::yield_now(); // willing to let another runnable thread run
                            },
                            Err((RingBufferError::NoRecipient, _message)) => {
                                //println!("No recipient for message");
                                std::thread::yield_now();
                            }
                            _ => panic!("Unexpected ring buffer error"),
                        } 
                    }

                }
            });
            handles.push(producer);
        }
        
        handles
    }

    fn recipient_mask(message_id: u64, consumer_count: u64) -> u64 {
        assert!(consumer_count <= 64);
        let all_readers = if consumer_count == 64 {
            u64::MAX
        } else {
            (1_u64 << consumer_count) - 1
        };

        (message_id % all_readers) + 1
    }
    
    // Prevent code duplication
    fn producers_consumers_test(no_producers: usize, no_consumers: usize, no_entries: usize) -> () {
        // create atomically reference counted object --> essentially a shared pointer?
        let buffer = Arc::new(RingBuffer::<700>::new());

        // Value belongs to thread that created it, arc solves that ownership problem
        // Multiple threads can own an Arc pointing to the same object
        let prod_buffer = Arc::clone(&buffer);
        let cons_buffer = Arc::clone(&buffer);

        // First build planned message for the writers
        let messages : Vec<PlannedMessage> = (1..no_entries+1).map(|index| PlannedMessage { message_id: index as u64, reader_mask: recipient_mask(index as u64, no_consumers as u64)}).collect();
        // N producers writing all messages in vector - we don't care who writes what, at the end it should equal the no_entries
        let producers = spawn_producers(prod_buffer, messages.clone(), no_producers);

        // Build reader masks for all expected consumers
        let agents : Vec<AgentDetails> = (0..no_consumers).map(|index| test_agent(1_u64 << index)).collect();
        let mut expected: Vec<ReaderExpectedMessage> = Vec::new();

        for agent in agents {
            let mut agent_messages = Vec::new();
            for message in messages.clone() {
                if message.reader_mask & agent.agent_bit != 0 {
                    agent_messages.push(message.message_id);
                }
            }
            expected.push(ReaderExpectedMessage { agent_bit: agent.agent_bit, sent_messages: agent_messages });
        }
        let consumers = spawn_consumers(cons_buffer, expected.clone());

        for producer in producers {
            producer.join().unwrap();
        }

        let mut results = Vec::new();
        for consumer in consumers {
            results.push(consumer.join().unwrap());
        }

        results.sort_by_key(|result| result.agent_bit);
        expected.sort_by_key(|result| result.agent_bit);

        assert_eq!(expected.len(), results.len());
        
        for (expected_item, actual_item) in expected.iter().zip(results.iter()) {
            assert_eq!(expected_item.agent_bit, actual_item.agent_bit);

            let mut expected_ids = expected_item.sent_messages.clone();
            let mut actual_ids = actual_item.received_messages.clone();

            expected_ids.sort_unstable();
            actual_ids.sort_unstable();

            assert_eq!(expected_ids, actual_ids);
        }
    }
    

    #[test]
    fn two_producers_one_consumer() {
        let no_of_writes = 4_000;
        producers_consumers_test(2, 1, no_of_writes);
    }

    #[test]
    fn three_producers_two_consumers() {
        let no_of_writes = 40_000;
        producers_consumers_test(3, 2, no_of_writes);
    }

    #[test]
    fn eight_producers_five_consumers() {
        let no_of_writes = 400_000;
        producers_consumers_test(8, 5, no_of_writes);
    }

    #[test]
    fn three_producers_eight_consumers() {
        let no_of_writes = 40_000;
        producers_consumers_test(3, 8, no_of_writes);
    }

    #[test]
    fn eight_producers_one_consumers() {
        let no_of_writes = 4_000_000;
        producers_consumers_test(8, 1, no_of_writes);
    }

    #[test]
    fn one_producers_ten_consumers() {
        let no_of_writes = 400_000;
        producers_consumers_test(1, 10, no_of_writes);
    }
}