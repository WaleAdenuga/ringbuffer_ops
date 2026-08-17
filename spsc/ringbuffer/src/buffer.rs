use std::{mem::MaybeUninit, sync::atomic::{AtomicU64, Ordering}};

const BUFFER_SIZE: usize = 10;

/// Let RingBuffer support any type
/// Keep head and tail monotonically increasing
#[derive(Debug)]
pub struct RingBuffer<T> {
    buffer: [MaybeUninit<T>; BUFFER_SIZE], // use MaybeUninit --> fixed size of potentially uninitialized T's
    head: AtomicU64, // owned by producer
    tail: AtomicU64, // owned by consumer
}

#[derive(Debug, PartialEq)]
pub enum RingBufferError {
    BufferFull,
    BufferEmpty,
    InvalidSlot,
    InvalidEntry,
}

impl<T> RingBuffer<T> {

    pub fn new() -> Self {
        Self { 
            buffer: [const {MaybeUninit::uninit()}; BUFFER_SIZE], // initialize the entire array as unitialized
            head: AtomicU64::new(0), 
            tail: AtomicU64::new(0) 
        }
    }

    pub fn try_write(&self, value: T) -> Result<(), RingBufferError> {
        // Is buffer full?
        let current_head = self.head.load(Ordering::Relaxed); // producer owns head, 
        let current_tail = self.tail.load(Ordering::Acquire); // synchronization needed --> acquire

        // 10 slots - head ahead of tail by 10 -> buffer full
        if current_head - current_tail == (BUFFER_SIZE as u64) {
            println!("Buffer is full and can't be written");
            return Err(RingBufferError::BufferFull)
        }

        let slot_index = (current_head % (BUFFER_SIZE as u64)) as usize;
        // No unsafe required for writing -- Nope that's wrong --> prevents concurrent access of ring buffer (taking &mut self)
        // If consumer had taken a reference, then producer can't take mutable reference to ring until the last
        // usage of the consumer reference.
        //self.buffer[slot_index].write(value);

        // How can we mutate the buffer through &self --> MaybeUninit::write() and using a raw pointer
        // and casting it to a mutable one
        let slot = self.buffer[slot_index].as_ptr() as *mut MaybeUninit<T>;
        // SAFETY:
        // current_head identifies the next slot owned by the producer.
        // The full-buffer check guarantees that this slot does not contain
        // an unread value. Since this is SPSC, the producer is the only
        // thread that writes to this slot at this point. The consumer can
        // only access the slot after head is updated below.
        unsafe {
            (*slot).write(value);
        }

        // Advance head
        // Safe to do this instead of fetch_add since one producer
        // fetch_add is an atomic read-modify-write operation ==> read head, add 1 and write it back atomically
        // no race so fetch_add is overkill
        let next_head = current_head + 1;
        self.head.store(next_head, Ordering::Release);

        Ok(())
    }

    pub fn try_read(&self) -> Result<T, RingBufferError> {
        let current_head = self.head.load(Ordering::Relaxed);
        let current_tail = self.tail.load(Ordering::Acquire);

        if current_head == current_tail {
            // head equals tail, ring is empty
            return Err(RingBufferError::BufferEmpty);
        }

        // Ring is not empty
        // Locate next slot to read (sequential read)
        let slot_to_read = (current_tail % (BUFFER_SIZE as u64)) as usize;
        // unsafe because it may not be initialized
        let value_in_slot = unsafe {
            self.buffer[slot_to_read].assume_init_read() // init_read moves T out of the slot and leaves it uninitialized
            // You don't want to return a reference here: consumer now owns the value and does something with it
        };

        // Advance tail
        // Safe to do this instead of fetch_add since one consumer
        let next_tail = current_tail + 1;
        self.tail.store(next_tail, Ordering::Release);

        Ok(value_in_slot)
    }
}


// Testing
#[cfg(test)] // only compile when running tests
mod tests {
    use core::panic;
use std::{assert_eq, assert_ne, matches, println, sync::Arc, thread};

use super::*; // parent module

    #[test]
    fn new_buffer_is_empty() {
        let buffer = RingBuffer::<u64>::new();

        assert_eq!(buffer.head.load(Ordering::Relaxed), 0);
        assert_eq!(buffer.tail.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn read_from_empty_buffer() {
        let buffer = RingBuffer::<u64>::new();
        assert!(matches!(buffer.try_read(), Err(RingBufferError::BufferEmpty)));
        //assert_eq!(buffer.try_read().unwrap_err(), RingBufferError::BufferEmpty);
    }

    #[test]
    fn read_and_write_buffer() {
        let buffer = RingBuffer::<u64>::new();

        buffer.try_write(46).unwrap();
        assert_eq!(buffer.try_read().unwrap(), 46);
    }

    #[test]
    fn overload_buffer() {
        let buffer = RingBuffer::<u64>::new();

        for i in 0..10 {
            buffer.try_write(i).unwrap();
        }

        // try an 11th write
        assert!(matches!(buffer.try_write(46), Err(RingBufferError::BufferFull)));
    }

    #[test]
    fn overload_buffer_release_retry() {
        let buffer = RingBuffer::<u64>::new();

        for i in 0..10 {
            assert!(buffer.try_write(i).is_ok());
        }

        // try an 11th write
        assert!(matches!(buffer.try_write(46), Err(RingBufferError::BufferFull)));

        //read first slot, value should be 0
        assert_eq!(buffer.try_read().unwrap(), 0);

        // write should be successful now
        assert_ne!(buffer.try_write(14), Err(RingBufferError::BufferFull));
    }

    #[test]
    fn physical_wrap() {
        let buffer = RingBuffer::<u64>::new();

        for i in 0..10 {
            buffer.try_write(i).unwrap();
        }
        // try an 11th write
        assert!(matches!(buffer.try_write(46), Err(RingBufferError::BufferFull)));

        //read first 5 slots, value should be i, then rewrite them
        for i in 0..5 {
            assert_eq!(buffer.try_read().unwrap(), i);
            assert!(buffer.try_write(i).is_ok());
        }
    }

    #[test]
    fn physical_wrap_extended() {
        let buffer = RingBuffer::<u64>::new();

        for j in 0..1000 {
            println!("Run: {j}");
            for i in 0..10 {
                buffer.try_write(i).unwrap();
            }
            // try an 11th write, should error
            assert!(matches!(buffer.try_write(46), Err(RingBufferError::BufferFull)));

            //read first 5 slots, value should be i, then rewrite them
            for i in 0..5 {
                assert_eq!(buffer.try_read().unwrap(), i);
                assert!(buffer.try_write(i).is_ok());
            }

            // read/rewrite remaining 5 slots
            for i in 5..10 {
                assert_eq!(buffer.try_read().unwrap(), i);
                assert!(buffer.try_write(i).is_ok());
            }

            // read all slots so it's empty at the end of a run (since we rewrite every time we read above)
            for i in 0..10 {
                assert_eq!(buffer.try_read().unwrap(), i);
            }
        }
    }

    #[test]
    fn prod_consumer_thread() {
        // create atomically reference counted object --> essentially a shared pointer?
        let buffer = Arc::new(RingBuffer::<u64>::new());

        // Value belongs to thread that created it, arc solves that ownership problem
        // Multiple threads can own an Arc pointing to the same object
        let prod_buffer = Arc::clone(&buffer);
        let cons_buffer = Arc::clone(&buffer);

        let producer = thread::spawn(move || {
            for i in 0..6_000 {
                loop { // actual retry mechanism
                    match prod_buffer.try_write(i) {
                        Ok(_) => break,
                        Err(RingBufferError::BufferFull) => {
                            println!("Yielding to consumer");
                            std::thread::yield_now(); // willing to let another runnable thread run
                        },
                        _ => panic!("Unexpected ring buffer error"),
                    }   
                }
            }
        });

        let consumer = thread::spawn(move || {
            for i in 0..6_000 {
                loop {
                    match cons_buffer.try_read() {
                        Ok(value) => {
                            assert_eq!(value, i);
                            break;
                        },
                        Err(RingBufferError::BufferEmpty) => {
                            println!("Yielding to producer");
                            std::thread::yield_now();
                        },
                        _ => panic!("Unexpected ring buffer error"),
                    }
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn prod_consumer_stress_test() {
        // create atomically reference counted object --> essentially a shared pointer?
        let buffer = Arc::new(RingBuffer::<u64>::new());

        // Value belongs to thread that created it, arc solves that ownership problem
        // Multiple threads can own an Arc pointing to the same object
        let prod_buffer = Arc::clone(&buffer);
        let cons_buffer = Arc::clone(&buffer);

        let producer = thread::spawn(move || {
            for i in 0..6_000_000 {
                loop { // actual retry mechanism
                    match prod_buffer.try_write(i) {
                        Ok(_) => break,
                        Err(RingBufferError::BufferFull) => {
                            println!("Yielding to consumer");
                            std::thread::yield_now(); // willing to let another runnable thread run
                        },
                        _ => panic!("Unexpected ring buffer error"),
                    }   
                }
            }
        });

        let consumer = thread::spawn(move || {
            for i in 0..6_000_000 {
                loop {
                    match cons_buffer.try_read() {
                        Ok(value) => {
                            assert_eq!(value, i);
                            break;
                        },
                        Err(RingBufferError::BufferEmpty) => {
                            println!("Yielding to producer");
                            std::thread::yield_now();
                        },
                        _ => panic!("Unexpected ring buffer error"),
                    }
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }
}