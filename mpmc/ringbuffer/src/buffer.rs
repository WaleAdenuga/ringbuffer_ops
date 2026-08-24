use std::{mem::MaybeUninit, println, sync::atomic::{AtomicU64, Ordering}};

const BUFFER_SIZE: usize = 5;


// MPMC - Multiple Producer, Multiple Consumer
// see notes.txt for info on this
// a single tail is no longer sufficient
// If 2 consumers try to read at the same tail, 
// one claims slot 5, other one has to take slot 6 (only one can claim --> CAS and retry)
// with single tail, tail becomes 7.
// Producer with head 4 sees 5 and 6 as free and tries to write
// but what if one of the consumers got descheduled and isn't done reading
// We need one more check before just relinquishing control to producer
// Create a new Slot struct with a sequence number and value. The sequence number is also monotically increasing
// and is used to signify generations -> If seq. no has changed by the time consumer reads, you know it has been overwritten
// seqno is used as state ownership or ownership protocol through the whole lifecycle

#[derive(Debug)] // AtomicU64 and MaybeUnit don't implement Copy so a new doesn't track
pub struct Slot<T> {
    seqno: AtomicU64, // make atomic in case multiple consumers?
    value: MaybeUninit<T>, // use MaybeUninit --> fixed size of potentially uninitialized T's
}

/// Let RingBuffer support any type
/// Keep head and tail monotonically increasing
#[derive(Debug)]
pub struct RingBuffer<T> {
    buffer: [Slot<T>; BUFFER_SIZE], 
    head: AtomicU64, // owned by producer
    tail: AtomicU64, // consumer claim position
}

#[derive(Debug, PartialEq)]
pub enum RingBufferError {
    BufferFull,
    BufferEmpty,
    SlotAccessDenied,
}

impl<T> RingBuffer<T> {

    pub fn new() -> Self {
        Self { 
            buffer: std::array::from_fn(|i| Slot {
                seqno: AtomicU64::new(i as u64),
                value: MaybeUninit::uninit(),
            }), // each slot's initial seqno depends on its index
            head: AtomicU64::new(0), 
            tail: AtomicU64::new(0) 
        }
    }

    // Producer algorithm
    // 1. Load head
    // 2. Find slot
    // 3. Check slot.seqno == head
    // 4. CAS head → head + 1
    // 5. If CAS succeeds → we own the slot, and head is advanced
    // 6. Write value
    // 5. Change seqno to head + 1
    pub fn try_write(&self, value: T) -> Result<(), RingBufferError> {
        loop {
            // Is buffer full?
            let current_head = self.head.load(Ordering::Acquire); // producer owns head, 

            let slot_index = (current_head as usize) % BUFFER_SIZE;
            let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);

            // Use difference as splitting point rather than head/tail semantics

            let difference : i64 = current_seqno as i64 - current_head as i64;
            if difference == 0 {
                // I own this slot from the consumer, I can write
                // but first, let me make sure I own this from other producers
                // perform CAS on head
                let result = self.head.compare_exchange(
                    current_head, 
                    current_head + 1, 
                    Ordering::Acquire, 
                    Ordering::Relaxed);

                match result {
                    Ok(claimed_head) => { // note that claimed_head will always equal current_head based on compare_exchange docs
                        let slot_to_write = (claimed_head as usize) % BUFFER_SIZE;
                        let slot_value = self.buffer[slot_to_write].value.as_ptr() as *mut MaybeUninit<T>;
                        unsafe {
                            (*slot_value).write(value);
                        }

                        // Advance seqno
                        self.buffer[slot_index].seqno.store(current_seqno + 1, Ordering::Release);
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


    // Consumer Algorithm
    // 1. Load tail
    // 2. Find slot
    // 3. Check slot.seqno == tail + 1
    // 4. CAS tail → tail + 1
    // 5. If CAS succeeds → you own the slot
    // 6. Read value
    // 7. Change seqno to tail + BUFFER_SIZE
    pub fn try_read(&self) -> Result<T, RingBufferError> {
        loop {
            let current_tail = self.tail.load(Ordering::Acquire);

            let slot_index = (current_tail % (BUFFER_SIZE as u64)) as usize;
            let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);
            // Use difference as splitting point rather than head/tail semantics
            let difference = current_seqno as i64 - (current_tail + 1) as i64;

            if difference == 0 { // ready for consumers
                // if tail == current_tail, change to current_tail +1 and tell me I succeeded
                // on success, the return is guaranteed to be current (i.e. value we had when we entered the function)
                // on failure, return the value there now (that someone else had changed)
                let result = self.tail.compare_exchange(
                        current_tail, 
                        current_tail + 1, 
                        Ordering::Acquire, // makes store part of operation Relaxed
                        Ordering::Relaxed);

                match result {
                    Ok(claimed_tail) => {
                        // CAS succeeded ==> We own the current slot from other consumers
                        // Locate next slot to read for the position we've just claimed
                        let slot_to_read = (claimed_tail % (BUFFER_SIZE as u64)) as usize;
                        
                        let value_in_slot = unsafe {
                            self.buffer[slot_to_read].value.assume_init_read()// init_read moves T out of the slot and leaves it uninitialized
                            // You don't want to return a reference here: consumer now owns the value and does something with it
                        };

                        // Advance seqno and head
                        self.buffer[slot_to_read].seqno.store(current_tail + (BUFFER_SIZE as u64), Ordering::Release);
                        return Ok(value_in_slot);
                    
                    },
                    Err(actual) => {
                        println!("Value actually in tail {actual}, Retrying for next slot");
                        // Rather than recursively trying to retry, use a loop and continue
                        // until you get an actual value in the read
                        continue;
                    }
                }
            } else if difference < 0 { // Value not published ==> empty
                return Err(RingBufferError::BufferEmpty);
            } else { // ==> slot is ahead, retry
                continue;
            }
        }
        
    }
}


// Testing
#[cfg(test)] // only compile when running tests
mod tests {
    use core::panic;
use std::thread::JoinHandle;
use std::{assert_eq, assert_ne, matches, sync::Arc, thread};
use std::sync::atomic::AtomicUsize;

use super::*; // parent module

    #[test]
    fn new_buffer_is_empty() {
        let buffer = RingBuffer::<u64>::new();
        assert_eq!(buffer.head.load(Ordering::Relaxed), 0);
        assert_eq!(buffer.tail.load(Ordering::Relaxed), 0);

        for i in 0..BUFFER_SIZE {
            let seqno = buffer.buffer[i].seqno.load(Ordering::Relaxed) as usize;
            assert_eq!(i, seqno);
        }
    }

    #[test]
    fn read_from_empty_buffer() {
        let buffer = RingBuffer::<u64>::new();
        assert!(matches!(buffer.try_read(), Err(RingBufferError::BufferEmpty)));
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

        for i in 0..(BUFFER_SIZE as u64) {
            buffer.try_write(i).unwrap();
        }

        assert!(matches!(buffer.try_write(46), Err(RingBufferError::BufferFull)));
    }

    #[test]
    fn overload_buffer_release_retry() {
        let buffer = RingBuffer::<u64>::new();

        for i in 0..(BUFFER_SIZE as u64) {
            assert!(buffer.try_write(i).is_ok());
        }

        assert!(matches!(buffer.try_write(46), Err(RingBufferError::BufferFull)));
        //read first slot, value should be 0
        assert_eq!(buffer.try_read().unwrap(), 0);
        assert_ne!(buffer.try_write(14), Err(RingBufferError::BufferFull));
    }

    #[test]
    fn single_consumer_lifecycle() { // test that seqno updates properly as well
        let buffer = RingBuffer::<u64>::new();

        buffer.try_write(46).unwrap();
        assert_eq!(buffer.try_read().unwrap(), 46);

        buffer.try_write(46).unwrap();
        assert_eq!(buffer.try_read().unwrap(), 46);

        for i in 0..(BUFFER_SIZE as u64) {
            assert!(buffer.try_write(i).is_ok());
            assert_eq!(buffer.try_read().unwrap(), i);

            let seqno = buffer.buffer[i as usize].seqno.load(Ordering::Relaxed) as usize;
            assert_eq!(i + (BUFFER_SIZE as u64), (seqno as u64));
        }

        for i in (BUFFER_SIZE as u64)..(BUFFER_SIZE as u64) *2 {
            assert!(buffer.try_write(i).is_ok());
            assert_eq!(buffer.try_read().unwrap(), i);

            let seqno = buffer.buffer[(i as usize) - (BUFFER_SIZE)].seqno.load(Ordering::Relaxed) as usize;
            assert_eq!(i + (BUFFER_SIZE as u64), (seqno as u64));
        }

        for i in (BUFFER_SIZE as u64) *2..(BUFFER_SIZE as u64) *3 {
            assert!(buffer.try_write(i).is_ok());
            assert_eq!(buffer.try_read().unwrap(), i);

            let seqno = buffer.buffer[(i as usize) - (BUFFER_SIZE * 2)].seqno.load(Ordering::Relaxed) as usize;
            assert_eq!(i + (BUFFER_SIZE as u64), (seqno as u64));
        }
    }

    // Helper function to spawn multiple consumer threads (saves code duplication)
    fn spawn_consumers(buffer: Arc<RingBuffer<u64>>, consumed: Arc<AtomicUsize>, count: usize, limit: usize) -> Vec<JoinHandle<Vec<u64>>> {
        
        let mut handles = Vec::new();
        
        for _ in 0..count {
            let cons_buffer = Arc::clone(&buffer);
            let cons_consumed = Arc::clone(&consumed);
            let consumer = thread::spawn(move || {
                let mut read_values : Vec<u64> = Vec::new();
                loop {
                    match cons_buffer.try_read() {
                        Ok(value) => {
                            read_values.push(value);
                            cons_consumed.fetch_add(1, Ordering::Relaxed);
                        },
                        Err(RingBufferError::BufferEmpty) => {
                            std::thread::yield_now();
                        },
                        _ => panic!("Unexpected ring buffer error"),
                    }
                    if cons_consumed.load(Ordering::Relaxed) >= limit {
                        break;
                    }
                }
                read_values
            });
            handles.push(consumer);
        }
        handles
    }

    fn spawn_producers(buffer: Arc<RingBuffer<u64>>, written: Arc<AtomicUsize>, count: usize, limit: usize,) -> Vec<JoinHandle<()>> {
        
        let mut handles = Vec::new();
        let next_value = Arc::new(AtomicUsize::new(0));

        for _ in 0..count {
            let prod_buffer = Arc::clone(&buffer);
            let prods_written = Arc::clone(&written);
            let prod_next_value = Arc::clone(&next_value);
            
            let producer = thread::spawn(move || {
                
                loop {
                    // prevents producers writing the same value --> like in a standard for loop
                    let i = prod_next_value.fetch_add(1, Ordering::Relaxed);

                    if i >= limit {
                        break;
                    }

                    loop { // actual retry mechanism
                        match prod_buffer.try_write(i as u64) {
                            Ok(_) => {
                                prods_written.fetch_add(1, Ordering::Relaxed);
                                break;
                            },
                            Err(RingBufferError::BufferFull) => {
                                println!("BufferFull");
                                std::thread::yield_now(); // willing to let another runnable thread run
                            },
                            Err(RingBufferError::SlotAccessDenied) => {
                                println!("Slot Access Denied");
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

    // Prevent code duplication
    fn producers_consumers_test(no_producers: usize, no_consumers: usize, no_entries: usize) -> Vec<u64> {
        // create atomically reference counted object --> essentially a shared pointer?
        let buffer = Arc::new(RingBuffer::<u64>::new());

        // Value belongs to thread that created it, arc solves that ownership problem
        // Multiple threads can own an Arc pointing to the same object
        let prod_buffer = Arc::clone(&buffer);
        let cons_buffer = Arc::clone(&buffer);

        // 2 producers writing from 0 to 6000 - written hard stops the writing
        let written = Arc::new(AtomicUsize::new(0));
        let producers = spawn_producers(prod_buffer, written, no_producers, no_entries);

        // So that we know when to stop the consumer thread,
        // was running into a hang because I was breaking after the first run
        // Multiple threads can own an Arc pointing to the same object, updating either updates original
        let consumed = Arc::new(AtomicUsize::new(0));
        let consumers = spawn_consumers(cons_buffer, consumed, no_consumers, no_entries);

        for producer in producers {
            producer.join().unwrap();
        }

        let mut results = Vec::new();
        for consumer in consumers {
            results.append(&mut consumer.join().unwrap());
        }
        results.sort();

        results
    }

    #[test]
    fn two_producers_one_consumer() {
        let no_of_writes = 40_000;
        let results = producers_consumers_test(2, 1, no_of_writes);
        for i in 0..no_of_writes {
            assert_eq!(i, (results[i] as usize));
        }
    }

    #[test]
    fn three_producers_two_consumers() {
        let no_of_writes = 400_000;
        let results = producers_consumers_test(3, 2, no_of_writes);
        for i in 0..no_of_writes {
            assert_eq!(i, (results[i] as usize));
        }
    }

    #[test]
    fn eight_producers_five_consumers() {
        let no_of_writes = 4_000_000;
        let results = producers_consumers_test(8, 5, no_of_writes);
        for i in 0..no_of_writes {
            assert_eq!(i, (results[i] as usize));
        }
    }

    #[test]
    fn three_producers_eight_consumers() {
        let no_of_writes = 4_000_000;
        let results = producers_consumers_test(3, 8, no_of_writes);
        for i in 0..no_of_writes {
            assert_eq!(i, (results[i] as usize));
        }
    }

    #[test]
    fn eight_producers_one_consumers() {
        let no_of_writes = 400_000;
        let results = producers_consumers_test(8, 1, no_of_writes);
        for i in 0..no_of_writes {
            assert_eq!(i, (results[i] as usize));
        }
    }

    #[test]
    fn one_producers_ten_consumers() {
        let no_of_writes = 400_000;
        let results = producers_consumers_test(1, 10, no_of_writes);
        for i in 0..no_of_writes {
            assert_eq!(i, (results[i] as usize));
        }
    }
}