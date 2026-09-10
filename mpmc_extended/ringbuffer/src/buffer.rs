use std::{mem::MaybeUninit, sync::atomic::{AtomicU64, Ordering}};

const BUFFER_SIZE: usize = 500;

#[derive(Debug)] // AtomicU64 and MaybeUnit don't implement Copy so a new doesn't track
pub struct Slot<T> {
    seqno: AtomicU64, // make atomic in case multiple consumers?
    value: MaybeUninit<T>, // use MaybeUninit --> fixed size of potentially uninitialized T's
    ref_count: AtomicU64, // reference counter - to know how many readers are left before slot is available
}

/// Let RingBuffer support any type
#[derive(Debug)]
pub struct RingBuffer<T> {
    buffer: [Slot<T>; BUFFER_SIZE], 
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
impl<T> RingBuffer<T> {

    pub fn new() -> Self {
        Self { 
            buffer: std::array::from_fn(|i| Slot {
                seqno: AtomicU64::new(i as u64),
                value: MaybeUninit::uninit(),
                ref_count: AtomicU64::new(0),
            }), // each slot's initial seqno depends on its index
            head: AtomicU64::new(0),
        }
    }

    pub fn try_write(&self, value: T, no_subscribers: usize) -> Result<(), RingBufferError> {
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
                    Ordering::Acquire, 
                    Ordering::Relaxed);

                match result {
                    Ok(claimed_head) => { // note that claimed_head will always equal current_head based on compare_exchange docs
                        let slot_to_write = (claimed_head as usize) % BUFFER_SIZE;
                        let slot_value = self.buffer[slot_to_write].value.as_ptr() as *mut MaybeUninit<T>;
                        unsafe {
                            (*slot_value).write(value);
                        }

                        // Advance seqno and set the refcount to the no of subscribers for the topic in this slot
                        self.buffer[slot_index].ref_count.store(no_subscribers as u64, Ordering::Relaxed);
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

    /// Just returns reference to possible message in slot
    /// Things like updating ref_count are handled by update_after_read
    pub fn try_read(&self, read_cursor: &AtomicU64) -> Result<&T, RingBufferError> {
        let current_tail = read_cursor.load(Ordering::Acquire);

        let slot_index = (current_tail % (BUFFER_SIZE as u64)) as usize;
        let current_seqno = self.buffer[slot_index].seqno.load(Ordering::Acquire);
        
        // Use difference as splitting point rather than head/tail semantics
        let difference = current_seqno as i64 - (current_tail + 1) as i64;
        if difference == 0 { // ready for consumers
            // return reference to the value in slot, let the caller decide what to do with value
            let value_in_slot = unsafe {
                self.buffer[slot_index].value.assume_init_ref()
            };
            return Ok(value_in_slot);
        } else if difference < 0 { // Value not published ==> empty
            return Err(RingBufferError::BufferEmpty);
        } else { // ==> slot is ahead, retry
            return Err(RingBufferError::SlotAccessDenied);
        }
    }

    /// Multiple consumers can read, but not every message is useful (if not subscribed)
    /// When a message is useful, we need to update the ref_count (readers)
    /// And handle the situation where there's no one left to read, and we revert ownership to the producer
    pub fn update_after_read(&self, read_cursor: u64) -> Result<(), RingBufferError> {
        let slot_index = (read_cursor % (BUFFER_SIZE as u64)) as usize;

        // to get readers - use fetch_sub: multiple consumers can access slot, don't want to lose a decrement
        // fetch_sub subtracts from the current value (and stores that in the atomic variable) 
        // and returns the previous value
        let prev_readers = self.buffer[slot_index].ref_count.fetch_sub(1, Ordering::AcqRel);
        // If prev_readers is 1, advance seqno, and make slot available to producers
        // recall that fetch_sub returns the previous value so it being 1 implies that slot.readers is now 0
        if prev_readers == 1 {
            self.buffer[slot_index].seqno.store(read_cursor + (BUFFER_SIZE as u64), Ordering::Release);
        }
        Ok(())
    }
}