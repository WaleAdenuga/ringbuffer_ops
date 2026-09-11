# Ring Buffer Bitmask and Pressure-Handling Scope

## Purpose

This document scopes the changes needed to evolve the current prototype toward
the AgentCommunicator buffer model discussed so far.

The scope assumes:

- one global ring buffer, not one ring per topic;
- fixed physical slots rather than `Slot<T>` as the shared transport format;
- each agent owns a UUID and a `read_cursor`;
- topic subscriptions are fan-out: every subscriber to a topic should receive
  that message;
- normal operation should avoid explicit acknowledgement messages;
- slots should not be overwritten while their readers can still safely consume
  them;
- pressure handling may spill messages to an overflow journal as an emergency
  path;
- shared-memory placement, Binder, AiSeal, and the final eventfd mechanism are
  outside this document.

No Rust files are changed by this scope document.

## Current Prototype

### `src/buffer.rs`

The current transport has:

```text
RingBuffer<T>
  buffer: [Slot<T>; BUFFER_SIZE]
  head: AtomicU64

Slot<T>
  seqno: AtomicU64
  value: MaybeUninit<T>
  ref_count: AtomicU64
```

`try_write()` receives only `no_subscribers: usize`. This loses the identity of
the readers that own the message.

`try_read()` returns a reference into the slot. `update_after_read()` performs a
`fetch_sub(1)` and advances `seqno` when the previous count was one.

### `src/subscribe.rs`

The registry already contains the essential control-plane information:

```text
HashMap<MessageTopic, Vec<Uuid>>  // topic subscribers
HashMap<Uuid, AgentDetails>       // agent identity and state
```

Each `AgentDetails` currently contains:

- `read_cursor: AtomicU64`;
- subscribed topics;
- an `Arc<Event>` used by the prototype notification path.

This is the correct place to retain UUID identity and reader state. An agent
does not need to own a ring-buffer slot.

### `src/broker.rs`

`Broker::publish()` currently:

1. locks the registry;
2. obtains a `Vec<Uuid>` for the topic;
3. passes only `subscribers.len()` to the ring;
4. notifies subscribers by UUID.

`Broker::read()` uses the agent UUID to obtain its cursor and subscription set,
then advances the cursor across messages that do not belong to that agent.

## Core Change: Replace the Count with Reader Ownership

The current `ref_count` should not be treated as the final design. A scalar
count answers only “how many readers remain?” It cannot answer “which readers
remain?” That prevents targeted pressure notifications and makes agent death,
unsubscription, and reader-bit reuse difficult to handle safely.

Use a reader ownership mask instead:

```text
pending_readers: ReaderMask
ReaderMask = u64 for the initial bounded prototype
```

Each set bit identifies one registered reader. This is a compact registry index,
not a ring-buffer slot and not the agent’s cursor.

The registry entry remains UUID-based:

```text
AgentDetails
  uuid
  reader_id / reader_bit
  read_cursor
  subscriptions
  notification handle
  lifecycle state
```

The `reader_id` is only the mapping needed to represent UUIDs in a per-message
bitmask. If the design must support more than 64 simultaneous readers, replace
the single `u64` with a dynamically sized bitmap or bitmap words; that decision
should be made before treating `u64` as a wire/shared-memory format.

## Bitmask Lifecycle

### 1. Allocate a reader identity

At registration, the registry allocates a currently unused bit and associates it
with the agent UUID. The agent continues to use its UUID and `read_cursor` for
all public operations.

The registry must not reuse a reader bit until the previous owner’s outstanding
ownership has been cleared. A generation number may be added later to prevent
stale control events from being interpreted by a new agent using the same bit.

### 2. Build the topic subscriber mask

The registry should be able to derive:

```text
subscribers_for(topic) -> ReaderMask
```

The mask is a snapshot at publication time. A subscriber added later does not
own an earlier message. A subscriber removed later still owns messages that it
already appeared in unless its removal path explicitly releases that ownership.

The existing `HashMap<MessageTopic, Vec<Uuid>>` can remain as an identity-oriented
structure, but publication should obtain a mask without passing only a count.
The registry may maintain both:

- UUID collections for lifecycle and diagnostics;
- topic-to-reader-mask data for fast ownership lookup.

### 3. Publish

Conceptually, publication changes from:

```text
try_write(message, subscribers.len())
```

to:

```text
try_write(message, subscribers_for(message.topic))
```

The slot stores the exact subscriber snapshot as `pending_readers`.

If the mask is empty, the broker should have an explicit policy: reject the
publication, discard it without occupying a slot, or retain it for journaling.
The current prototype rejects topics without subscribers.

### 4. Read and release

The agent’s `read_cursor` still determines which logical slot it examines. Once
the agent has safely copied the message out of the slot, it clears only its own
bit:

```text
my_bit = reader_bit(agent.reader_id)
old_mask = pending_readers.fetch_and(!my_bit)
```

The `fetch_and` notation is conceptual here; the implementation must use the
appropriate atomic read-modify-write operation. A normal non-atomic
`pending_readers & !my_bit` expression would only calculate a new value and
would not update the slot.

The actual implementation must use an atomic read-modify-write operation. The
important property is that clearing a bit is idempotent: accidentally clearing
the same bit twice must not underflow or release the slot early.

The slot is logically releasable when:

```text
pending_readers == 0
```

The release operation must happen after the reader has finished copying the
payload. Whether “release” means copied, decrypted, callback-dispatched, or
fully processed is an API decision; the transport should not silently mix
these meanings.

### 5. Targeted notification

The bitmask directly solves the targeted-notification problem.

When the monitor identifies a blocked slot, it reads that slot’s pending reader
mask. It then resolves each set bit through the registry:

```text
blocked_mask = slot.pending_readers

for each set bit in blocked_mask:
    reader_id = bit index
    agent = registry.lookup_by_reader_id(reader_id)
    signal(agent.notification_handle)
```

Only agents that still own the message are signaled. Unrelated agents do not
receive the pressure notification, and the monitor does not need to broadcast
to every registered agent.

The notification itself should remain a wakeup, not a data payload. The agent
can inspect control state or its cursor after waking to determine what it must
drain.

If a fixed `u64` reader identity is undesirable, the alternative is to skip the
bitmask for targeting and scan the UUID registry during pressure handling:

```text
for each agent:
    if agent subscribes to blocked_topic
       and agent.read_cursor is behind blocked_sequence:
        signal(agent.notification_handle)
```

This has no 64-reader limit but costs `O(number_of_agents)` during pressure.
The bitmask is therefore an optimization and an ownership record, not a
requirement for targeted notification itself.

## Slot State

The slot lifecycle needs more than a sequence number once forced journaling is
introduced. A conceptual state machine is:

```text
FREE
  -> WRITING
  -> READY
  -> EVICTING
  -> JOURNALED
  -> FREE
```

The state should be represented by an atomic state value in the eventual
implementation, not a cross-process Rust mutex. A mutex may still protect the
service-local registry `HashMap`.

The important transitions are:

- `FREE -> WRITING`: a producer claims the physical slot;
- `WRITING -> READY`: payload and reader mask are fully published;
- `READY -> EVICTING`: pressure handling prevents new readers from claiming
  the slot while preparing journal fallback;
- `EVICTING -> JOURNALED`: the complete message is safely recorded in the
  journal;
- `JOURNALED -> FREE`: no reader is still actively copying the physical slot.

`pending_readers` alone does not distinguish a reader that has not started from
one that is currently copying. If a slot can be overwritten while a reader is
copying, an additional active-reader claim protocol or active-reader mask is
needed. Otherwise the writer could modify payload bytes concurrently with a
reader.

## Agent Unregister and Failure

When an agent unregisters or dies:

1. Remove it from future topic subscriber masks.
2. Mark its registry record unavailable.
3. Clear its reader bit from outstanding messages, or mark the bit as revoked
   and make reclamation ignore it.
4. Wake the producer/reclaimer if the agent was blocking the oldest region.
5. Recycle the reader bit only after stale ownership cannot affect a new agent.

The cleanup must be idempotent. A normal unregister and a death notification may
race, and both must be safe to apply.

## Pressure Monitoring and Journal Fallback

Use one monitor or pressure-control loop rather than a timer per slot. It can
inspect the oldest unreleased region and derive the set of readers blocking
reclamation.

Recommended terminology:

```text
PRESSURE_WATERMARK  = 80%  // targeted drain notification
CRITICAL_WATERMARK  = 90%  // begin spill/reclamation policy
RECOVERY_WATERMARK  = lower value, e.g. 50–60%
```

If “clear at 90%” means clearing the pressure condition while occupancy is
rising, that is inverted. If it means escalating at 90%, call it critical. A
true hysteresis policy needs a lower recovery watermark than its trigger so the
system does not oscillate.

The emergency spill path should be:

```text
identify blocked slots
signal only their pending readers
allow normal readers to release ownership
if critical pressure persists:
    enter EVICTING
    journal complete messages before reuse
    prevent new physical readers from claiming those slots
    wait for active copies to finish
    mark slots JOURNALED/FREE
```

Readers that encounter a journaled gap need a precise sequence range and topic,
not merely a generic error. Replay also needs deduplication and a defined handoff
back to the live ring.

## Current-Code Changes to Scope Later

These are implementation changes, not part of this documentation pass:

### `src/buffer.rs`

- Replace `Slot<T>` with the fixed physical slot representation.
- Replace `ref_count` with reader ownership state.
- Add slot lifecycle state.
- Change `try_write()` to accept a reader mask rather than a subscriber count.
- Make release idempotent and reader-specific.
- Define safe ownership around `MaybeUninit` or remove `MaybeUninit` by storing a
  fixed wire payload.
- Ensure a slot is not physically reused while a reader can still access it.

### `src/message.rs`

- Keep topic-specific logical messages above the physical slot layer.
- Keep the fixed payload representation separate from the ring ownership
  metadata.
- Decide whether `Message` is the fixed wire envelope or an API-level value.

### `src/subscribe.rs`

- Add reader-bit allocation and lookup, or explicitly choose registry scanning
  instead of bitmasks.
- Add a topic-to-reader-mask lookup.
- Preserve UUID-to-agent details for notification targeting.
- Define bit cleanup and reuse behavior.
- Define unregister/death cleanup for outstanding ownership.

### `src/broker.rs`

- Pass the topic reader mask into the buffer.
- Do not hold the registry mutex across unnecessary buffer operations.
- Target notifications using the ownership mask rather than blindly iterating
  all topic subscribers.
- Resolve the current publisher-notification policy. The current code counts
  the publisher if subscribed but skips notifying it, which can leave an
  ownership bit unreleased.

### `src/agent.rs`

- Keep the UUID and read cursor as agent identity/state.
- Add the registry-assigned reader identity only if the bounded bitmask option
  is selected.
- Make pressure/replay behavior part of the agent-facing contract later.

## Required Tests

Before adding journal fallback, test the ownership mask in isolation:

- one topic with one reader;
- one topic with multiple readers;
- a reader clearing its bit twice;
- a reader attempting to clear a message it does not own;
- unrelated-topic readers not appearing in the mask;
- a publisher that is also a subscriber;
- no-subscriber publication;
- simultaneous reader releases where exactly one release observes the final
  ownership transition;
- unregister/death cleanup;
- reader-bit reuse without stale ownership;
- targeted pressure notification reaches only set bits;
- registry scanning alternative, if the 64-reader limit is rejected.

For the state/journal phase, add:

- reader-versus-eviction race tests;
- no payload mutation while an active reader is copying;
- journal-before-reuse ordering;
- sequence-gap detection;
- replay/live-ring deduplication;
- chunked-message spill and replay;
- critical-to-recovery hysteresis;
- journal failure behavior.

## Recommended Implementation Order

1. Document the ownership invariants and release point.
2. Add a registry reader identity or choose registry scanning.
3. Return topic subscriber identity information, not only a count.
4. Replace the scalar refcount with a pending-reader mask.
5. Add idempotent release and agent cleanup.
6. Add atomic slot states and the active-reader safety protocol.
7. Add targeted pressure monitoring.
8. Add journal spill and replay.
9. Benchmark one global ring against any future partitioned/ring-per-topic
   alternative.

## Explicitly Out of Scope

- Editing or implementing the Rust files during this scope pass.
- AiSeal crypto placement.
- Binder service design.
- Cloud-agent key exchange.
- Final shared-memory ABI and memory-ordering proof.
- Guaranteed delivery semantics beyond the emergency journal behavior.
- Automatically choosing between a single `u64` mask and a dynamic bitmap.

## Concrete Code-Change Sketch

The following sketches are intentionally implementation-oriented but are not
drop-in code. They describe the interfaces and ownership rules that the Rust
changes should implement.

### `src/subscribe.rs`: Reader Identity and Masks

The registry should retain UUIDs as the public identity while assigning a
compact bit position for ownership tracking:

```rust
pub type ReaderMask = u64;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReaderId(pub u8); // bit position, not a ring-buffer slot

impl ReaderId {
    pub fn bit(self) -> ReaderMask {
        assert!(self.0 < 64);
        1u64 << self.0
    }
}

pub struct AgentDetails {
    pub reader_id: ReaderId,
    pub read_cursor: AtomicU64,
    pub subscriptions: HashSet<MessageTopic>,
    pub listener: Arc<Event>, // eventfd adapter in the device version
    pub alive: bool,
}
```

The `ReaderId` is only an internal dense key used in masks. It does not replace
the UUID or the agent’s cursor.

The registry needs a reverse lookup so a mask can be converted into targeted
notifications:

```rust
pub struct Registry {
    pub agents: HashMap<Uuid, AgentDetails>,
    pub reader_to_agent: [Option<Uuid>; 64],
    pub topic_masks: HashMap<MessageTopic, ReaderMask>,
}

impl Registry {
    pub fn readers_for_topic(&self, topic: MessageTopic) -> ReaderMask;

    pub fn agent_for_reader(&self, reader: ReaderId) -> Option<&AgentDetails>;

    pub fn notify_reader_mask(&self, mask: ReaderMask);

    pub fn release_reader(&mut self, agent_id: &Uuid);
}
```

`topic_masks` can be maintained alongside the existing UUID vectors. The UUID
vectors remain useful for diagnostics and lifecycle operations; the mask avoids
allocating and scanning a `Vec<Uuid>` for every publication.

The current `register()` path should allocate an unused reader bit and store it
in `AgentDetails`. `subscribe()` and `unsubscribe()` should update both the
agent’s `HashSet` and the topic mask. `unregister()` should remove the UUID from
all topic masks and trigger ownership cleanup before the reader bit is reused.

### `src/buffer.rs`: Fixed Slot and Reader Ownership

The transport should stop making the physical slot generic. A conceptual fixed
slot is:

```rust
#[repr(C)]
pub struct Slot {
    pub seqno: AtomicU64,
    pub state: AtomicU8,
    pub pending_readers: AtomicU64,
    pub active_readers: AtomicU64,
    pub topic_id: u8,
    pub flags: u8,
    pub payload_len: u32,
    pub payload: [u8; MAX_PAYLOAD_SIZE],
}
```

This is the physical transport representation. `Message` remains an API-level
value created after the slot’s fixed bytes are copied and decoded. The physical
slot should not contain Rust enums, `usize`, `Vec`, references, or other
pointer-bearing values. The exact alignment, padding, and payload size are still
open.

The important changes are:

- no `Slot<T>` in the physical ring;
- no scalar `ref_count`;
- ownership is represented by a reader mask;
- active readers are tracked separately if forced eviction can happen;
- the slot state is explicit.

The write API should receive the identity mask:

```rust
pub fn try_write(
    &self,
    message: Message,
    readers: ReaderMask,
) -> Result<u64, RingBufferError>;
```

The producer stores the subscriber snapshot in the slot. It must not look up
the current subscriber count after writing, because the current registry may
have changed by then.

The read path should no longer return a long-lived `&T` into a reusable slot.
The minimum safe shape is an owned read result:

```rust
pub struct ReadRecord {
    pub sequence: u64,
    pub message: Message,
}

pub fn try_read(
    &self,
    cursor: &AtomicU64,
    reader: ReaderId,
) -> Result<ReadRecord, RingBufferError>;

pub fn release_read(
    &self,
    sequence: u64,
    reader: ReaderId,
) -> Result<ReleaseResult, RingBufferError>;
```

The reader’s normal sequence is:

```text
locate slot from read_cursor
verify slot is READY for the expected sequence
claim/mark active reader
copy the fixed message out of the slot
clear the reader’s active bit
clear the reader’s pending bit
advance read_cursor
```

In the recommended model, the pending bit remains set while the reader is
actively copying. The active bit is a safety guard that tells the monitor that
the reader is currently touching the physical payload. After copying completes,
the reader clears `active_readers`, then clears `pending_readers`, then advances
its cursor according to the final cursor-ordering decision.

The exact ordering of those operations must be fixed by the implementation. A
reader must never release either form of ownership before it has copied all
bytes it needs from the slot.

The old method:

```rust
ref_count.fetch_sub(1, Ordering::AcqRel)
```

should not be carried forward. Bit clearing is idempotent and can report
whether the caller actually owned the bit:

```text
old_mask = pending_readers.fetch_and(!reader_bit)
if old_mask did not contain reader_bit:
    duplicate release or no ownership
if old_mask contained only reader_bit:
    pending ownership reached zero
```

The actual implementation must also account for `active_readers` before
physically reusing a slot.

### Pending/active ownership alternatives

There are two valid ownership conventions:

1. **Pending-through-copy (recommended):** `pending_readers` means readers that
   have not completed delivery. A reader sets `active_readers` while copying and
   clears both masks only after copying. This makes pressure targeting simple;
   the pending mask directly identifies readers that still need the message.
2. **Pending-at-claim:** a reader clears `pending_readers` when it claims the
   slot and uses `active_readers` as the only physical-safety guard. This can
   make logical ownership look complete before delivery finishes and requires
   more careful pressure/replay bookkeeping.

The first convention is recommended unless a benchmark shows that keeping the
pending bit through the copy is materially expensive.

### `src/broker.rs`: Snapshot, Publish, and Notify

`Broker::publish()` should take a subscriber mask snapshot while holding the
registry lock, then release the lock before doing buffer work:

```rust
pub fn publish(
    &self,
    agent_id: &Uuid,
    message: Message,
) -> Result<(), RegistryError> {
    let readers = {
        let registry = self.registry();
        registry.require_agent(agent_id)?;
        registry.readers_for_topic(message.topic)
    };

    if readers == 0 {
        return Err(RegistryError::NoSubscriberForTopic);
    }

    let sequence = self.buffer.try_write(message, readers)?;

    // Notification is targeted to the publication snapshot. If an agent
    // unregisters between snapshot and notification, the registry ignores it.
    self.registry().notify_reader_mask(readers);
    Ok(())
}
```

The exact lock lifetime and error conversion need to be adapted to the current
implementation. The important semantic changes are:

- pass a mask, not `subscribers.len()`;
- notify exactly the same readers represented by that mask;
- decide consistently whether a publisher subscribed to its own topic receives
  and gets notified about its own message.

The current code counts the publisher if it is subscribed but skips notifying
the publisher. That can leave an ownership bit unreleased. Either include the
publisher in both the mask and notifications, or exclude it from both.

### `src/agent.rs`: Preserve UUID and Cursor Ownership

`Agent` should continue to expose the UUID and broker-facing operations. It may
cache its registry-assigned `ReaderId`, but the reader identity is not a ring
slot and does not replace `read_cursor`:

```rust
pub struct Agent {
    pub id: Uuid,
    pub reader_id: ReaderId,
    pub event: Arc<Event>,
    pub broker: Arc<Broker>,
}
```

If the project chooses registry scanning instead of a bounded mask, `reader_id`
can be omitted from the public `Agent` object and kept entirely inside the
registry.

## Proposed `src/monitor.rs`

The monitor is a single pressure-control component. It is not a per-slot timer
and it is not part of the normal publish/read data path.

### Responsibilities

`monitor.rs` should:

1. observe ring occupancy and the oldest unreleased sequence;
2. identify which reader masks are blocking reclamation;
3. signal only those readers;
4. escalate from pressure to critical spill mode;
5. coordinate journal-before-reuse;
6. return the system to normal after enough slots are released;
7. wake when publication, release, agent lifecycle, or journal state changes.

It should not own the registry’s UUID identity model. It asks the registry to
resolve a reader mask into notification handles.

### Configuration

```rust
pub struct MonitorConfig {
    pub pressure_watermark: u32, // e.g. 80 percent
    pub critical_watermark: u32, // e.g. 90 percent
    pub recovery_watermark: u32, // e.g. 50–60 percent
    pub scan_interval: Duration,
    pub pressure_grace: Duration,
    pub max_spill_batch: usize,
}
```

The terminology should distinguish three events:

```text
80% occupancy  -> PRESSURE: targeted drain notification
90% occupancy  -> CRITICAL: allow journal spill
50–60%         -> RECOVERY: clear pressure state
```

If 90% was intended to mean “clear the alert,” that is not hysteresis for an
occupancy value that rises toward 100%. It is better represented as a critical
watermark, with a lower recovery watermark.

### Monitor state

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PressureState {
    Normal,
    Pressure,
    Critical,
}

pub struct Monitor {
    pub config: MonitorConfig,
    pub state: PressureState,
    pub pressure_epoch: u64,
    // references to the buffer, registry, wakeup source, and journal
}
```

The state transition logic should be explicit:

```text
Normal + occupancy >= pressure_watermark
    -> Pressure

Pressure + occupancy >= critical_watermark
    -> Critical

Critical + occupancy < critical_watermark
    -> Pressure

Pressure + occupancy <= recovery_watermark
    -> Normal
```

An epoch increments whenever a new pressure episode begins. Agents can use the
epoch to avoid repeatedly handling the same pressure request.

### Monitor loop

The monitor can be woken by a shared `Event`/eventfd adapter and can retain a
periodic scan as a safety net:

```rust
pub fn run(mut self) {
    loop {
        self.wait_for_activity_or_scan_deadline();

        let snapshot = self.buffer.pressure_snapshot();
        self.update_pressure_state(snapshot.occupancy);

        match self.state {
            PressureState::Normal => {}
            PressureState::Pressure => {
                self.signal_blocking_readers(&snapshot);
            }
            PressureState::Critical => {
                self.signal_blocking_readers(&snapshot);
                self.spill_blocking_slots(&snapshot);
            }
        }
    }
}
```

The monitor should not hold the registry mutex while performing journal I/O. It
should first snapshot the target reader IDs or notification handles, release
the registry lock, and then signal or journal.

### Occupancy measurement alternatives

The monitor needs a defined source for `snapshot.occupancy`; the current
prototype has `head` but no reclaim-frontier field. There are three options:

1. **Reclaim frontier (recommended):** add a logical `reclaim_cursor`. It
   advances only across consecutively reusable slots. Occupancy is derived from
   `head - reclaim_cursor`. This identifies both the occupancy level and the
   oldest region that is blocking reuse.
2. **Occupied-slot counter:** increment when a producer claims a slot and
   decrement when a slot becomes reusable. This is cheap to read but does not by
   itself identify which oldest slot or readers are blocking reclamation.
3. **Monitor scan:** inspect sequence/state for the ring’s slots on each monitor
   pass. This avoids another counter but costs `O(BUFFER_SIZE)` per scan and is
   most useful as a diagnostic or pressure-path fallback.

The recommended design is a reclaim frontier for normal decisions, with a
bounded scan of the oldest unreleased range when pressure needs the exact
blocking masks. A conceptual snapshot is:

```rust
pub struct PressureSnapshot {
    pub head: u64,
    pub reclaim_cursor: u64,
    pub occupancy: u32,
    pub blocked_slots: Vec<BlockedSlot>,
}
```

The monitor must define whether `occupancy` is measured before or after a
producer reservation and use that convention consistently at the watermarks.

### Identifying blocking readers

For each oldest unreleased slot, the monitor obtains:

```rust
pub struct BlockedSlot {
    pub sequence: u64,
    pub topic: MessageTopic,
    pub pending_readers: ReaderMask,
    pub active_readers: ReaderMask,
}
```

The targeted notification mask is the union of the masks for the slots that
are preventing the reclaim frontier from advancing:

```text
notify_mask = pending_readers | active_readers
```

The monitor then calls a registry operation conceptually equivalent to:

```rust
pub struct PressureNotification {
    pub epoch: u64,
    pub oldest_sequence: u64,
    pub newest_sequence: u64,
}

// The notification metadata is recorded in the agent/control state before
// the wakeup handles are signaled. The event itself remains only a wakeup.
registry.notify_reader_mask(
    notify_mask,
    PressureNotification {
        epoch,
        oldest_sequence,
        newest_sequence,
    },
);
```

In the current prototype, `notify_reader_mask()` resolves each reader bit to an
`Arc<Event>` and calls `notify(1)`. In the device implementation, the same
operation resolves to the agent’s eventfd. The notification carries no message
data; it only wakes the selected agent.

### Spill coordination

Spilling must not blindly overwrite a slot with active readers. A monitor pass
should use a non-blocking state transition:

```text
READY -> EVICTING
```

Once `EVICTING`:

- new readers do not claim the physical payload;
- readers that already claimed it may finish copying;
- the monitor appends the complete message to the journal exactly once;
- the slot records the journal sequence/location or an equivalent durable
  spill marker;
- the monitor waits across later passes for `active_readers == 0`;
- pending readers that did not consume the physical slot are redirected to
  journal replay;
- only then does the slot become reusable.

This avoids blocking the monitor thread on one reader while still preventing a
writer from modifying payload bytes during an active read.

The `READY -> EVICTING` transition must be claimed atomically. A later monitor
pass that sees `EVICTING` must inspect the existing journal marker rather than
append the message again. If the journal append fails, the slot must remain
non-reusable and the monitor must apply an explicit failure policy: retry,
publisher backpressure, or message loss.

The journal interface should expose a sequence-addressable operation:

```rust
pub trait OverflowJournal {
    fn append(&self, record: JournalRecord) -> Result<JournalLocation, JournalError>;
    fn replay(
        &self,
        topic: MessageTopic,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<Vec<Message>, JournalError>;
}
```

The reader-facing result should identify a range and replay source, not merely
return an undifferentiated read error:

```rust
pub enum ReadOutcome {
    Live(Message),
    JournalGap {
        topic: MessageTopic,
        first_sequence: u64,
        last_sequence: u64,
    },
    Empty,
    Retry,
}
```

Whether replay is transparent inside `Broker::read()` or exposed to the agent
API is a later API decision. Internally, replay should deduplicate against live
messages using sequence numbers.

## Registry API Sketch

The current registry API returns UUID vectors. The minimum additions are:

```rust
impl Registry {
    pub fn readers_for_topic(&self, topic: &MessageTopic) -> ReaderMask {
        self.topic_masks.get(topic).copied().unwrap_or(0)
    }

    pub fn notify_reader_mask(
        &self,
        mask: ReaderMask,
        notification: &PressureNotification,
    ) {
        let mut bits = mask;
        while bits != 0 {
            let index = bits.trailing_zeros() as usize;
            bits &= bits - 1;

            if let Some(Some(agent_id)) = self.reader_to_agent.get(index) {
                if let Some(details) = self.agents.get(agent_id) {
                    // The device implementation records `notification` in
                    // per-agent control state before signaling the eventfd.
                    let _ = notification;
                    details.listener.notify(1);
                }
            }
        }
    }
}
```

The production version should not hold a mutable registry lock while writing
eventfds. It should snapshot notification targets first, then signal them.

## Important Prototype Corrections

The scope should account for these existing issues while implementing the new
model:

1. `try_write()` currently passes and stores only a count. It must receive the
   publication-time reader mask.
2. `update_after_read()` can underflow if a reader releases ownership it does
   not have. Mask clearing makes the operation idempotent.
3. `try_read()` returns a reference into a reusable slot. The fixed-slot API
   should copy or reserve the record safely before release/reuse.
4. The current publisher notification loop skips the publisher while the
   count may include it. The ownership mask and notification set must match.
5. `Registry::unregister()` removes UUID mappings but currently has no way to
   release outstanding buffer ownership.
6. `get_subscribers_for()` exposes a `Vec<Uuid>` while the write path needs a
   stable publication snapshot. The mask is that snapshot.
7. The current `RingBuffer<T>` overwrites `MaybeUninit<T>` without a defined
   drop policy for arbitrary `T`. A concrete fixed message/wire slot avoids
   this generic ownership problem.
8. Registry operations currently remain locked across some buffer operations.
   The monitor and publisher should use short registry snapshots instead.

## Monitor Tests

Add monitor-focused tests before integrating journal I/O:

```text
normal occupancy does not notify anyone
80% occupancy notifies only pending readers
unrelated-topic readers are not notified
90% occupancy transitions to Critical
critical state journals only blocked slots
pressure does not create duplicate notification obligations
recovery below the lower watermark returns to Normal
agent unregister removes its bit from future notifications
agent unregister releases its outstanding ownership
reader-bit reuse cannot target the new agent with stale work
```

Add concurrency tests for:

```text
reader release racing with monitor inspection
reader claim racing with READY -> EVICTING
last reader release racing with producer reclamation
multiple producers entering pressure simultaneously
journal append failure during Critical state
```

## Design Questions and Recommended Alternatives

### Reader identity representation

**Option A — bounded `u64` reader mask (recommended for the prototype):**

- assign each live agent a temporary `ReaderId` from 0–63;
- keep UUID and `read_cursor` as the agent’s real identity/state;
- use the bit only inside topic masks and slot ownership masks;
- resolve bits through the UUID registry for targeted notifications.

This has constant-size per-slot state and fast target selection, but caps the
number of simultaneous readers at 64 unless the mask becomes an array of words.

**Option B — dynamic bitmap:**

- use a bitmap with enough words for the configured reader limit;
- preserve the same ownership and targeted-notification model;
- pay additional per-slot memory and atomic-update cost.

This is appropriate if more than 64 readers is a near-term requirement.

**Option C — registry scan:**

- store no reader bit in each slot;
- compare each agent’s UUID-associated cursor and subscriptions against the
  blocked sequence range during pressure;
- signal matching eventfds directly.

This removes the reader-count limit and simplifies bit reuse, but costs
`O(number_of_agents)` whenever pressure handling runs and makes per-message
ownership less explicit.

### Occupancy calculation

**Option A — reclaim frontier (recommended):** best for quickly identifying the
oldest blocked region and implementing a bounded ring.

**Option B — occupied counter:** best if only a coarse watermark is needed, but
it still requires another method to find the readers blocking the oldest slot.

**Option C — scan:** simplest to reason about in the prototype, but potentially
expensive if the monitor scans frequently. It is a good verification mechanism
for testing the frontier/counter implementation.

### Pressure thresholds

**Option A — pressure/critical/recovery (recommended):**

```text
80% occupancy -> notify targeted readers
90% occupancy -> permit journal spill
50–60%       -> clear pressure state
```

**Option B — two-level pressure only:** notify at 80% and spill at 90%, with a
separate fixed cooldown before returning to normal. This is simpler but can
oscillate if occupancy remains near a threshold.

If “clear at 90%” was intended literally, the document needs a different
definition of the measured quantity. For an occupancy value that rises toward
100%, 90% is an escalation threshold, not a recovery threshold. If the measured
quantity is free capacity, the numerical direction should be inverted.

### Pressure notification delivery

**Option A — eventfd/`Arc<Event>` wakeup plus shared control state
(recommended):** the event carries only a wakeup; the agent reads its pressure
epoch and sequence range from control state.

**Option B — encoded eventfd values:** use the event counter value to identify a
notification class or epoch. This reduces a control-state read but does not
carry a full sequence range and makes coalescing semantics harder to reason
about.

**Option C — direct registry callback:** invoke an agent callback from the
monitor. This is unsuitable for the final process-separated design and should
remain a host-test-only possibility.

### Replay API boundary

**Option A — transparent replay (recommended):** `Broker::read()` detects a
journal gap, replays it, deduplicates by sequence, and returns ordinary
messages to the agent.

**Option B — explicit replay result:** return a `JournalGap` result and require
the agent to query the journal. This exposes more control but complicates every
agent and makes live/journal handoff part of the public API.

**Option C — journal only as an administrative recovery path:** notify that
messages were spilled but do not automatically replay them. This is simplest,
but it no longer provides the intended no-loss emergency behavior.

Transparent replay is the better default if the journal is part of the normal
AgentCommunicator contract; explicit replay may be appropriate for a later
advanced API.

### Journal scope

**Option A — journal complete logical messages (recommended):** chunked messages
are appended and replayed as one logical record with a sequence range.

**Option B — journal physical slots:** simpler storage code, but readers must
reconstruct chunk sequences and handle partial journal records.

The complete-message approach is safer for replay and deduplication, while the
slot approach may be easier to integrate with the current physical ring.

## Suggested File-Level Work Breakdown

### New file: `src/monitor.rs`

- `MonitorConfig`;
- `PressureState`;
- `PressureSnapshot` / `BlockedSlot`;
- `Monitor` lifecycle and wakeup loop;
- pressure transition logic;
- targeted notification orchestration;
- spill/reclamation coordination;
- unit tests for thresholds and state transitions.

### Modify: `src/buffer.rs`

- concrete fixed `Slot`;
- `ReaderMask` ownership;
- explicit slot state;
- safe read reservation/release API;
- pressure snapshot API;
- reclaim-frontier tracking.

### Modify: `src/subscribe.rs`

- reader-bit allocation and reverse lookup;
- topic masks;
- targeted notification by mask;
- unregister/death cleanup;
- reader-bit lifecycle and generation policy.

### Modify: `src/broker.rs`

- publication mask snapshot;
- mask-based buffer write;
- targeted normal and pressure notifications;
- shorter registry lock scope;
- journal replay handoff.

### Modify: `src/agent.rs`

- preserve UUID and cursor ownership;
- optionally cache the internal reader identity;
- handle pressure/replay notifications.

### Modify: `src/main.rs`

- declare `pub mod monitor;`;
- construct and shut down one monitor for the broker/buffer lifecycle.

### Optional future file: `src/journal.rs`

- append-only overflow records;
- sequence/topic indexes;
- replay and deduplication;
- journal retention and failure handling.
