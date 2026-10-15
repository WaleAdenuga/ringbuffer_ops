# Ringbuffer Prototype — Implementation and Test Plan

> Current host-prototype plan, October 2026. This document is intentionally
> implementation-oriented. It reflects the current AgentCommunicator design, not
> the earlier blind-overwrite design.

## Scope and Fixed Decisions

This crate proves the transport before it moves to Android shared memory. It is
not yet an Android Binder service and it does not implement AiSeal or Protobuf.

| Item | Prototype target |
|---|---|
| Ring | Fixed `RingBuffer`, 500 physical slots |
| Payload | Opaque bytes, 4 KiB per slot |
| Producers | Many concurrent publishers |
| Readers | Up to 64 agent bits; independent monotonic cursors |
| Topics | 256 dynamically allocated `u8` IDs (service/catalog is modelled locally) |
| Delivery | Pub/sub fan-out; publisher never receives its own publication |
| Normal reclamation | Last recipient releases the slot; no normal-path overwrite |
| Pressure safeguard | >80% targeted drain wakeups; >90% journal-before-free reclamation |
| Full ring | Return backpressure/`BufferFull` if normal draining and journal spill cannot free space |
| Large message | Reserve `1..=255` contiguous logical slots; physical wrap is valid |
| Notification | Host event abstraction now; Android later has broker ingress + per-agent eventfds |

The public payload stays opaque for now. Protobuf/bincode, fixed cross-process ABI
types, AES-GCM, AiSeal, `memfd`, Binder, and publisher-death recovery come after
the queue semantics are correct.

## Current Code: What Is Present

`src/buffer.rs` already has the essential MPMC reservation idea:

- a global atomic `head`;
- a per-slot sequence number;
- CAS ownership of a producer position;
- a `u64` `pending_readers` mask;
- release when the final reader clears its bit.

`src/subscribe.rs` has an in-process UUID registry, subscriptions, reader-bit
allocation, per-agent cursors, and `event_listener::Event` handles. `src/broker.rs`
connects registration, publishing, reading, and notification. `src/agent.rs`
provides an intentionally small agent-facing wrapper.

That is a useful proof-of-concept shape, but it is not yet a correct message bus.

## Target Block Diagrams

### Final on-device topology

```text
                         Android vendor process
┌───────────────────────────────────────────────────────────────────────────┐
│ AgentCommunicatorService                                                   │
│                                                                           │
│  AgentRegistry + TopicCatalog + TopicKey authority                       │
│             │ Binder: register / topics / keys / lifecycle               │
│             │                                                             │
│  Service-side Broker                                                      │
│    broker_ingress_eventfd  <── publisher kick after commit                │
│    notification_cursor     ──> recipient data_eventfd fan-out             │
│    pressure monitor        ──> 80% targeted drain / 90% journal spill     │
└─────────────┬─────────────────────────────────────────────────────────────┘
              │ fd handoff at registration; no Binder payload path
              ▼
┌──────────────────────────────── Shared memfd ─────────────────────────────┐
│ Header: write cursor | catalog generation | agent registry                 │
│ TopicIndex[256]: AtomicU64 subscriber masks                                │
│ Agent cursors[64]: padded AtomicU64                                        │
│ Ring[500]: sequence | state | pending readers | active readers | payload   │
└─────────────┬─────────────────────────────────────────────────────────────┘
              │ direct atomic read/write
   ┌──────────┴──────────┐                                  ┌───────────────┐
   ▼                     ▼                                  ▼               ▼
Publisher AgentBus   Publisher AgentBus                Subscriber AgentBus ...
serialize/encrypt    reserve/write/commit              epoll(data, journal)
local topic key      broker ingress event              copy/drain; journal recovery batch
```

### Pressure-safeguard state flow

```text
                 normal reader progress
FREE -> WRITING -> READY ---------------------> FREE
                  │  pending mask reaches zero
                  │
                  │ occupancy > 80%
                  ▼
          targeted data_eventfd wake to pending-reader bits
                  │
                  │ occupancy > 90% and draining did not recover
                  ▼
READY -> EVICTING -> journal complete logical message -> JOURNALED -> FREE
                  │          │                               │
                  │          └─ wait active_reader mask == 0  └─ signal journal-only
                  │                                               event; agent drains its
                  │                                               recovery batch later
                  └─ new physical copy claims denied
```

### Host prototype versus final placement

```text
TODAY (one process)                         FINAL (many processes)
───────────────────                         ───────────────────────
Agent -> Arc<Broker> -> RingBuffer          AgentBus -> mmap SharedRingBuffer
     \-> Arc<Event>                              \-> broker_ingress_eventfd
Registry Mutex<HashMap>                     Service registry + shared atomic TopicIndex
MessageTopic enum                           Dynamic TopicId(u8) + descriptor generation
UnsafeCell<Message>                         fixed #[repr(C)] bytes + explicit state
```

## Desired Repository Layout

The prototype should grow into small, testable modules before Android files are
introduced. Names are suggestions; the separation and dependency direction matter
more than exact filenames.

```text
ringbuffer/
├── src/
│   ├── lib.rs                 # public host-test API; main.rs becomes optional demo
│   ├── ring/
│   │   ├── mod.rs             # RingBuffer and public errors
│   │   ├── slot.rs            # Slot state, sequence helpers, owned ReadRecord
│   │   ├── reserve.rs         # single/range reservation and commit protocol
│   │   ├── read.rs            # cursor, active claims, release/fast-forward
│   │   └── pressure.rs        # host pressure monitor and journal interface
│   ├── message/
│   │   ├── mod.rs             # opaque payload message API
│   │   ├── topic.rs           # TopicId, generation, descriptor (host model)
│   │   └── chunk.rs           # chunk metadata and reassembly validation
│   ├── registry/
│   │   ├── mod.rs             # agent slot allocation and lifecycle
│   │   └── catalog.rs         # dynamic topics and visibility
│   ├── broker/
│   │   ├── mod.rs             # host broker loop facade
│   │   └── notifier.rs        # data/control notification traits
│   └── test_support/
│       ├── reference_bus.rs   # deterministic oracle, test only
│       └── model.rs           # loom/reduced capacity helpers
├── tests/
│   ├── ring_single_slot.rs
│   ├── fanout_and_lifecycle.rs
│   ├── chunks.rs
│   ├── pressure_journal.rs
│   ├── mpmc_stress.rs
│   └── loom_model.rs
└── PROTOTYPE_IMPLEMENTATION_PLAN.md
```

`main.rs` should not be the only crate root for queue tests. Make the queue a
library so integration tests can use only its public behavior and cannot depend on
private slot fields accidentally.

## Current Shortcomings

### Ring safety and ownership

1. **Unsafe borrowed reads.** `try_read()` returns `&Message` from an
   `UnsafeCell`. The caller can retain that reference while another reader frees
   the slot and a producer overwrites its bytes. The final transport must copy an
   owned record before clearing any reader ownership.
2. **No explicit publication state.** A sequence number distinguishes free/ready
   generations, but a multi-slot reservation needs `WRITING` and one
   reservation-level `READY`/commit state. Readers and the future broker must not
   observe a partial chunk range.
3. **No range reservation.** `try_write()` claims one head position only. It
   cannot reserve a contiguous chunk range atomically or roll back a failed range.
4. **No fast-forward.** `try_read()` returns `SlotAccessDenied` when the physical
   slot contains a newer generation. An agent that did not own skipped records
   must advance its cursor instead.
5. **Release lacks validation.** `update_after_read()` does not verify the expected
   generation or that the prior mask contained `agent_bit`. A duplicate/stale
   release is currently silently accepted.
6. **Writer arithmetic needs a deliberate wrap policy.** Avoid unchecked signed
   casts/subtraction for sequence comparisons and define how logical `u64`
   sequence wrap is handled. The prototype does not need to solve practical
   wraparound now, but tests must not rely on undefined arithmetic.

### Pub/sub and lifecycle

1. **Cursor starts at zero.** A new agent must start at a snapshot of the current
   write cursor before its subscription bit becomes visible. Starting at zero
   scans stale/lapped generations.
2. **Self-publication leaks a bit.** `Broker::publish()` builds a mask containing
   the publisher if it subscribed, but skips waking that publisher. Its bit can
   keep the slot unavailable forever. Build the recipient mask as
   `topic_mask & !publisher_bit` and reject zero recipients.
3. **Delivery tests mutable subscriptions.** `Broker::read()` currently checks
   `details.subscriptions.contains(topic)`. It must instead test the immutable
   slot recipient bit. A later subscribe must not receive old records, and a
   later unsubscribe must not strand an already-owned record.
4. **No dynamic topic registry.** `MessageTopic` is a static Rust enum, while the
   target uses service-allocated IDs `0..255`, immutable descriptors, and a topic
   generation for safe ID reuse.
5. **Reader bits are never recycled.** `Registry::unregister()` leaves
   `deallocate_bit()` unused. A bit can be reused only after it has been removed
   from future masks and no outstanding slot retains it.
6. **Unsubscribe policy is incomplete.** An unsubscribe must define what happens
   to already-owned records: normally continue draining/releasing them, possibly
   without dispatching them to the application. It cannot simply stop reading.

### Architecture still to model

1. The current broker owns the ring and performs the publication call. The final
   topology is direct agent-to-ring data access, then a broker ingress wakeup.
2. `event_listener::Event` is an in-process stand-in. Android needs an ingress
   eventfd for the service-side broker plus one `data_eventfd` and one
   `journal_eventfd` per agent. Catalog and lifecycle operations remain Binder
   calls.
3. Shared-memory layout, `memfd` attach, fixed-width ABI fields, catalog Binder
   calls, AiSeal/Keystore, C++ FFI, and Binder death recipients do not exist yet.

## Step-by-Step Execution Plan

Work in this order. Each step fixes a coherent class of problem and has an exit
test gate before the next feature makes the state machine more complex.

### Step 0 — Audit, freeze, and test the current behavior

**Goal:** make the existing code's failures explicit before changing its algorithm.
This step does not add chunks, shared memory, or Android behavior. It produces a
baseline test harness, turns hidden bugs into named regression tests, and prevents
later refactoring from accidentally changing what was observed.

#### Step 0A — Source-level bug ledger

The table below is deliberately tied to the current files. A fix must delete or
rewrite the cited code and add the corresponding test; do not merely add a comment
or catch the resulting generic error in `Broker::read()`.

| ID | Current source | Problem | Consequence | Fixed in | Required regression test |
|---|---|---|---|---|---|
| RB-01 | `buffer.rs:103` | `try_read()` returns `&Message` from `UnsafeCell`. | Borrow can outlive safe slot ownership and observe later writer mutation. | Step 1 | Hold a read result while another reader releases; API must make this impossible. |
| RB-02 | `buffer.rs:126` | `update_after_read()` accepts any sequence and bit; it never checks current generation or prior ownership. | Duplicate/non-owner release can report success and may release a slot incorrectly. | Step 1 | Double release and non-recipient release return explicit errors. |
| RB-03 | `buffer.rs:110` | Sequence-ahead state becomes `SlotAccessDenied`. | Readers that skipped unowned traffic fail instead of fast-forwarding. | Step 2 | Slow unrelated reader fast-forwards and still receives its next addressed record. |
| RB-04 | `buffer.rs:61` | Direct `u64 -> i64` sequence subtraction has no documented wrap rule. | Long-running behavior and debug overflow assumptions are unclear. | Step 1 | Test sequence comparison helper near chosen artificial boundary. |
| RB-05 | `buffer.rs:68` | `current_head + 1` has no checked/wrapping policy. | Logical sequence wrap is undefined by the implementation. | Step 1 | Reserve helper has defined checked/wrapping behavior. |
| RB-06 | `buffer.rs:54` | Write claims exactly one position only. | No atomic contiguous multi-slot reservation or chunk commit exists. | Step 4 | Two writers reserve ranges that never interleave. |
| RB-07 | `buffer.rs:8-14` | Slot has no `state` or active-reader tracking. | Journal spill cannot stop new physical copies or wait for copies in progress. | Step 4/5 | Reader-versus-eviction interleaving model test. |
| RB-08 | `buffer.rs:13` | `UnsafeCell` means `RingBuffer` is not automatically `Sync`. | The crate has not proven true cross-thread MPMC use; existing tests are single-threaded. | Step 1 | Threaded test compiles only after documented safety invariants are enforced. |
| BR-01 | `broker.rs:70-82` | Publisher is included in the topic mask but skipped for notification. | A self-subscribed publisher can leave its bit pending forever. | Step 2 | Self-publisher is absent from stored recipient mask. |
| BR-02 | `broker.rs:107` | Delivery follows the current `subscriptions` set, not slot ownership snapshot. | Late subscribe can receive history; unsubscribe can strand an old owned bit. | Step 2 | Late subscribe and unsubscribe-with-pending-record tests. |
| BR-03 | `broker.rs:58,93` | Registry mutex is held through publish/read ring work and the read scan loop. | Host code serializes control work with data work; it cannot represent final fast path. | Step 2/6 | Lock-scope test/instrumentation proves no registry lock during slot copy. |
| BR-04 | `broker.rs:121` | All ring conditions collapse to `RegistryError::ReadError`. | Caller cannot distinguish empty, partial, stale, or corrupted state. | Step 1 | Error matrix has one assertion per public read state. |
| RG-01 | `subscribe.rs:18` | Every new cursor starts at zero. | A late registration scans stale generations and immediately fails. | Step 2 | Register after N writes starts at current head and receives only future data. |
| RG-02 | `subscribe.rs:178-200` | Unregister removes the agent but never drains or frees its reader bit. | Bits leak permanently; immediate reuse would be unsafe. | Step 2 | Graceful unregister drains, verifies, then recycles exactly one bit. |
| RG-03 | `subscribe.rs:29,120` | Topic lookup is an enum-keyed `Vec<Uuid>` scan. | Cannot model 256 dynamic topics or O(1) mask snapshots. | Step 3 | Dynamic topic allocation and direct mask snapshot tests. |
| RG-04 | `subscribe.rs:138-141` | Notification uses `unwrap()` and has no unreachable/dead agent state. | Lifecycle failures panic instead of being reported/cleaned. | Step 6 | Removed/dead target cannot panic notifier. |
| MSG-01 | `message.rs:4,8,25` | 300-byte payload, static enum, caller-provided message ID, and `usize` length. | Does not model 4 KiB slots, dynamic topics, ring-assigned ID, chunks, or final ABI. | Step 3/4 | Metadata construction follows reservation sequence and topic generation. |
| TEST-01 | `message.rs:90` | Seven tests exercise constructors only. | No current test detects any queue, fan-out, cursor, or notification bug above. | Step 0 | Integration tests fail on each intentionally injected ledger bug. |

**Important:** the ledger does not mean every current line is wrong. For example,
the producer's release store of sequence is the right publication boundary concept.
The task is to preserve valid ideas while making the surrounding ownership and
state-machine rules explicit.

#### Step 0B — Establish a baseline test harness

1. Convert the crate into a library plus optional demonstration binary. Tests must
   import the public API instead of reaching into private slots.
2. Introduce a capacity parameter or `const generic` test constructor. Production
   stays at 500; deterministic tests use 4, 5, 8, and 16 slots.
3. Add `TestAgentId`, `TestTopicId`, deterministic payload builders, and a
   `drain_until_blocked()` helper. Never depend on random UUID ordering in a unit
   test.
4. Add a reference model using `VecDeque<ModelRecord>`, recipient masks, and
   per-agent cursors. The model need not be lock-free; it defines expected logical
   delivery for short randomized traces.
5. Standardize diagnostic context in every assertion: logical sequence, physical
   slot index, expected/observed sequence, state, pending mask, active mask,
   publisher slot, reader slot, and cursor.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRecord {
    sequence: u64,
    topic: u8,
    recipients: u64,
    body: Vec<u8>,
}

impl ModelBus {
    fn publish(&mut self, topic: u8, recipients: u64, body: Vec<u8>) -> Result<u64, ModelError> {
        if recipients == 0 { return Err(ModelError::NoSubscriber); }
        if self.live.len() == self.capacity { return Err(ModelError::Full); }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.live.push_back(ModelRecord { sequence, topic, recipients, body });
        Ok(sequence)
    }
}
```

The model deliberately omits atomics and physical reuse. It answers *who should
receive which logical sequence*. The implementation tests then compare observed
owned `ReadRecord`s against that result.

#### Step 0C — Baseline tests and acceptance gate

| Test | Setup | Assertion |
|---|---|---|
| `message_constructor_baseline` | Existing message tests | Preserve constructor behavior until message API migration is deliberate. |
| `single_slot_current_round_trip` | One producer, one recipient, empty ring | Existing prototype can write/read/clear one record. |
| `current_self_publish_reproduces_leak` | Publisher is also subscribed | Documents the existing stuck-bit behavior, then becomes a fixed regression in Step 2. |
| `late_registration_starts_at_zero` | Write before register | Documents current failure/incorrect cursor state, then flips expectation in Step 2. |
| `unowned_cursor_is_slot_access_denied` | Advance ring with unrelated records | Documents current sequence-ahead error, then flips to fast-forward in Step 2. |
| `unregister_leaks_bit` | Register/unregister 64 agents | Documents exhaustion/leak, then flips to safe reuse in Step 2. |

It is acceptable for explicitly named **current-behavior** tests to demonstrate a
known defect temporarily. Mark them `#[ignore]` or isolate them from the default
green suite after they establish the reproduction. The normal required suite must
be green; a bug reproduction belongs in the issue/commit history once its fixed
regression test replaces it.

**Step 0 exit gate:** the team can run `cargo test --test current_behavior`,
`cargo test --test ring_single_slot`, and a short reference-model trace. Every
known defect has an ID, source location, expected fix step, and a future permanent
test name. No algorithm rewrite starts before this baseline exists.

### Step 1 — Make the existing single-slot transport memory-safe

**Goal:** preserve the current single-slot behavior without returning a reusable
slot reference to caller code.

- Replace `try_read(&AtomicU64) -> &Message` with an owned result such as
  `try_read_copy(sequence, reader_bit) -> Result<ReadRecord, ReadError>`.
- Validate the slot sequence and the caller's reader bit before copying. Copy the
  `Message` into local owned storage while the bit is still pending.
- Make `release_read(sequence, reader_bit)` validate both the expected generation
  and prior ownership. A non-owner or duplicate release is an error, never a
  silent success.
- Keep the existing sequence-based producer claim, but make error states explicit:
  `BufferFull`, `WouldBlock`, `NotOwner`, `StaleSequence`, and `CorruptState`.

```rust
// Host-prototype shape: clone/copy while the slot cannot be reused.
let old_mask = slot.pending_readers.load(Ordering::Acquire);
if old_mask & reader_bit == 0 {
    return Err(ReadError::NotOwner);
}
let message = unsafe { (*slot.value.get()).clone() };
// Caller receives `message`; release happens only after this copy.
```

**Tests:** two-reader fan-out; reader one cannot free a slot for reader two;
duplicate release; non-recipient release; producer cannot overwrite until the
final release. Run this step under Miri where applicable and ThreadSanitizer where
available; do not add `unsafe impl Send/Sync` merely to make tests compile without
first writing the ownership proof.

### Step 2 — Correct cursor, recipient, and lifecycle semantics

**Goal:** make the in-process broker follow the final delivery contract before
adding new message features.

- Initialize a newly registered cursor from an acquire snapshot of `head`, then
  expose its reader bit in subscription masks. A new agent never scans history.
- Derive `recipient_mask = subscribers(topic) & !publisher_bit`. Reject zero
  recipients without reserving a slot.
- Decide delivery from the immutable slot mask, not mutable current subscriptions.
  An unsubscribe changes future masks only; it must still release already-owned
  records, even if the application no longer wants callbacks for them.
- Implement normal sequence-ahead fast-forward. A reader that did not own old
  records may advance to the current generation; a targeted record could not have
  been reused while its bit remained set.
- Add graceful unregister: remove future topic bits, capture a cutoff, drain every
  owned record through it, prove no slot retains the bit, then recycle the bit.

**Tests:** registration after writes; late subscribe; self-subscription publish;
empty topic; unsubscribing with an old owned record; cursor fast-forward through
unrelated traffic; all 64 reader bits plus safe reuse of one drained bit.

### Step 3 — Add dynamic topics and catalog generations

**Goal:** remove the static `MessageTopic` enum while retaining a bounded shared
index suitable for Android.

- Model `TopicId(u8)` and a local `TopicDescriptor` with name, immutable schema,
  codec, visibility, and `u32` generation.
- Change the registry from `HashMap<MessageTopic, Vec<Uuid>>` to an atomically
  readable 256-entry topic-mask model plus service-local descriptor metadata.
- Implement Broadcast versus Unlisted catalog changes in the host model. Broadcast
  increments a catalog generation and sends a control notification; Unlisted does
  not.
- Preserve old topic descriptor/key material until no old-generation slots remain;
  never interpret a reused `u8` ID without checking generation.

**Tests:** allocate 256 distinct IDs; reject the 257th; generation increases on
reuse; broadcast versus unlisted notification; an old-generation record is never
decoded with a new topic descriptor.

### Step 4 — Add all-or-nothing range reservation and chunking

**Goal:** support logical messages larger than 4 KiB without interleaving or
partial delivery.

- Replace one-slot claim with `reserve_range(chunk_count)`. It validates all
  candidate generations before one CAS advances `head` by the range length.
- Store shared message metadata in each chunk: first sequence/message ID, sender,
  topic ID/generation, recipient mask, index, and count.
- Introduce an explicit reservation state. Every physical slot is `WRITING` until
  payload bytes are present; one release commit makes the full range `READY`.
- Treat a physical end-to-start wrap as contiguous logical sequence space.
- Reassemble an owned payload before clearing the reader bit from any chunk.

```text
start = reserve_range(chunk_count)?
for (index, bytes) in chunks(payload):
    write_chunk(start + index, metadata(index, chunk_count), bytes)
commit_range(start, chunk_count)       // one visibility point
```

**Tests:** 1-, 2-, and 255-chunk messages; concurrent writers cannot interleave;
wrap from slot 499 to slot 0; failed reservation makes no partial record visible;
reader sees `WouldBlock` before commit and one assembled message after it.

### Step 5 — Add the pressure safeguard and overflow journal

**Goal:** drain lagging readers first, then preserve delivery in a journal before
freeing live-ring slots under critical pressure.

The safeguard is a service-side pressure loop, not a per-slot timer. It inspects
the oldest unreleased region and uses the reader mask for the purpose it was added
for: finding exactly which agents still need each record.

1. **Above 80% occupancy:** read pending masks for blocked records and send a final
   targeted data notification to only those readers. This is a drain request, not
   an ownership revoke. A responsive reader releases normally and no journal I/O is
   needed.
2. **Above 90% occupancy:** select complete logical messages for journal spill.
   Atomically claim `READY -> EVICTING`, write the entire message (all chunks) to
   the journal, wait for active physical readers to finish, then mark the ring
   range `JOURNALED -> FREE`. Writers may now reuse the physical slots while the
   journal retains the pending delivery obligation.
3. **Below a lower recovery watermark:** leave critical mode only after occupancy
   drops sufficiently; the exact hysteresis value is a later tuning decision.

Journal spill is not an untracked overwrite. Each journal entry needs ticket or
sequence range/message ID, timestamp, sender, topic ID and generation, recipient
mask, chunk metadata, payload bytes, and integrity metadata. The journal is an
emergency delivery tier: the safeguard signals a separate journal-only event after
commit, and `read_journal()` drains all records still owned by that agent. The live
ring cursor never probes or advances through the journal. Recovery delivery can
follow newer live delivery; applications use timestamp/message ID where chronology
matters.

`pending_readers` is insufficient to free an evicted slot safely because it cannot
distinguish a reader that has not started from a reader currently copying bytes.
Add `active_readers` (or an equivalent active-copy protocol) before journal spill:

```text
reader: claim active bit -> copy physical bytes -> clear active bit -> clear pending bit
monitor: READY -> EVICTING -> journal complete record -> wait active mask == 0
         -> JOURNALED -> FREE
```

The monitor must prevent new active claims after `EVICTING` begins. A reader that
loses the claim does not touch physical bytes; the recovery record is later exposed
through `read_journal()`.

```text
if occupancy > PRESSURE_WATERMARK:     // 80%
    notifier.signal_readers(oldest_unreleased.pending_readers)
if occupancy > CRITICAL_WATERMARK:     // 90%
    spill_complete_messages_to_journal_until_recovered()
if reserve_range(n) == BufferFull:
    return BufferFull                  // journal full/failed or pressure unresolved
```

The Android safeguard additionally has two process-failure paths:

1. **Dead reader:** Binder death proves the process is gone. The service removes it
   from future masks, clears/revokes its outstanding ownership safely, wakes
   blocked writers, and recycles the bit only after cleanup.
2. **Dead writer during `WRITING`:** the reservation records publisher slot and
   generation. A Binder death recipient lets the service atomically transition the
   abandoned reservation to `ABORTED`/ordered skip so later records do not remain
   permanently blocked. A live stalled writer is not timeout-aborted in V1.

**Tests:** 79% occupancy causes no pressure action; crossing 80% signals only set
reader bits; a responsive reader drains and returns below recovery watermark;
crossing 90% journals complete messages before their physical slots become free;
no reader sees mutated physical payload; chunked spill/replay is all-or-nothing;
journal replay/live deduplication yields one delivery; journal failure leaves the
slot non-reusable and causes backpressure. Host model covers dead-reader cleanup;
Android service integration later covers dead-writer `WRITING -> ABORTED` ordering.

### Step 6 — Model the service-side broker and eventfd roles

**Goal:** match the final notification topology without placing a Binder call on
the data path.

- Publishers write a complete record/range directly and notify one host ingress
  event. The broker holds a notification cursor, not a delivery cursor.
- The broker only scans fully committed records, resolves the immutable recipient
  mask, and signals recipients. It never copies payloads or clears reader bits.
- Model separate live-data and journal-recovery notifications. Always signal
  addressed data; do not add `wait_count`, polling, or signal elision. Catalog and
  lifecycle operations remain Binder calls.

**Tests:** a commit causes one ingress event; only recipients wake; publisher does
not wake itself; multiple publications coalesce yet a drain loop receives all
records; journal events never masquerade as live-data events.

### Step 7 — Move the proven model to Android

**Goal:** preserve the host semantics across process boundaries instead of
redesigning them during integration.

- Freeze the fixed-width `#[repr(C)]` shared-memory ABI and map it with
  `memfd`/`ASharedMemory`.
- Implement vendor AIDL registration, fd handoff, agent/eventfd lifecycle, dynamic
  topic catalog, SELinux policy, and Binder death recipients.
- Add Protobuf/bincode codecs, AiSeal-backed `TopicKeyStore`, local AES-GCM, C FFI,
  and the C++ `AgentBus` wrapper after the core transport tests pass unchanged.

**Tests:** two-process host `memfd` test; device registration/fd-handoff test;
eventfd fan-out test; Binder death cleanup; C++ publisher to Rust subscriber and
the reverse; encrypted Protobuf round trip on an AiSeal-enabled device.

## Mapping to the Binderized Android Version

| Current host component | Android counterpart | Migration rule |
|---|---|---|
| `RingBuffer` | `SharedRingBuffer` over `memfd` / `ASharedMemory` | Keep sequence, masks, and reservation semantics; replace heap storage only. |
| `Slot` + `Message` | Fixed-width `#[repr(C)]` slot ABI | Remove `usize`, Rust-only enums, pointers, and `UnsafeCell` from the wire layout. |
| `Agent` | Rust/C++ `AgentBus` client | Owns mapped regions, assigned agent slot, cursor, key store, and data/journal eventfd reads. |
| Current `Broker` | Split into client ring helper and service-side notifier broker | Client writes/reads ring directly; service broker only observes commit and fans out wakeups. |
| `Registry` | Service `AgentRegistry` + `TopicCatalog`; shared `TopicIndex` | Service owns metadata/lifecycle; atomics expose fast subscription masks/cursors. |
| UUID | Service-assigned `u8` agent slot plus diagnostic name/UUID | Slot is sender ID and reader-mask bit; name/UUID never drive the fast path. |
| `MessageTopic` enum | Dynamic `TopicId(u8)` + `TopicDescriptor` generation | Service allocates IDs; public creation emits a `TopicCreated` ring record, while catalog queries use Binder. |
| `event_listener::Event` | Linux eventfd + epoll | One broker ingress event, plus data/journal eventfds per agent. |
| `Mutex<Registry>` | Service control-plane synchronization | Keep locks out of agent ring reads/writes; use atomics in shared memory. |
| Host pressure monitor | Vendor service safeguard + overflow journal | Target pending readers >80%; journal complete records >90%; Binder death handles dead readers/writers. |

## Target Host API and Error Contract

Do not let `Broker::read()` return one generic `RegistryError::ReadError` for every
condition. A caller and a test need to know whether to retry, drain, replay, or
escalate. The exact Rust names may change, but the behavioral categories should
survive all implementation phases.

### Ring API

```rust
pub struct Reservation {
    pub first_sequence: u64,
    pub chunk_count: u8,
    pub recipient_mask: ReaderMask,
}

pub struct ReadRecord {
    pub first_sequence: u64,
    pub message: Message,       // owned bytes, never references a slot
    pub source: ReadSource,     // LiveRing | JournalReplay
}

pub enum ReadOutcome {
    Record(ReadRecord),
    WouldBlock,
    JournalGap(JournalGap),
    FastForwarded { from: u64, to: u64 },
}

pub enum PublishError {
    NoSubscriber,
    Backpressure,
    PayloadTooLarge,
    InvalidTopic,
    TopicGenerationMismatch,
    JournalUnavailable,
    CorruptState,
}

impl RingBuffer {
    pub fn reserve_range(
        &self,
        chunk_count: NonZeroU8,
        recipients: ReaderMask,
    ) -> Result<Reservation, PublishError>;

    pub fn write_chunk(&self, reservation: &Reservation, chunk_index: u8,
                       message: &Message) -> Result<(), PublishError>;

    pub fn commit(&self, reservation: Reservation) -> Result<(), PublishError>;

    pub fn read_next(&self, cursor: &mut ReadCursor, reader: ReaderId)
        -> Result<ReadOutcome, ReadError>;
}
```

`FastForwarded` may be internal to `read_next()` rather than exposed publicly; the
important rule is that it is a normal condition, not the old `SlotAccessDenied`
failure. `JournalGap` must carry the exact logical sequence range and journal
lookup key so the agent does not guess what to replay.

### Registry and broker API

```rust
pub trait TopicRegistry {
    fn register_agent(&mut self, name: &str, cursor_start: u64) -> Result<AgentLease, RegistryError>;
    fn create_topic(&mut self, descriptor: TopicDescriptor) -> Result<TopicHandle, RegistryError>;
    fn subscribe(&mut self, agent: AgentId, topic: TopicHandle) -> Result<(), RegistryError>;
    fn recipients_for(&self, topic: TopicHandle, publisher: ReaderMask) -> ReaderMask;
    fn begin_unregister(&mut self, agent: AgentId, cutoff: u64) -> Result<DrainPlan, RegistryError>;
    fn complete_unregister(&mut self, plan: DrainPlan) -> Result<(), RegistryError>;
}

pub trait Notifier {
    fn broker_ingress(&self) -> io::Result<()>;
    fn signal_data_mask(&self, readers: ReaderMask) -> io::Result<()>;
    fn signal_control_mask(&self, readers: ReaderMask) -> io::Result<()>;
}

pub trait Journal {
    fn append_complete(&self, record: JournalRecord) -> Result<JournalLocation, JournalError>;
    fn load(&self, gap: JournalGap) -> Result<JournalRecord, JournalError>;
    fn release_reader(&self, location: JournalLocation, reader: ReaderId)
        -> Result<JournalRelease, JournalError>;
}
```

The host versions can use in-memory stores and `event_listener`; the interfaces
make it possible to replace them with file-backed journal/eventfd/Binder adapters
without changing ring correctness tests.

### Error disposition table

| Error / outcome | Publisher action | Reader action | Metric severity |
|---|---|---|---|
| `NoSubscriber` | Return to caller; do not reserve | N/A | info/debug |
| `Backpressure` | Retry only after progress/wakeup; apply caller policy | Drain normally | warning with occupancy/mask |
| `WouldBlock` | N/A | Sleep/wait; do not advance cursor | normal |
| Fast-forward | N/A | Advance to reported live generation and retry | debug counter |
| `JournalGap` | N/A | Replay exact gap before continuing | info counter |
| Journal failure | Stop safe reclamation; propagate pressure | Surface unavailable delivery state | error |
| State/sequence corruption | Fail closed; do not reuse slot | Fail closed; do not skip | fatal/error |
| Topic generation mismatch | Refresh catalog or reject publish | Refresh catalog; do not decode with wrong schema | error |

## Observability, Metrics, and Debug Dumps

Lock-free errors are notoriously difficult to reproduce after the fact. The host
prototype should add a test-only snapshot API before performance optimization. It
does not need to be wait-free; it is used after a failure or from the pressure
monitor.

```rust
pub struct RingSnapshot {
    pub head: u64,
    pub occupancy_estimate: usize,
    pub slots: Vec<SlotSnapshot>,
}

pub struct SlotSnapshot {
    pub physical_index: usize,
    pub seqno: u64,
    pub state: SlotState,
    pub pending_readers: u64,
    pub active_readers: u64,
    pub first_sequence: Option<u64>,
    pub topic_id: Option<u8>,
    pub chunk_index: Option<u8>,
    pub chunk_count: Option<u8>,
}
```

Record at least these counters, tagged by topic and agent only where bounded and
safe to do so:

- publishes accepted, rejected for no-subscriber, and rejected for backpressure;
- reservations attempted, committed, aborted, and range-reservation retries;
- live reads, fast-forwards, journal gaps, successful journal replays, and replay
  deduplications;
- pressure entries above 80%, critical entries above 90%, targeted wake masks, and
  recovery transitions;
- journal append bytes, append failures, current retained entries, and entries
  garbage-collected after their final reader;
- unregister drain latency, dead-reader cleanup count, and dead-writer recovery
  count once Android work begins.

Every timeout/panic in a stress test should include `RingSnapshot`, each agent's
cursor and reader bit, topic masks, journal index state, and the last 32 logical
operations. That diagnostic data is more valuable than simply increasing the test
timeout.

## Test Execution Matrix

The commands below become the minimum local/CI matrix as the corresponding tests
are added. Do not enable a tool merely because it is fashionable; each is tied to
a distinct failure class.

| Command / tool | Scope | Finds |
|---|---|---|
| `cargo test` | deterministic unit/integration suite | basic API, mask, cursor, chunk, registry regressions |
| `cargo test --release` | same tests with optimized atomics | debug-only arithmetic assumptions and timing-sensitive behavior |
| `cargo test -- --ignored` | bounded stress suite | throughput regressions, livelock, missed/repeated delivery |
| `cargo test --test mpmc_stress -- --ignored` | full-scale 80k-message scenario | fan-out loss/duplicate and pressure-journal handoff |
| `cargo test --features loom` | tiny capacities/models only | atomic interleavings that stress tests rarely hit |
| ThreadSanitizer build/run | threaded host tests | unprotected non-atomic slot access and host data races |
| Miri (small tests only) | unsafe owned-copy/slot paths | invalid references and UB in non-concurrent cases |
| `cargo fmt --check` / `cargo clippy` | all code | API mistakes and accidental unsafe/style regressions |

Suggested dev-only dependencies, introduced one at a time with a specific test:

```toml
[dev-dependencies]
loom = "0.7"        # reduced atomic state-space models
proptest = "1"       # generated publish/subscribe operation traces
tempfile = "3"       # journal failure/rotation fixtures
```

`proptest` traces should be replayable. When a generated sequence fails, print or
persist the seed plus the operation list so it becomes a deterministic regression
test rather than a one-off random failure.

## Per-Module Implementation Specifications

This section translates the staged plan into concrete file-level work. It is more
specific than a directory listing so an implementation session can stop after any
module and still leave a coherent, testable repository.

### `src/message.rs` — logical message and opaque payload

**Current role:** static `MessageTopic`, 300-byte inline payload, caller-supplied
message ID, and `usize` length.

**Target host role:** owned logical message value. It should contain no atomic
ownership state; ownership belongs to `Slot`/reservation code. The host version may
continue to use Rust-friendly values temporarily, but it must expose the fields the
final ABI will need.

```rust
pub const HOST_SLOT_PAYLOAD_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TopicId(pub u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopicGeneration(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkInfo {
    pub index: u8,
    pub count: NonZeroU8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub message_id: u64,        // filled by reservation, not caller
    pub sender_id: u8,          // registry-assigned slot
    pub topic: TopicId,
    pub topic_generation: TopicGeneration,
    pub flags: MessageFlags,
    pub chunk: ChunkInfo,
    pub payload_len: usize,     // host only; final ABI uses u32
    pub payload: [u8; HOST_SLOT_PAYLOAD_BYTES],
}
```

**Implementation rules:**

1. Provide a constructor for an *unassigned publish request* and a separate
   `with_reservation_metadata()` internal method. An agent must not choose
   `message_id` or `sender_id`.
2. Make empty payload policy explicit. Sensor values can legitimately encode to
   zero bytes in some future schemas; avoid treating empty bytes as automatically
   invalid unless the public API requires that restriction.
3. Validate `chunk.index < chunk.count` at construction and reassembly.
4. Keep payload bytes opaque. Do not add Protobuf/bincode dependencies during ring
   correctness work.
5. Add `Message::patterned_for_test()` that produces deterministic 4 KiB data from
   sequence/sender/topic. It catches torn/cross-slot copies better than a short
   integer payload.

**Tests:** field validation; patterned payload round trip; chunk metadata invalid
cases; topic generation equality; message ID only assigned by reservation path.

### `src/ring/slot.rs` — physical ownership state

**Current role:** `seqno`, `UnsafeCell<Message>`, and one pending mask are defined
inline in `buffer.rs`.

**Target role:** keep all physical state and its documented transitions together.

```rust
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    Free = 0,
    Writing = 1,
    Ready = 2,
    Evicting = 3,
    Journaled = 4,
    Aborted = 5,
}

pub struct Slot {
    seqno: AtomicU64,
    state: AtomicU8,
    pending_readers: AtomicU64,
    active_readers: AtomicU64,
    value: UnsafeCell<Message>,
}
```

**Allowed state transitions:**

```text
FREE -> WRITING       producer has uniquely reserved this generation
WRITING -> READY      complete single/range commit release-published
READY -> EVICTING     pressure monitor won CAS; no new active claims
EVICTING -> JOURNALED complete journal record exists and old active copies ended
JOURNALED -> FREE     physical bytes may be reused; journal owns pending delivery
READY -> FREE         normal final-reader release
WRITING -> ABORTED    later Android dead-writer recovery only
ABORTED -> FREE       ordered skip is visible and reclaim conditions hold
```

Never permit `READY -> FREE` directly from a monitor. Only final reader release or
a successful journal handoff may make the physical slot reusable.

**Methods to implement:**

- `expected_free(sequence) -> u64` and `expected_ready(sequence) -> u64` helpers;
- `try_claim_writer(sequence)` returning a generation-specific writer guard;
- `try_claim_active(sequence, reader_bit)` that fails if state is not READY or the
  reader bit is absent/eviction won;
- `copy_owned_message(active_guard)` returning an owned `Message`;
- `release_reader(sequence, reader_bit)` returning whether it was the final live
  reader;
- `begin_evict(sequence)` and `finish_journal(sequence, location)` used only by
  the pressure monitor.

**Tests:** state transition matrix; invalid transition rejects; active claim versus
evict CAS race; final live reader versus journal handoff race; `JOURNALED` slot
cannot be physically read by a new active claim.

### `src/ring/reserve.rs` — write cursor and range reservation

**Current role:** `RingBuffer::try_write()` loads head, compares slot sequence, and
CASes one position.

**Target role:** reserve one or more logical positions without exposing partially
claimed ranges.

```rust
fn reserve_range(&self, count: NonZeroU8) -> Result<Reservation, PublishError> {
    loop {
        let start = self.head.load(Ordering::Acquire);
        self.ensure_range_is_free(start, count)?; // reads every target generation
        let end = start.checked_add(count.get() as u64).ok_or(PublishError::SequenceExhausted)?;
        if self.head.compare_exchange_weak(
            start, end, Ordering::AcqRel, Ordering::Acquire,
        ).is_ok() {
            return Ok(Reservation::new(start, count));
        }
    }
}
```

`ensure_range_is_free()` must validate the exact expected free sequence for every
physical slot in the logical range, including wrap. If any is unavailable, it must
not alter head or any slot. This means a large message can receive `Backpressure`
even when some individual slots are free; contiguity is part of the contract.

**Producer failure boundary:** after a successful range reservation, only the
reserving producer may write/commit the range. The host prototype records enough
metadata to inspect this; Android later records publisher slot/generation for
dead-writer recovery. Do not try to “undo” head with a decrement CAS—later writers
may already have reserved subsequent ranges.

**Tests:** reservation contention; count 1/2/255; wrap; partial-free rejection;
later writer reservation behind an earlier WRITING range; head never moves on a
failed range probe.

### `src/ring/read.rs` — cursor advance, copy, release, replay

**Current role:** `Broker::read()` loops over `try_read()`, checks current
subscription, clones, then calls `update_after_read()`.

**Target role:** one explicit algorithm that owns all cursor movement and returns
typed outcomes. The broker/service must not duplicate this logic.

```text
loop:
  expected = cursor.sequence
  slot = physical(expected)
  observed = slot.load_sequence(Acquire)

  if observed names a newer logical generation:
      cursor.fast_forward_to(observed)
      continue
  if observed is not expected READY:
      return WouldBlock
  if slot is JOURNALED or a journal-gap marker:
      return JournalGap(slot.journal_location)
  if !slot.pending_contains(reader.bit):
      cursor.advance_one_or_range()
      continue

  active = slot.try_claim_active(expected, reader.bit)?
  record = active.copy_owned()
  active.finish_copy()
  slot.release_reader(expected, reader.bit)?
  cursor.advance_after(record)
  return Record(record)
```

The same-agent concurrent-read decision is open. Until decided, make `ReadCursor`
non-cloneable and store it behind a single drain-owner guard. A public atomic cursor
does not make two `read_next()` calls semantically safe: they could copy or release
the same logical record without an additional per-agent claim.

**Tests:** all outcome variants; cursor moves exactly once; reader bit is unchanged
on copy failure; journal gap does not skip the sequence; unowned record fast-forward
does not decrement any mask.

### `src/registry/mod.rs` — agent slots and subscriptions

**Current role:** UUID maps, `HashSet<MessageTopic>`, topic `Vec<Uuid>`, and an
allocated-bit word under a process-local mutex.

**Target role:** service-style registry model with stable control-plane identities
and hot-path masks.

```rust
pub struct AgentLease {
    pub diagnostic_id: Uuid,
    pub slot: u8,
    pub bit: ReaderMask,
    pub cursor_start: u64,
    pub registration_generation: u32,
}

pub struct Registry {
    agents: HashMap<Uuid, AgentDetails>,
    topic_masks: [AtomicU64; 256],
    allocated_agents: u64,
    catalog: TopicCatalog,
}
```

The host implementation may keep a lock around maps/catalog updates, but publication
must obtain a recipient mask without allocating a `Vec` or holding that lock over
ring copies. On Android the service updates the shared topic mask atomically and
clients load it directly.

**Unregister algorithm:**

```text
lock control plane
  remove agent bit from all future topic masks
  capture cutoff = current write cursor
  mark lease DRAINING
unlock

agent drains/replays every owned record through cutoff
service verifies no live slot or journal entry retains bit
lock control plane
  free bit and diagnostic mapping; increment lease generation
unlock
```

**Tests:** allocation order; 65th failure; no duplicate masks; subscription
idempotency/error policy; future-only unsubscribe; drain cutoff; attempted early
reuse rejection; bit reuse after both live and journal ownership have cleared.

### `src/broker/` — notification and pressure ownership

**Current role:** in-process broker owns ring and registry and synchronously notifies
inside `publish()`.

**Target role:** split responsibilities:

- `RingClient` is embedded in AgentBus and performs direct reservation/write/read.
- `ServiceBroker` owns ingress notifications, a notification cursor, per-agent
  data/control handles, and pressure monitor scheduling.
- `PressureMonitor` consumes metrics/snapshots, executes 80% targeted wakeups and
  90% journal transactions, and never mutates an unread payload without a successful
  handoff.

The broker's notification cursor advances after it sees a committed logical message
and has queued/sent recipient signals. It is not one of the recipient cursors and
does not participate in `pending_readers`.

**Tests:** broker cannot notify a `WRITING` range; notification mask equals stored
recipient mask; data and control events stay separate; pressure monitor's targeted
mask equals oldest blocked mask; journal transition wakes readers that must replay.

### `src/journal.rs` — host implementation before Android storage

Start with an in-memory journal behind the `Journal` trait. It permits deterministic
failure injection and exact inspection of handoff state. Add file-backed storage
only after the handoff protocol and tests are green.

Required test hooks:

- fail next append before writing bytes;
- fail after bytes but before commit/index publication;
- delay append while a reader owns an active claim;
- expose entry count, retained masks, and sequence-to-location index;
- inject an integrity mismatch to prove reader failure is surfaced.

File-backed implementation later needs segment/header versioning, encryption at
rest using the same topic security context, bounded capacity/rotation policy, and
crash-recovery scanning. Those are not reasons to postpone the in-memory handoff
tests.

## Required Invariants

The implementation should document these next to the core types and test them.

1. A producer writes payload/header bytes before release-publishing readiness.
2. A reader acquire-observes readiness, copies bytes into an owned value, then
   clears only its own bit.
3. A producer reuses a slot only after its sequence says the expected generation
   is free; a nonzero pending mask makes it unavailable.
4. A recipient bit is a snapshot at publication time. Subscription changes affect
   future publications only.
5. The publisher bit is never in a message recipient mask.
6. A zero recipient mask reserves no slot.
7. Every committed chunk range is all-or-nothing visible; notification occurs once
   after the complete range commits.
8. A sequence-ahead read is safe to fast-forward only because any skipped record
   that targeted this reader could not have been reclaimed.
9. A normal reader must never expose a reference into a reusable slot.

## Suggested Core Shapes

These are sketches, not drop-in code. Keep the existing fixed `RingBuffer` rather
than restoring a generic shared transport.

```rust
const BUFFER_SIZE: usize = 500;
const MAX_PAYLOAD: usize = 4 * 1024;
pub type ReaderMask = u64;

#[repr(u8)]
enum SlotState { Free, Writing, Ready, Aborted }

struct Message {
    message_id: u64,       // first reserved logical sequence
    sender_id: u8,
    topic_id: u8,
    topic_generation: u32,
    flags: u8,
    chunk_index: u8,
    chunk_count: u8,
    payload_len: usize,    // prototype only; final ABI becomes fixed-width
    payload: [u8; MAX_PAYLOAD],
}

struct Slot {
    seqno: AtomicU64,
    state: AtomicU8,
    pending_readers: AtomicU64,
    value: UnsafeCell<Message>,
}

struct ReadRecord {
    sequence: u64,
    message: Message, // owned copy; never a &Message into a live slot
}
```

For the later shared-memory ABI, `Slot` must contain no Rust enum, `usize`, `Vec`,
reference, or pointer. This host phase may retain `usize` only as a temporary
prototype convenience.

### Publication outline

```text
publish(agent, topic, payload):
    recipient_mask = topic_index.load(topic) & !agent.reader_bit
    if recipient_mask == 0:
        return NoSubscriber

    chunk_count = ceil(payload.len / MAX_PAYLOAD)
    start = ring.reserve_range(chunk_count)        // CAS, no partial claim
    write every chunk with message_id = start
    store pending_readers = recipient_mask for every chunk
    release-commit the reservation as READY         // chunks become visible together
    broker_ingress.notify(1)                        // Android phase; host stand-in now
```

`reserve_range(n)` must refuse `n == 0`, `n > 255`, `n > BUFFER_SIZE`, and a range
whose expected slots are not all reusable. It may physically wrap while logical
sequences remain `start..start+n`.

### Read/release outline

```text
read_next(agent):
    expected = agent.read_cursor
    slot = ring.slot(expected % BUFFER_SIZE)
    observed = slot.seqno.acquire_load()

    if observed represents a newer READY generation:
        agent.read_cursor = logical_sequence_from(observed)
        retry                                      // normal fast-forward
    if observed is not READY for expected:
        return WouldBlock
    if slot.pending_readers does not contain agent.bit:
        agent.read_cursor += 1
        retry                                      // not addressed to this agent

    claim/verify the committed range
    copy all chunk bytes into an owned message
    for every chunk: clear agent.bit with fetch_and(!agent.bit, AcqRel)
    advance agent.read_cursor past the range
    return assembled message
```

The exact same-agent concurrent-read contract is still open. Until it is decided,
enforce one drain owner per `AgentBus` rather than pretending an atomic cursor
makes simultaneous `read()` calls safe.

### Broker outline

```text
broker_loop:
    wait on broker_ingress_eventfd
    while next notification cursor points at a fully committed record:
        recipient_mask = record.pending_readers
        for each set agent bit:
            data_eventfd[agent].write(1)
        notification_cursor advances over the complete logical message
```

The broker has a notification cursor only. It does not clear reader bits or copy
payload. Catalog and lifecycle operations are Binder calls; a public topic
announcement is an ordinary `TopicCreated` record delivered through `data_eventfd`.

## Memory Ordering and Unsafe-Code Contract

The eventual implementation cannot be reviewed by looking only at the CAS loop.
Every non-atomic payload access through `UnsafeCell` needs a written happens-before
argument. This section is the minimum contract that code review and loom tests must
enforce.

### Publication ordering

For a single slot/range, the logical ordering is:

```text
producer owns expected FREE generation
    -> writes non-atomic header and payload bytes
    -> writes pending reader mask / chunk metadata
    -> release-store READY sequence or reservation commit

reader acquire-loads READY sequence or reservation commit
    -> sees every preceding payload/header write
    -> proves its bit is pending
    -> claims active access (when safeguard is enabled)
    -> copies bytes into owned storage
```

The release store on readiness and acquire load by the reader are the publication
edge. The eventfd wake is not the memory-ordering proof. An agent may wake before
or after it observes the commit, so it must always acquire-check the shared state.

### Reclamation ordering

```text
reader copy complete
    -> clear active bit (release)             [spill-capable implementation]
    -> fetch_and(!my_bit, AcqRel) pending mask
    -> if old pending mask was exactly my_bit:
           release-store expected next FREE sequence

producer acquire-loads expected FREE sequence
    -> may write the next physical generation
```

`pending_readers` must remain nonzero throughout copying. Otherwise the final
reader could make a slot reusable before it finished reading bytes. Once journal
spill exists, `active_readers` is also required because the monitor needs to know
whether it can transfer delivery from a physical slot to the journal safely.

### Reservation ordering for chunks

A multi-slot writer may reserve a sequence range before another writer completes
an earlier range. Readers and the broker must preserve logical order: when they
reach a `WRITING` reservation at cursor `N`, they stop even if a later reservation
is already physically filled. This avoids exposing messages out of logical order.

The reservation descriptor must have exactly one publication point. Two valid
implementations are:

1. Put a `reservation_state`/generation in the first chunk and have every chunk
   reference it; release-commit only that descriptor after all chunks are filled.
2. Give every chunk a state and release-store a shared commit marker in the first
   chunk; readers verify all expected chunk sequences after seeing the marker.

Do not make every chunk independently `READY` without a range-level marker. That
would allow a reader to observe chunk zero while later chunks are still being
written.

### UnsafeCell review checklist

Before marking `RingBuffer` `Send`/`Sync`, document and test all of these:

- A writer is the unique owner of a physical slot generation before it mutates
  `value`.
- A reader touches `value` only after it observes the matching committed sequence.
- A recipient bit (and active claim, when enabled) prevents writer reuse while that
  reader copies.
- No public method returns `&Message`, `&[u8]`, or a pointer into a reusable slot.
- A reader cannot clear/release a generation it did not observe and own.
- Pressure eviction blocks new active claims before it waits for old ones.
- A process-local mutex never protects cross-process payload correctness; only
  shared atomics and fixed shared state do.

Use comments beside each atomic operation to state its synchronization partner;
avoid comments such as “atomic for multiple consumers” without naming the precise
publication/reclamation edge.

## Overflow Journal Contract

The journal is an emergency delivery tier used by the safeguard, not merely an
append-only debug trace. Once a live record has been transferred to the journal,
its readers must be able to complete delivery without touching the reused physical
slot.

### Logical journal entry

```rust
// Conceptual record. Final disk/shared ABI uses fixed-width values and explicit
// byte serialization; this is not a Rust struct copied directly to disk.
struct JournalEntry {
    first_sequence: u64,
    message_id: u64,
    sender_id: u8,
    topic_id: u8,
    topic_generation: u32,
    flags: u8,
    chunk_count: u8,
    remaining_readers: AtomicU64,
    encoded_len: u32,
    encoded_message: Vec<u8>, // all chunks reassembled before append
    integrity: JournalIntegrity,
}
```

The journal owns a copy of the **same recipient snapshot** that was in the ring.
When a reader replays it, the journal clears that reader's bit only after the
reader has copied/decoded the journal entry. The journal entry can be garbage
collected only after its remaining mask becomes zero and no live-ring handoff can
refer to it.

### Spill transaction

```text
1. monitor chooses oldest complete READY logical message at critical pressure
2. CAS reservation state READY -> EVICTING
3. deny new active physical-reader claims; existing active claims may finish
4. copy all chunks and append one complete journal entry
5. verify journal append/integrity succeeds
6. wait until active_reader mask is zero
7. install a journal-gap marker or sequence-to-journal index
8. mark physical chunks JOURNALED -> FREE for producer reuse
9. wake pending readers so they drain/replay promptly
```

If any operation through step 6 fails, the ring record remains non-reusable. The
system may retry or return publisher backpressure, but it must not free the slot.
The exact durability policy (append only versus `fdatasync` before free) is an
explicit later performance/reliability choice. The correctness minimum is that the
journal reader can recover a complete, verified entry before physical bytes change.

### Reader journal handoff

```text
reader cursor reaches sequence S
    -> sees JOURNALED/gap marker instead of expected READY physical generation
    -> lookup journal entry by first_sequence/message_id
    -> verify topic generation and integrity
    -> copy/reassemble/decode owned journal payload
    -> clear reader bit in journal remaining mask
    -> advance cursor over the full logical range
```

The implementation must record enough metadata for a reader to distinguish:

- a normal fast-forward over a record it never owned;
- a journaled record it does own and must replay;
- a corrupted/missing journal entry, which is a hard delivery error and must be
  surfaced in metrics rather than silently skipped.

### Journal test matrix

| Case | Required assertion |
|---|---|
| 80% targeted wake succeeds | No journal write occurs; normal reader releases live slot. |
| 90% spill of one slot | Journal contains identical metadata/body before physical sequence changes. |
| Chunked spill | Either all chunks appear as one journal message or none do. |
| Active reader race | Eviction waits; reader sees original bytes or replays journal, never mixed bytes. |
| Journal append failure | Record remains live/EVICTING; writer cannot reuse it. |
| Two pending readers | One journal replay leaves entry; final replay makes it collectible. |
| Live/journal overlap | Same message ID is delivered once after deduplication. |
| Old topic generation | Replay uses old descriptor/key generation, never a reused topic ID's descriptor. |

## Supporting Source-Level Checklist

The step-by-step plan above is the authoritative execution order. This compact
checklist cross-references the module-level changes expected in each step.

### Phase 0 — Stabilize the existing single-slot core

1. Change `MAX_PAYLOAD_SIZE` to 4 KiB; preserve fixed `Message` storage.
2. Replace borrowed `try_read()` with an owned-copy API.
3. Add expected-sequence and ownership validation to release.
4. Start new reader cursors from `head`, not zero.
5. Exclude publisher from masks; reject zero recipients.
6. Implement sequence-ahead fast-forward and explicit `WouldBlock`/`BufferFull`
   errors instead of treating normal states as generic read errors.

### Phase 1 — Registry and subscriber semantics

1. Replace the static `MessageTopic` enum with `u8` plus a local descriptor map.
2. Model all 256 topic IDs and `u32` topic generations.
3. Keep an atomically readable topic-to-mask table instead of allocating/scanning a
   `Vec<Uuid>` on each publish.
4. Define unsubscribe as future-only while still releasing already-owned records.
5. Implement graceful unregister: remove future masks, drain to cutoff, verify no
   pending bit, then recycle the reader bit.

### Phase 2 — Range reservation and chunking

1. Add `reserve_range(n)` with all-or-nothing range validation and CAS claim.
2. Add chunk metadata, a reservation-level state/commit, physical-wrap tests, and
   owned reassembly.
3. Ensure a reader seeing a partial/`WRITING` reservation returns `WouldBlock`; it
   never reads later records out of order.

### Phase 3 — Host broker notification model

1. Split the current `Broker` concept into ring access versus registry/notifier.
2. Model an ingress event and a broker notification cursor.
3. Signal a recipient once per complete logical message, even if it is already
   awake. Do not introduce `wait_count` or polling.
4. Model separate data/control events; topic creation increments a catalog
   generation and broadcasts only the control event.

### Phase 4 — Cross-process and Android integration

1. Convert fixed host fields to a documented `#[repr(C)]` ABI and move storage to
   `memfd`/`ASharedMemory`.
2. Add the vendor AIDL service, fd handoff, eventfd duplication, catalog operations,
   SELinux policy, and Binder death recipient.
3. Add publisher-death recovery for `WRITING` reservations; do not timeout-revoke a
   live slow reader.
4. Add Protobuf, per-topic key retrieval from AiSeal, local AES-GCM, C FFI, and
   the C++ top-level API.

## Test Plan (Required Before Android)

Tests should be deterministic first, then concurrent/stress tests. Do not rely on
a stress test as the only proof of ownership behavior.

### Unit tests: slots, masks, and cursors

- Initial sequence values make every physical slot writable for its first logical
  generation.
- Single recipient: publish, copy, release; only that reader releases the slot.
- Fan-out: two recipients see identical payload; first release retains the slot;
  final release makes it reusable.
- Duplicate release and release by a non-recipient return an explicit error and do
  not free a slot.
- Publisher subscribed to its own topic is excluded from the recipient mask.
- Zero effective recipients returns `NoSubscriber` without changing `head`.
- Reader registration at current `head` receives no historic data.
- Late subscription receives only a publication whose recipient snapshot contains
  its bit.
- Unsubscribe still clears old owned records according to the selected policy.
- Sequence-ahead observation fast-forwards; an exact expected sequence still reads;
  uncommitted expected sequence returns `WouldBlock`.

### Chunk tests

- One-, two-, and 255-chunk messages reassemble byte-for-byte.
- Two concurrent producers reserve ranges that never interleave.
- A reservation that wraps physical slot 499 to slot 0 reassembles in logical order.
- A full/partially unavailable range fails atomically without advancing `head` or
  making partial chunks visible.
- A reader sees `WouldBlock` during `WRITING` and sees the complete message only
  after reservation commit.
- Final recipient release of a chunked message releases every chunk exactly once.

### Registry/lifecycle tests

- All 64 reader bits allocate uniquely; the 65th registration fails cleanly.
- Graceful unregister removes future topic bits, drains to cutoff, and only then
  reuses the reader bit.
- A stale agent bit cannot be reused until no slot retains it.
- 256 dynamic topic IDs allocate uniquely; reuse increments topic generation.
- Public topic creation emits a `TopicCreated` ring record; private creation does
  not announce itself.

### Concurrency and model tests

- Four producers and multiple independent reader agents publish thousands of
  uniquely identified records. Every intended recipient sees each record exactly
  once; no unintended reader sees it.
- A deliberately slow reader crosses 80% and receives a targeted drain wake. At
  90%, complete records are journaled before their physical slots are reused;
  journal replay preserves delivery. Publishers receive `BufferFull` only if the
  safeguard cannot reclaim safely.
- Readers continuously fast-forward across high-volume non-subscribed topics while
  receiving every addressed record.
- Use `loom` for reduced-size atomic interleavings of publication, read/release,
  and unregister. Keep the model small (for example four slots and two readers).
- Run stress tests repeatedly under ThreadSanitizer where available. Also run
  `cargo test --release` because sequence and overflow bugs can be profile-specific.

### Full-scale host stress test example

The following is intentionally written against the **target host API**, not the
current incomplete `Broker` API. It is a template for `tests/mpmc_stress.rs` after
Steps 1–6 exist. Keeping it here makes the required behavior concrete before the
implementation is refactored.

Test configuration:

```text
4 publishers × 20,000 messages each = 80,000 logical messages
12 subscriber agents
4 dynamic topics
Each subscriber owns two topics; publishers are not subscribers in this test
Ring capacity: 128 slots (small enough to exercise pressure)
Payload: deterministic sender/ordinal/topic checksum
Pressure journal: enabled
```

```rust
// tests/mpmc_stress.rs -- target API sketch, made compilable as the host API lands.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Barrier, Mutex},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

const PUBLISHERS: usize = 4;
const SUBSCRIBERS: usize = 12;
const TOPICS: usize = 4;
const PER_PUBLISHER: usize = 20_000;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DeliveryKey {
    publisher: u8,
    ordinal: u32,
    topic: u8,
}

fn topic_for(publisher: usize, ordinal: usize) -> u8 {
    ((publisher * 17 + ordinal) % TOPICS) as u8
}

fn payload_for(key: DeliveryKey) -> Vec<u8> {
    // Includes redundant data so a bad slot reuse/copy is detected, not merely
    // a missing message. Real test uses a 4 KiB patterned body periodically.
    let checksum = (key.publisher as u32)
        .wrapping_mul(0x9e37_79b9)
        .wrapping_add(key.ordinal)
        .wrapping_add(key.topic as u32);
    [key.publisher, key.topic]
        .into_iter()
        .chain(key.ordinal.to_le_bytes())
        .chain(checksum.to_le_bytes())
        .collect()
}

fn decode_key(bytes: &[u8]) -> DeliveryKey {
    assert_eq!(bytes.len(), 10);
    let publisher = bytes[0];
    let topic = bytes[1];
    let ordinal = u32::from_le_bytes(bytes[2..6].try_into().unwrap());
    let checksum = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
    assert_eq!(checksum,
        (publisher as u32).wrapping_mul(0x9e37_79b9)
            .wrapping_add(ordinal).wrapping_add(topic as u32));
    DeliveryKey { publisher, ordinal, topic }
}

#[test]
#[ignore = "slow stress test; run with cargo test --release -- --ignored"]
fn mpmc_fanout_journal_stress_has_no_duplicate_or_lost_delivery()
    -> Result<(), TestError>
{
    let system = Arc::new(HostSystem::new(HostConfig {
        ring_slots: 128,
        payload_bytes: 4 * 1024,
        max_agents: 64,
        max_topics: 256,
        pressure_watermark_percent: 80,
        critical_watermark_percent: 90,
        journal: JournalConfig::in_memory_for_test(),
    }));

    // Register subscriber agents first. Their subscription pattern is stable and
    // known, so the test can calculate the exact expected fan-out set.
    let mut subscribers = Vec::new();
    let mut expected: HashMap<usize, HashSet<DeliveryKey>> = HashMap::new();
    for reader_index in 0..SUBSCRIBERS {
        let bus = system.register(format!("reader-{reader_index}"))?;
        let first = (reader_index % TOPICS) as u8;
        let second = ((reader_index + 1) % TOPICS) as u8;
        bus.subscribe(TopicId(first))?;
        bus.subscribe(TopicId(second))?;
        expected.insert(reader_index, HashSet::new());
        subscribers.push((reader_index, bus, [first, second]));
    }

    // Calculate all expected deliveries before threads start. Publishers have no
    // subscriptions, so no self-delivery subtraction is needed in this test.
    for publisher in 0..PUBLISHERS {
        for ordinal in 0..PER_PUBLISHER {
            let topic = topic_for(publisher, ordinal);
            let key = DeliveryKey { publisher: publisher as u8, ordinal: ordinal as u32, topic };
            for (reader_index, _, topics) in &subscribers {
                if topics.contains(&topic) {
                    expected.get_mut(reader_index).unwrap().insert(key);
                }
            }
        }
    }

    let expected = Arc::new(expected);
    let expected_counts = Arc::new(expected.iter()
        .map(|(reader, keys)| (*reader, keys.len()))
        .collect::<HashMap<_, _>>());
    let start = Arc::new(Barrier::new(PUBLISHERS + SUBSCRIBERS + 1));
    let publishers_done = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(Mutex::new(HashMap::<usize, HashSet<DeliveryKey>>::new()));
    let duplicate_count = Arc::new(AtomicUsize::new(0));
    let reader_handles: Vec<_> = subscribers.into_iter().map(|(reader_index, bus, _)| {
        let start = Arc::clone(&start);
        let done = Arc::clone(&publishers_done);
        let delivered = Arc::clone(&delivered);
        let duplicates = Arc::clone(&duplicate_count
