# Future Work: Producer Holes and Wakeup Progress

## Status

This is a known limitation of the current host prototype. It is deliberately
**not** addressed by adding an ordered producer `commit_head`: that design was
tested and rejected because one paused producer prevented every later producer
from completing a publish and could fill the ring with reserved-but-invisible
slots.

The current ring therefore keeps independent producer publication:

1. A producer claims a monotonically increasing ticket with the existing
   `head` CAS.
2. The producer writes its payload and recipient mask to its claimed slot.
3. The producer publishes that slot by storing its ready `seqno` with Release
   ordering.
4. Later producers may publish later tickets even when an earlier producer has
   been paused between claiming and publishing its own ticket.

This preserves producer progress. It does not eliminate a reader-side FIFO
hole.

## The Producer-Hole Scenario

Assume two independent producers and two agents:

- Writer A claims ticket `0`, whose message is for reader A.
- Writer A is descheduled before its slot becomes ready.
- Writer B claims and fully publishes ticket `1`, whose message is for reader
  B only.
- Reader B is notified for B's message and calls `read()`.

Reader B's cursor is still `0`. The ring is FIFO, so it examines physical slot
`0` first. That slot is not ready because writer A has not published it. The
read returns `BufferEmpty`/`NoAvailableRead`; reader B cannot skip to ticket
`1` merely because ticket `1` is ready.

Once A publishes ticket `0`, B's reader can skip it because B is not in its
recipient mask, advance its cursor to ticket `1`, and receive B's message.

The data is not overwritten or lost. The issue is notification progress:

1. B's targeted notification may already have been consumed by reader B.
2. A's later publish normally notifies only reader A, not reader B.
3. Reader B can therefore sleep after its unsuccessful attempt while its own
   message is now reachable behind A's completed slot.

`/tmp/ringbuffer-perf.XT4kna/src/bin/stalled_writer.rs` reproduces this exact
sequence in a disposable benchmark copy.

## Why We Do Not Use an Ordered Commit Cursor

An attempted design split `head` into `reserve_head` and `commit_head`:

- producers reserve tickets independently;
- a producer can publish only once `commit_head == its_ticket`;
- each successful publisher advances `commit_head` to the next ticket.

This makes reader visibility strictly contiguous, but creates producer
head-of-line blocking. With ticket `0` paused and tickets `1..5` reserved:

- all later writers spin or wait for ticket `0`;
- no later writer returns from publish;
- the ring can report full although no message is reader-visible;
- CPU is consumed by waiting writer threads.

The disposable experiment at
`/tmp/ringbuffer-perf.XT4kna/src/bin/stalled_commit.rs` demonstrated this with
a six-slot ring: while writer A held ticket `0`, five later writers reserved
the remaining tickets, `commit_head` remained `0`, and a seventh writer
received `BufferFull`. Releasing A allowed the queued writers to cascade, but
the design made every producer dependent on the earliest paused producer.

This is unacceptable for the intended MPMC data path. Do not reintroduce a
commit cursor without revisiting this trade-off.

## Current Guarantees and Non-Guarantees

The current design guarantees:

- A successful slot publication is visible to its intended readers through the
  slot `seqno` and recipient bitmask.
- A reader does not free a slot until every recipient has cleared its bit.
- A slow recipient can apply pressure, but a later producer is not blocked by
  an earlier producer merely because the earlier producer was descheduled.
- `try_read()` preserves cursor order: it does not read ticket `n + 1` before
  it has resolved ticket `n`.

The current design does not yet guarantee:

- A single targeted wakeup always leads immediately to delivery if an earlier
  ticket is still being published.
- A reader blocked behind an unrelated producer hole will receive another
  targeted wakeup when that hole closes.
- Recovery from a producer process that dies after claiming a ticket but
  before publication. This belongs to Android Binder death handling.

## Intended V1 Handling

Keep the ring implementation independent-producer and lock-free. Treat the
event notification as a prompt to attempt reads, not as a proof that the next
cursor position is immediately readable.

Use polling as the fallback already accepted for the project:

1. An agent receives a normal event notification and calls `read()` or
   `recv()`.
2. If the receive path sees no readable message, it uses the normal
   read/listen/read pattern to avoid ordinary lost wakeups.
3. If an earlier producer hole is suspected, use a bounded retry/backoff or a
   timed polling fallback instead of relying forever on the original targeted
   notification.
4. Once the earlier slot is published or cleaned up, the reader's sequential
   cursor reaches the later intended message.

Do not put an unbounded retry loop in `Broker::read()`. `read()` remains a
single non-blocking attempt. Any waiting/backoff belongs in the agent-facing
blocking receive behavior; otherwise `Broker::read()` would hold lifecycle
coordination during an unbounded wait and would cease to be non-blocking.

The exact API and backoff policy can be decided when eventfd is introduced.
At expected V1 traffic (roughly 10--30 messages per second for most topics),
the priority is correctness and bounded idle CPU use, not peak microbenchmark
throughput.

## Optional Journal Replay Hardening

The current broker serializes normal read, journal replay, and unregister work
for one agent through its lifecycle coordination. The safeguard is also the
only journal writer, so duplicate journal delivery is not a current bug.

If `Journal::replay_from_journal()` is ever exposed outside that broker
contract (for example, directly from a future shared-memory `AgentBus`), make
the pending-bit clear the authoritative delivery claim. For each candidate
entry, inspect the mask returned by `fetch_and(!agent_bit, AcqRel)` and clone
or return the message only when that returned mask still contains `agent_bit`.
A prior filtering `load()` is only a snapshot; two concurrent callers could
both observe it before either clears the bit.

Do not prioritize this before the journal API is directly callable or the
current lifecycle serialization changes.

## Safeguard and Journal Relationship

The safeguard solves a different pressure problem:

- A message has already been fully published.
- One or more intended readers are slow or no longer reading.
- Its recipient bits prevent the slot from being reclaimed.

At high ring pressure, the safeguard will:

1. issue final notifications to pending readers;
2. copy already-published pending messages to the overflow journal;
3. preserve the outstanding recipient mask in the journal;
4. free the live ring slot only after the journal is authoritative;
5. allow readers to complete delivery from the journal later.

### Recovery API boundary

The journal is a recovery channel, not a second leg of the live-ring cursor. A
successful spill signals a dedicated journal event only to the record's pending
readers. `read_journal()` drains all journal records still addressed to the caller
and returns an empty vector when no recovery work exists. It does not advance the
live `read_cursor`; normal `read()` remains entirely a ring operation.

This deliberately does not guarantee a merged live/journal receive order. A reader
may process a newer live record before a journaled older record. Journal records
retain their ticket, message ID, and timestamp so an application that requires
chronology can reconcile them. This is acceptable for V1 recovery; strict ordered
replay would require an agent-side merge protocol and is not part of the safeguard.

The safeguard must not treat a claimed-but-unpublished producer slot as a
normal unread message. It may not contain valid payload or recipient metadata
that can safely be journaled. A producer hole is a producer lifecycle issue,
not a slow-reader issue.

## Low-Occupancy Head Blocking

`live_slots / capacity` is not sufficient to describe whether the ring can
accept another publication. Per-agent cursors can reclaim physical slots out
of order. After the producer head wraps, one old unread slot can block the
next required physical position even when most other slots are free.

For example, with a ten-slot ring, ticket `1` may remain unread while ticket
`8` has been fully reclaimed. When `head` reaches ticket `11`, it maps to
physical slot `1` again and `try_write()` returns `BufferFull`; it cannot jump
ahead to the free physical slot `8` without skipping tickets and violating
the ring protocol. The live-slot percentage may be only ten percent in this
case, but writers are still blocked.

Before journal migration is implemented, add a buffer inspection API for the
current producer head. It must distinguish:

- `Writable`: the current head slot is free for this generation.
- `BlockedByPublishedMessage`: a ready message with pending recipients owns
  the current head slot.
- `BlockedByProducerHole`: a producer claimed the earlier ticket but has not
  fully published it.

Safeguard policy for a failed publication should be independent of the normal
80/90 percent thresholds:

1. On `BufferFull`, request an immediate safeguard check and inspect the
   current head slot.
2. For a published blocker, send one targeted final notification to that
   slot's pending readers.
3. If a later failed publication finds the same head ticket still blocked,
   journal that specific fully published message, then free the live slot only
   after the journal record is authoritative.
4. Never journal a producer hole. It requires bounded retry/backoff or future
   Android Binder-death cleanup.

At true high occupancy (`>= 90%`, including a completely full ring), normal
batch journal migration still applies and should reduce live pressure toward
the recovery threshold. Head-block handling is the reactive low-occupancy
exception.

## Broker Performance

The host prototype's direct ring path is materially faster than the current
`Broker` path under both sequential and contended workloads. This is expected
for the prototype but must be improved before treating the broker API as a
representative Android data-plane implementation.

The main hot-path costs to measure and reduce are:

1. taking the broker routing `RwLock` for every publication;
2. taking the registry `Mutex` and reconstructing the recipient mask and
   listener vector for every publication;
3. taking the publisher's lifecycle-state mutex for every write and the
   reader's lifecycle-state mutex for every read;
4. allocation/cloning of the listener vector before notification;
5. retry/yield pressure when the bounded ring reaches capacity.

Do not remove lifecycle or unregister correctness checks merely to improve a
microbenchmark. Instead, profile each cost independently and move only
immutable routing/session data needed by the eventual shared-memory data path
into agent-owned snapshots or safely published read-mostly structures. Keep
registration, topic creation, subscription mutation, and unregistration in
the control-plane synchronization domain.

Benchmark future changes in release mode with at least:

- direct ring versus broker sequential publish/read, with empty and 300-byte
  payloads;
- one and four concurrent writers with one reader;
- four-writer/four-reader mixed-recipient masks, validating exact delivery;
- one-writer/four-reader fan-out;
- event no-listener, inline, and cross-thread wake costs;
- both 500-slot and 1024-slot capacities, reporting buffer-pressure retries.

Record host topology and distinguish published-message throughput from total
per-recipient deliveries. Android and eventfd results must be measured on
target hardware separately; this host prototype does not include Binder,
shared-memory FD handoff, or eventfd system-call cost.

## Android Binder Death Handling

When an Android agent first registers, the vendor AIDL service should retain
its Binder client token and install a death recipient. On process death, the
service must:

1. remove the agent from future topic routing and subscriptions;
2. clear its recipient bit from live ring slots and journal records;
3. wake/re-run cleanup for slots that become releasable;
4. recycle the agent bit only after all pending references are gone.

For a producer that dies with an incomplete reservation, cleanup also needs a
way to identify that reservation's owner and release/skip it. This requires
small reservation-ownership metadata in the eventual shared-memory design
(for example, a producer agent bit stored atomically after ticket claim). It
does not require an ordered global commit cursor or a mutex per slot.

Already-published messages from a dead producer normally remain valid and
should stay deliverable to their recipients. The service should purge only
the dead agent's incomplete claims and messages still pending *for* the dead
agent, unless future policy explicitly chooses to discard a dead publisher's
committed messages.

## Future Alternatives

Re-evaluate these only if measured Android behavior shows the polling fallback
is unacceptable:

### Broadcast Ring Progress

Notify every registered agent whenever any producer publishes. A reader
blocked by an unrelated hole will be woken when that hole closes. This is
simple, but creates unnecessary eventfd writes and wakeups for uninterested
agents.

### Per-Agent Pending Work Counter

Maintain an agent-owned pending-work counter that the notification service
increments after a successful routed publish and that the agent decrements on
successful delivery. A nonzero value tells `recv()` that work is owed even if
the current cursor position is temporarily blocked.

This avoids global broadcasts but adds accounting across normal delivery,
journal transfer, unregister, and Binder death. It was intentionally deferred
for V1 to avoid adding a second delivery-accounting system alongside the ring
recipient mask. Do not place such state casually in `Registry::AgentDetails`;
if adopted, make its ownership explicit in the agent-facing wakeup/session
state.

### Richer Slot Metadata

Publish recipient metadata early enough that a reader can prove an unfinished
earlier ticket is not intended for it and skip that ticket. This increases
slot protocol complexity and must be compatible with journal and death
cleanup. It is not a V1 change.

### Per-Topic or Per-Producer Queues

These reduce global FIFO holes but substantially complicate topic routing,
agent death cleanup, FD handoff, wakeup aggregation, and pressure management.
They remain an alternative architecture, not the current plan.

## Required Future Tests

Before changing this behavior, add deterministic tests for:

1. writer A reserves ticket `0` and pauses;
2. writer B publishes ticket `1` for a different reader;
3. B's reader cannot deliver ticket `1` before ticket `0` is resolved;
4. after A publishes, B's reader reaches ticket `1` exactly once;
5. fallback receive behavior eventually retries rather than sleeping forever;
6. producer death after reservation releases the hole without corrupting later
   generations;
7. safeguard spill preserves recipient obligations while freeing live slots.

## Decision Record

For the current prototype:

- Keep the single `head` CAS and per-slot `seqno` publication.
- Do not add `commit_head` / ordered producer publication.
- Keep polling as the fallback for producer-hole wakeup progress.
- Defer global wake broadcast, pending-work counters, and richer slot metadata
  until Android measurements justify their added complexity.

## Safeguard Migration Wait Backoff

`begin_migration()` currently waits for `readers_in_flight` to reach zero after
claiming a slot's `migrating` marker. The initial prototype uses a simple busy
loop so the synchronization protocol is easy to inspect.

Before treating migration as production-ready, replace the bare `continue`
with bounded spin/backoff:

1. call `std::hint::spin_loop()` for a small, bounded number of checks;
2. if the reader is still in flight, call `std::thread::yield_now()` before
   beginning the next bounded spin period;
3. retain the `SeqCst` load and do not hold a registry or broker lock while
   waiting.

This does not block the ring globally: the chosen slot remains unavailable
under its migration marker while other slots remain independent. The purpose
is to avoid a paused in-flight reader causing the safeguard worker to consume
an entire CPU core repeatedly loading the same atomic.
