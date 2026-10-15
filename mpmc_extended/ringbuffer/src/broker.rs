use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::{eprintln};
use event_listener::Event;
use uuid::Uuid;

use crate::message::{Message, MessageKind, TopicDescriptor, TopicId, TopicVisibility};
use crate::buffer::{RingBuffer, RingBufferError};
use crate::registry::{AgentDetails, AgentState, Registry, RegistryError};
use crate::agent::Agent;


struct PublishSnapshot {
    reader_mask: u64,
    reader_events: Vec<Arc<Event>>,
    agent_details: Arc<AgentDetails>,
}

// Broker - to deal with Buffer operations and the subscription registry
// It owns handles to both (Arcs/shared pointers)
// Agents go through the broker for publishing/writing while the broker handles the notification stream
// Agent Registration also goes through the broker
pub struct Broker {
    buffer: RingBuffer,
    registry: Mutex<Registry>, // so registry can be updated when agents subscribe/unsubscribe to topics
    // lock to handle situation where an agent could be publishing something meant for another agent who's unregistering
    routing_gate: RwLock<()>, 
}

impl Broker {

    pub fn new(buffer: RingBuffer) -> Arc<Self> {
        Arc::new(Self { 
            buffer, 
            registry: Registry::new().into(),
            routing_gate: RwLock::new(()),
        })
    }

    // When you call register, you get an agent back
    // can't provide &mut self because it can't be turned into an Arc<Broker> as required by the user
    // request &Arc<Self> so that the broker can be used after this is called
    pub fn register(self: &Arc<Self>) -> Result<Agent, RegistryError> {
        let agent = Agent::new(Arc::clone(&self));
        // Update registry to include agent
        self.registry().register(&agent)?;
        Ok(agent)
    }

    // Unregister an agent from the centralized registry
    pub fn unregister(&self, agent_id: &Uuid) -> Result<(), RegistryError> {
        // TODO - Drain outstanding delivery - If agent is not done
        let guard = self.routing_gate.write().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;

        let mut state = details.agent_state.lock().unwrap();
        if *state != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        // set state to draining - so other operations can't proceed
        *state = AgentState::Draining;
        // remove agent from all topics
        {
            self.registry().start_unregister(agent_id)?;
        } // registry lock is dropped here
        let cutoff = self.buffer.current_write_cursor();
        drop(guard);
        if self.buffer.drain_to(&details, cutoff).is_err() {
            return Err(RegistryError::DrainError);
        }
        // Verify that there are no pending messages and deallocate bit
        // Should be impossible to be true since drain has finished
        if !self.buffer.has_pending_reader(details.agent_bit) {
            self.registry().finish_unregister(agent_id)?;
        } else {
            return Err(RegistryError::DrainError);
            //TODO - add some diagnostic module to handle this - or a resume_drain()
        }

        *state = AgentState::Removed;
        drop(state);
        Ok(())
    }

    /// Subscribe an agent to a topic
    pub fn subscribe(&self, agent_id: &Uuid, topic_id: TopicId) -> Result<(), RegistryError> {
        let _guard = self.routing_gate.read().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        self.registry().subscribe(agent_id, topic_id)
    }

    /// Unsubscribe an agent from a topic
    pub fn unsubscribe(&self, agent_id: &Uuid, topic: TopicId) -> Result<(), RegistryError> {
        let _guard = self.routing_gate.read().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        self.registry().unsubscribe(agent_id, topic)
    }

    pub fn get_all_topics(&self, agent_id: &Uuid) -> Result<Vec<TopicDescriptor>, RegistryError> {
        let _guard = self.routing_gate.read().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        self.registry().all_topics(agent_id)
    }

    pub fn find_topic(&self, agent_id: &Uuid, topic_id: &TopicId) -> Result<TopicDescriptor, RegistryError> {
        let _guard = self.routing_gate.read().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        self.registry().find_topic(agent_id, topic_id)
    }

    /// Creates a topic and sends a Topic Created message to the ring
    /// The owner gets back the TopicDescriptor while other agents get a json-like payload
    /// serde_json can be used to deserialize
    pub fn create_topic(&self, agent_id: &Uuid, name: String, schema: String, visibility: &TopicVisibility, version: &u32) -> Result<TopicDescriptor, RegistryError> {
        // Take write guard so this is a black box to other agents while in progress
        let guard = self.routing_gate.write().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        let mut registry = self.registry();
        let topic_descriptor = registry.create_topic(agent_id, name, schema, visibility, version)?;
        // Post a message on topic creation to everyone
        let mut message = Message::new(
            topic_descriptor.id, 
            MessageKind::TopicCreated, 
            1, 
            1
        ).unwrap();

        // only publish and broadcast if this topic is meant to be public
        if *visibility == TopicVisibility::Public {
            let payload = match serde_json::to_vec(&topic_descriptor).map_err(|_| RegistryError::InvalidMessage) {
                Ok(payload) => payload,
                Err(error) => {
                    registry.abort_topic_creation(agent_id, &topic_descriptor.id)?;
                    return Err(error);
                }
            };
            
            match message.set_payload(payload).map_err(|_| RegistryError::InvalidMessage) {
                Ok(_) => {},
                Err(error) => {
                    registry.abort_topic_creation(agent_id, &topic_descriptor.id)?;
                    return Err(error);
                }
            };

            let create_snapshot = match self.publish_snapshot_from_registry(agent_id, &message, &registry) {
                Ok(snapshot) => snapshot,
                Err(RegistryError::NoAgentsAvailable) => {
                    // since the message kind is TopicCreated and we have the write guard and have validated the agent
                    // the error will always be NoAgentsAvailable right? and we essentially treat that as success
                    return Ok(topic_descriptor);
                }
                Err(error) => {
                    registry.abort_topic_creation(agent_id, &topic_descriptor.id)?;
                    return Err(error);
                } 
            };

            match self.publish_internal(message, &create_snapshot) {
                Ok(_) => {
                    drop(registry);
                    drop(guard);
                    self.notify_agents(&create_snapshot);
                    return Ok(topic_descriptor);
                },
                Err((error, _message)) => {
                    registry.abort_topic_creation(agent_id, &topic_descriptor.id)?;
                    drop(registry);
                    drop(guard);
                    return Err(error);
                }
            }
        }
        
        Ok(topic_descriptor)
    }

    /// Main method for publishing to the ring. Everyone eventually gets here.
    /// Message doesn't have to be a reference because the buffer takes ownership anyways
    /// Return message on error (e.g. buffer full or no current recipient) so agent still keeps ownership
    /// Takes care of notifying all affected parties
    pub fn publish(&self, agent_id: &Uuid, message: Message) -> Result<(), (RegistryError, Message)> {
        let routing_guard = self.routing_gate.read().unwrap();

        let snapshot = match self.make_publish_snapshot(agent_id, &message) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err((error, message));
            },
        };

        match self.publish_internal(message, &snapshot) {
            Ok(_) => {
                drop(routing_guard);
                self.notify_agents(&snapshot);
            },
            Err((error, message)) => {
                return Err((error, message));
            }
        }
        Ok(())   
    }

    fn publish_internal(&self, message: Message, snapshot: &PublishSnapshot) -> Result<(), (RegistryError, Message)> {
        let state = snapshot.agent_details.agent_state.lock().unwrap();
        if *state != AgentState::Active {
            return Err((RegistryError::AgentDisconnecting, message));
        }

        match self.buffer.try_write(message, snapshot.reader_mask) {
            Ok(()) => {
                drop(state);
                return Ok(());
            },
            Err((error, message)) => {
                return Err((self.map_buffer_error(error), message));
            }
        }
    }

    fn notify_agents(&self, snapshot: &PublishSnapshot) {
        // Notify all subscribers for this topic
        // TODO: Figure out if it's worth notifying the original publisher as well
        for subscriber_event in &snapshot.reader_events {
            subscriber_event.notify(1);
        }
    }

    pub fn read(&self, agent_id: &Uuid) -> Result<Message, RegistryError> {
        let details = self.get_agent_details_from_registry(agent_id)?;
        // don't read normally if this agent is currently draining
        let state = details.agent_state.lock().unwrap();
        if *state != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        // Leave looping until it's the appropriate message to the buffer
        // This returns the message for a topic agent is subscribed to
        match self.buffer.try_read(details.as_ref())  {
            Ok(message) => Ok(message),
            Err(error) => {
                return Err(self.map_buffer_error(error));
            }
        }
    }

    /// So I don't have to type lock.unwrap a thousand times
    /// '_ means "Rust, infer the lifetime here" --> guard lives as long as the mutex borrow
    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap()
    }

    fn get_agent_details_from_registry(&self, agent_id: &Uuid) -> Result<Arc<AgentDetails>, RegistryError> {
        // Keep mutex alive for the duration of the function - don't hold it through buffer operations
        let registry = self.registry();
        if !registry.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        Ok(registry.get_agent_details(agent_id)?)
    }

    fn make_publish_snapshot(&self, agent_id: &Uuid, message: &Message) -> Result<PublishSnapshot, RegistryError> {
        // Keep mutex alive for the duration of the function
        // Keep outside of hot data path - point is lock-free data structure
        let registry = self.registry();
        if !registry.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }

        if !registry.topic_sanity(message.topic_id) {
            eprintln!("Topic is not recognized. Is it created?");
            return Err(RegistryError::TopicNotRecognized);
        }

        // Only an owner can send a topic creation message
        if message.kind == MessageKind::TopicCreated {
            if let Some(uuid) = registry.return_owner_for_topic(&message.topic_id) {
                if uuid != agent_id {
                    return Err(RegistryError::PermissionDenied);
                }
            }
        }
        self.publish_snapshot_from_registry(agent_id, message, &registry)
    }

    fn publish_snapshot_from_registry(&self, agent_id: &Uuid, message: &Message, registry: &Registry) -> Result<PublishSnapshot, RegistryError> {
        let mut topic_reader_mask;
        let mut events: Vec<Arc<Event>> = Vec::new();
        let publisher_details: Arc<AgentDetails> = registry.get_agent_details(agent_id)?;

        match message.kind {
            
            MessageKind::Data => {
                if let Some(subscribers) = registry.get_subscribers_for(&message.topic_id) { 
                    // Only write message to buffer if there's someone listening
                    if subscribers.is_empty() {
                        return Err(RegistryError::NoSubscriberForTopic);
                    }
                    topic_reader_mask = registry.create_mask_from_subscribers(&message.topic_id)?;
                    // Remove publisher from reader mask - to prevent hanging notification - see below
                    topic_reader_mask &= !publisher_details.agent_bit;

                    if topic_reader_mask == 0 {
                        return Err(RegistryError::NoSubscriberForTopic);
                    }
                    events.append(&mut subscribers.iter()
                            .filter(|uuid| *uuid != agent_id)
                            .map(|uuid| registry.get_agent_details(uuid).as_ref().unwrap().listener.clone())
                            .collect());
                } else {
                    return Err(RegistryError::NoSubscriberForTopic);
                }
            },

            // This message doesn't need subscribers
            MessageKind::TopicCreated => {
                topic_reader_mask = registry.create_mask_for_all_agents();
                topic_reader_mask &= !publisher_details.agent_bit;
                if topic_reader_mask == 0 {
                    events.clear(); // creation successful without notification
                    return Err(RegistryError::NoAgentsAvailable);
                } else {
                    events = registry.return_all_agent_listeners(publisher_details.agent_bit);
                }
            },

            _ => {
                return Err(RegistryError::MessageTypeNotSupported);
            }
        };

        Ok(PublishSnapshot { reader_mask: topic_reader_mask, reader_events: events, agent_details: publisher_details})
    }
    
    fn map_buffer_error(&self, error: RingBufferError) -> RegistryError {
        match error {
            RingBufferError::BufferEmpty => RegistryError::NoAvailableRead,
            RingBufferError::NoRecipient => RegistryError::NoSubscriberForTopic,
            RingBufferError::BufferFull => RegistryError::BufferPressure,
            RingBufferError::InvalidMetaData => RegistryError::InvalidMessage
        }
    }

    pub fn current_write_cursor(&self) -> u64 {
        self.buffer.current_write_cursor()
    }

}

// Testing
#[cfg(test)] // only compile when running tests
mod tests {
use std::{
    assert_eq,
    collections::HashSet,
    matches,
    sync::{mpsc, Arc, Barrier},
    thread,
    time::Duration,
};
use event_listener::Listener;

use crate::buffer::{DEFAULT_BUFFER_SIZE, DefaultRingBuffer};

use super::*; // parent module

    fn publish_with_retry(agent: &Agent, mut message: Message) {
        loop {
            match agent.publish(message) {
                Ok(()) => return,
                Err((RegistryError::BufferPressure, returned_message)) => {
                    message = returned_message;
                    thread::yield_now();
                }
                Err((error, _)) => panic!("unexpected publish error: {error:?}"),
            }
        }
    }

    fn read_expected_ids(agent: Agent, expected_ids: HashSet<u64>) -> HashSet<u64> {
        let mut received_ids = HashSet::with_capacity(expected_ids.len());

        while received_ids.len() < expected_ids.len() {
            match agent.read() {
                Ok(message) => {
                    assert!(
                        expected_ids.contains(&message.message_id),
                        "agent received an unexpected message id {}",
                        message.message_id
                    );
                    assert!(
                        received_ids.insert(message.message_id),
                        "agent received message {} more than once",
                        message.message_id
                    );
                }
                Err(RegistryError::NoAvailableRead) => thread::yield_now(),
                Err(error) => panic!("unexpected read error: {error:?}"),
            }
        }

        received_ids
    }

    #[test]
    fn register_subscribe_publish_read_round_trip() {
        let broker = Broker::new(RingBuffer::new());

        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();
        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());

        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let expected = message.clone();
        assert!(publisher.publish(message).is_ok());
        assert_eq!(expected, receiver_a.read().unwrap());
    }

    #[test]
    fn publisher_doesnt_receive_own_message() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let expected = message.clone();
        
        assert!(publisher.publish(message).is_ok());
        let publisher_read = publisher.read();
        assert!(publisher_read.is_err());
        assert!(matches!(publisher_read, Err(RegistryError::NoAvailableRead)));
        assert_eq!(expected, receiver_a.read().unwrap());
    }

    #[test]
    fn publish_with_only_self_subscribed_returns_error_and_message() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        assert!(publisher.subscribe(MessageTopic::Sports).is_ok());

        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let _expected = message.clone();
        let publisher_publish = publisher.publish(message);
        assert!(publisher_publish.is_err());
        assert!(matches!(publisher_publish, Err((RegistryError::NoSubscriberForTopic, _expected))));
    }

    #[test]
    fn publish_without_subscribers_returns_message() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let _expected = message.clone();
        let (error, published_message) = publisher.publish(message).expect_err("publish should fail without subscribers");
        assert!(matches!(error, RegistryError::NoSubscriberForTopic));
        assert_eq!(published_message, message);
    }

    #[test]
    fn read_with_no_messages_returns_error() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let receiver_a = broker.register().unwrap();
        let error = receiver_a.read().expect_err("read with no message should return error");
        assert!(matches!(error, RegistryError::NoAvailableRead));
    }

    #[test]
    fn unsubscribe_affects_future_publications_only() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let expected = message.clone();
        assert!(publisher.publish(message).is_ok());

        // receiver unsubscribes when message has been written
        assert!(receiver_a.unsubscribe(MessageTopic::Sports).is_ok());
        // receiver should still be able to read the one message
        assert_eq!(expected, receiver_a.read().unwrap());

        // Future message on same topic should return an error while publishing if no recipient
        let message_b = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let expected_b = message_b.clone();
        assert!(publisher.publish(message_b).is_err());
        let receiver_b = broker.register().unwrap();
        assert!(receiver_b.subscribe(MessageTopic::Sports).is_ok());

        // Now publish and read for b should succeed while receiver_a read should fail
        assert!(publisher.publish(message_b).is_ok());
        let error = receiver_a.read().expect_err("read with no message should return error");
        assert!(matches!(error, RegistryError::NoAvailableRead));
        assert_eq!(expected_b, receiver_b.read().unwrap());   
    }

    #[test]
    fn publish_returns_buffer_pressure_and_preserves_message() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let expected_a = message.clone();
        
        for _ in 0..DEFAULT_BUFFER_SIZE {
            assert!(publisher.publish(message).is_ok());
        }

        // Next publish should return an error + sent message since buffer is full
        let message_b = Message::new(MessageTopic::Sports, 90, 1).unwrap();
        let expected_b = message_b.clone();
        let (error, retained_msg) = publisher.publish(message_b).expect_err("buffer should be full");
        assert!(matches!(error, RegistryError::BufferPressure));
        assert_eq!(retained_msg, expected_b);

        // receiver reads one message, next write should succeed
        assert_eq!(expected_a, receiver_a.read().unwrap()); 
        assert!(publisher.publish(retained_msg).is_ok());
    }

    #[test]
    fn unregister_with_no_pending_messages_removes_agent() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        assert!(publisher.publish(message).is_ok());
        let expected_a = message.clone();
        assert_eq!(expected_a, receiver_a.read().unwrap());

        assert!(receiver_a.unregister().is_ok());
        assert!(matches!(receiver_a.subscribe(MessageTopic::Sports), Err(RegistryError::AgentNotRecognized)));
        assert!(matches!(receiver_a.read(), Err(RegistryError::AgentNotRecognized)));
        assert!(matches!(receiver_a.publish(expected_a), Err((RegistryError::AgentNotRecognized, _message))));
        assert!(matches!(receiver_a.unsubscribe(MessageTopic::Sports), Err(RegistryError::AgentNotRecognized)));
    }

    #[test]
    fn unregister_drains_sole_recipient_and_reclaims_slots() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();

        for _ in 0..DEFAULT_BUFFER_SIZE {
            assert!(publisher.publish(message).is_ok());
        }

        // Unregister should drain the messages for receiver_a
        // New receiver should be able to publish since slots are reclaimed
        assert!(receiver_a.unregister().is_ok());

        let publisher_b = broker.register().unwrap();
        let receiver_b = broker.register().unwrap();
        let message_b = Message::new(MessageTopic::Personalization, 90, 2).unwrap();
        assert!(receiver_b.subscribe(MessageTopic::Personalization).is_ok());
        for _ in 0..DEFAULT_BUFFER_SIZE {
            assert!(publisher_b.publish(message_b).is_ok());
        }
    }

    #[test]
    fn unregister_clears_only_departing_recipient_bit() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();
        let receiver_b = broker.register().unwrap();

        // Fill slots with publish for both a and b
        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        assert!(receiver_b.subscribe(MessageTopic::Sports).is_ok());
        assert!(receiver_b.subscribe(MessageTopic::Movies).is_ok());
        let message = Message::new(MessageTopic::Sports, 1, 1).unwrap();
        let message_b = Message::new(MessageTopic::Movies, 1, 1).unwrap();
        let expected = message.clone();
        let expected_b = message_b.clone();
        for _ in 0..DEFAULT_BUFFER_SIZE {
            assert!(publisher.publish(message).is_ok());
        }

        // unregister a - should only reduce pending readers - buffer should still be full
        assert!(receiver_a.unregister().is_ok());
        let (error, retained_msg) = publisher.publish(message_b).expect_err("buffer should be full");
        assert!(matches!(error, RegistryError::BufferPressure));
        assert_eq!(retained_msg, expected_b);

        // should still be possible for receiver b to read one message and write one back
        assert_eq!(expected, receiver_b.read().unwrap());
        assert!(publisher.publish(message_b).is_ok());
    }

    #[test]
    fn double_unregister_is_rejected() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let receiver_a = broker.register().unwrap();
        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        assert!(receiver_a.unregister().is_ok());
        assert!(matches!(receiver_a.unregister(), Err(RegistryError::AgentNotRecognized)));
    }

    #[test]
    fn bit_is_reusable_only_after_successful_unregister() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let mut receivers: Vec<Agent> = Vec::new();
        for _ in 0..64 {
            receivers.push(broker.register().unwrap());
        }
        assert_eq!(receivers.len(), 64);
        let receivers_details: Vec<Arc<AgentDetails>> = receivers.iter().map(|agent| broker.get_agent_details_from_registry(&agent.id).unwrap()).collect();
        assert!(receivers_details.iter().any(|details| (*details).agent_bit.count_ones() == 1));
        assert_eq!(receivers_details.len(), receivers.len());
        let agent_bits: HashSet<u64> = receivers_details.iter().map(|details| (*details).agent_bit).collect();
        assert_eq!(agent_bits.len(), receivers.len());

        assert!(matches!(broker.register(), Err(RegistryError::NoAvailablePosition)));
        // Unregister one agent and next registration should succeed
        assert!(receivers.get(0).unwrap().unregister().is_ok());
        assert!(broker.register().is_ok());
    }

    #[test]
    fn publish_notifies_subscribed_recipient() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        
        let receiver_listener = receiver_a.event.listen();

        let message = Message::new(MessageTopic::Sports, 74, 1).unwrap();
        let expected = message.clone();
        assert!(publisher.publish(message).is_ok());

        receiver_listener.wait();
        assert_eq!(receiver_a.read().unwrap(), expected);
    }

    #[test]
    fn publish_notifies_recipient_threaded() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        let receiver_for_waiter = receiver_a.clone();

        // use () as type since it's only a signal
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (woken_tx, woken_rx) = mpsc::channel::<()>();

        let waiter = thread::spawn(move || {
            let listener = receiver_for_waiter.event.listen();
            ready_tx.send(()).unwrap();
            listener.wait();
            woken_tx.send(()).unwrap();
        });

        ready_rx.recv().unwrap();

        let message = Message::new(MessageTopic::Sports, 2, 3).unwrap();
        let expected = message.clone();
        assert!(publisher.publish(message).is_ok());

        woken_rx.recv_timeout(Duration::from_secs(1)).expect("subscriber was not notified");
        waiter.join().unwrap();

        assert_eq!(receiver_a.read().unwrap(), expected);
    }

    #[test]
    fn publisher_is_not_notified_for_own_publication() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver_a = broker.register().unwrap();

        assert!(receiver_a.subscribe(MessageTopic::Sports).is_ok());
        assert!(publisher.subscribe(MessageTopic::Sports).is_ok());

        let publisher_for_waiter = publisher.clone();
        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (woken_tx, woken_rx) = mpsc::channel::<()>();

        let waiter = thread::spawn(move || {
            let listener = publisher_for_waiter.event.listen();
            ready_tx.send(()).unwrap();
            listener.wait();
            woken_tx.send(()).unwrap();
        });

        ready_rx.recv().unwrap();

        let message = Message::new(MessageTopic::Sports, 2, 3).unwrap();
        assert!(publisher.publish(message).is_ok());

        assert!(
            matches!(
                woken_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "publisher received its own notification"
        );

        // Teardown only - wake the test thread so it can exit
        publisher.event.notify(1);

        woken_rx.recv_timeout(Duration::from_secs(1)).expect("manual wake failed");
        waiter.join().unwrap();

    }

    #[test]
    fn concurrent_publishers_fan_out_to_every_subscriber_once() {
        const PUBLISHER_COUNT: usize = 3;
        const SUBSCRIBER_COUNT: usize = 4;
        const MESSAGES_PER_PUBLISHER: usize = 40;

        let broker = Broker::new(DefaultRingBuffer::new());
        let publishers: Vec<Agent> = (0..PUBLISHER_COUNT)
            .map(|_| broker.register().unwrap())
            .collect();
        let subscribers: Vec<Agent> = (0..SUBSCRIBER_COUNT)
            .map(|_| {
                let agent = broker.register().unwrap();
                agent.subscribe(MessageTopic::Sports).unwrap();
                agent
            })
            .collect();

        let expected_ids: HashSet<u64> = (0..PUBLISHER_COUNT)
            .flat_map(|publisher_index| {
                (0..MESSAGES_PER_PUBLISHER)
                    .map(move |message_index| (publisher_index * 1_000 + message_index + 1) as u64)
            })
            .collect();
        let start = Arc::new(Barrier::new(PUBLISHER_COUNT + SUBSCRIBER_COUNT));

        let reader_handles: Vec<_> = subscribers
            .into_iter()
            .map(|subscriber| {
                let expected_ids = expected_ids.clone();
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    read_expected_ids(subscriber, expected_ids)
                })
            })
            .collect();

        let writer_handles: Vec<_> = publishers
            .into_iter()
            .enumerate()
            .map(|(publisher_index, publisher)| {
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for message_index in 0..MESSAGES_PER_PUBLISHER {
                        let message_id = (publisher_index * 1_000 + message_index + 1) as u64;
                        let message = Message::new(MessageTopic::Sports, message_id, 1).unwrap();
                        publish_with_retry(&publisher, message);
                    }
                })
            })
            .collect();

        for handle in writer_handles {
            handle.join().unwrap();
        }
        for handle in reader_handles {
            assert_eq!(handle.join().unwrap(), expected_ids);
        }
    }

    #[test]
    fn concurrent_topic_routing_delivers_only_subscribed_topics() {
        const MESSAGES_PER_TOPIC: usize = 40;

        let broker = Broker::new(DefaultRingBuffer::new());
        let sports_publisher = broker.register().unwrap();
        let movies_publisher = broker.register().unwrap();
        let sports_only = broker.register().unwrap();
        let movies_only = broker.register().unwrap();
        let both_topics = broker.register().unwrap();

        sports_only.subscribe(MessageTopic::Sports).unwrap();
        movies_only.subscribe(MessageTopic::Movies).unwrap();
        both_topics.subscribe(MessageTopic::Sports).unwrap();
        both_topics.subscribe(MessageTopic::Movies).unwrap();

        let sports_ids: HashSet<u64> = (1..=MESSAGES_PER_TOPIC as u64).collect();
        let movie_ids: HashSet<u64> = (10_000..10_000 + MESSAGES_PER_TOPIC as u64).collect();
        let mut both_ids = sports_ids.clone();
        both_ids.extend(movie_ids.iter().copied());
        let start = Arc::new(Barrier::new(5));

        let sports_reader = {
            let start = Arc::clone(&start);
            let expected = sports_ids.clone();
            thread::spawn(move || {
                start.wait();
                read_expected_ids(sports_only, expected)
            })
        };
        let movies_reader = {
            let start = Arc::clone(&start);
            let expected = movie_ids.clone();
            thread::spawn(move || {
                start.wait();
                read_expected_ids(movies_only, expected)
            })
        };
        let combined_reader = {
            let start = Arc::clone(&start);
            let expected = both_ids.clone();
            thread::spawn(move || {
                start.wait();
                read_expected_ids(both_topics, expected)
            })
        };
        let sports_writer = {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                for message_id in 1..=MESSAGES_PER_TOPIC as u64 {
                    publish_with_retry(
                        &sports_publisher,
                        Message::new(MessageTopic::Sports, message_id, 1).unwrap(),
                    );
                }
            })
        };
        let movies_writer = {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                for message_id in 10_000..10_000 + MESSAGES_PER_TOPIC as u64 {
                    publish_with_retry(
                        &movies_publisher,
                        Message::new(MessageTopic::Movies, message_id, 1).unwrap(),
                    );
                }
            })
        };

        sports_writer.join().unwrap();
        movies_writer.join().unwrap();
        assert_eq!(sports_reader.join().unwrap(), sports_ids);
        assert_eq!(movies_reader.join().unwrap(), movie_ids);
        assert_eq!(combined_reader.join().unwrap(), both_ids);
    }

    #[test]
    fn pressure_retry_delivers_every_message_once_after_reader_starts() {
        const MESSAGE_COUNT: usize = DEFAULT_BUFFER_SIZE + 100;

        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver = broker.register().unwrap();
        receiver.subscribe(MessageTopic::Technology).unwrap();

        let (pressure_tx, pressure_rx) = mpsc::channel::<()>();
        let (resume_tx, resume_rx) = mpsc::channel::<()>();
        let writer = thread::spawn(move || {
            let mut reported_pressure = false;
            for message_id in 1..=MESSAGE_COUNT as u64 {
                let mut message = Message::new(MessageTopic::Technology, message_id, 1).unwrap();
                loop {
                    match publisher.publish(message) {
                        Ok(()) => break,
                        Err((RegistryError::BufferPressure, returned_message)) => {
                            message = returned_message;
                            if !reported_pressure {
                                pressure_tx.send(()).unwrap();
                                resume_rx.recv().unwrap();
                                reported_pressure = true;
                            }
                            thread::yield_now();
                        }
                        Err((error, _)) => panic!("unexpected publish error: {error:?}"),
                    }
                }
            }
        });

        pressure_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer never observed buffer pressure");
        resume_tx.send(()).unwrap();

        let expected_ids: HashSet<u64> = (1..=MESSAGE_COUNT as u64).collect();
        assert_eq!(read_expected_ids(receiver, expected_ids.clone()), expected_ids);
        writer.join().unwrap();
    }

    #[test]
    fn concurrent_registration_allocates_unique_bits_and_reuses_reclaimed_bit() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let start = Arc::new(Barrier::new(64));
        let registration_handles: Vec<_> = (0..64)
            .map(|_| {
                let broker = Arc::clone(&broker);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    broker.register().unwrap()
                })
            })
            .collect();

        let agents: Vec<Agent> = registration_handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let agent_bits: Vec<u64> = agents
            .iter()
            .map(|agent| broker.get_agent_details_from_registry(&agent.id).unwrap().agent_bit)
            .collect();
        assert!(agent_bits.iter().all(|bit| bit.count_ones() == 1));
        assert_eq!(agent_bits.iter().copied().collect::<HashSet<_>>().len(), 64);
        assert!(matches!(broker.register(), Err(RegistryError::NoAvailablePosition)));

        let reclaimed_bit = agent_bits[0];
        agents[0].unregister().unwrap();
        let replacement = broker.register().unwrap();
        let replacement_bit = broker
            .get_agent_details_from_registry(&replacement.id)
            .unwrap()
            .agent_bit;
        assert_eq!(replacement_bit, reclaimed_bit);
    }

    #[test]
    fn publish_and_unregister_race_leaves_capacity_usable() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let departing_receiver = broker.register().unwrap();
        departing_receiver.subscribe(MessageTopic::Pictures).unwrap();

        let start = Arc::new(Barrier::new(2));
        let publisher_for_thread = publisher.clone();
        let publishing = {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                publisher_for_thread.publish(Message::new(MessageTopic::Pictures, 1, 1).unwrap())
            })
        };
        let unregistering = thread::spawn(move || {
            start.wait();
            departing_receiver.unregister()
        });

        let publish_result = publishing.join().unwrap();
        match publish_result {
            Ok(()) | Err((RegistryError::NoSubscriberForTopic, _)) => {}
            Err((error, _)) => panic!("unexpected result while racing unregister: {error:?}"),
        }
        unregistering
            .join()
            .unwrap()
            .expect("unregister should complete after publish race");

        let replacement = broker.register().unwrap();
        replacement.subscribe(MessageTopic::Pictures).unwrap();
        let expected = Message::new(MessageTopic::Pictures, 2, 1).unwrap();
        publisher.publish(expected).unwrap();
        assert_eq!(replacement.read().unwrap(), expected);
    }

    #[test]
    fn read_and_unregister_race_delivers_at_most_one_message() {
        let broker = Broker::new(DefaultRingBuffer::new());
        let publisher = broker.register().unwrap();
        let receiver = broker.register().unwrap();
        receiver.subscribe(MessageTopic::Videos).unwrap();
        let expected = Message::new(MessageTopic::Videos, 1, 1).unwrap();
        publisher.publish(expected).unwrap();

        let start = Arc::new(Barrier::new(2));
        let receiver_for_read = receiver.clone();
        let reading = {
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                receiver_for_read.read()
            })
        };
        let unregistering = thread::spawn(move || {
            start.wait();
            receiver.unregister()
        });

        match reading.join().unwrap() {
            Ok(message) => assert_eq!(message, expected),
            Err(RegistryError::AgentDisconnecting | RegistryError::AgentNotRecognized) => {}
            Err(error) => panic!("unexpected read/unregister result: {error:?}"),
        }
        unregistering
            .join()
            .unwrap()
            .expect("unregister should complete after read race");

        let replacement = broker.register().unwrap();
        replacement.subscribe(MessageTopic::Videos).unwrap();
        let replacement_message = Message::new(MessageTopic::Videos, 2, 1).unwrap();
        publisher.publish(replacement_message).unwrap();
        assert_eq!(replacement.read().unwrap(), replacement_message);
    }
}
