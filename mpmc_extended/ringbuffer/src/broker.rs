use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::{eprintln};
use event_listener::Event;
use uuid::Uuid;

use crate::message::{Message, MessageTopic};
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
    pub fn subscribe(&self, agent_id: &Uuid, topic: MessageTopic) -> Result<(), RegistryError> {
        let _guard = self.routing_gate.read().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        self.registry().subscribe(agent_id, topic)
    }

    /// Unsubscribe an agent from a topic
    pub fn unsubscribe(&self, agent_id: &Uuid, topic: MessageTopic) -> Result<(), RegistryError> {
        let _guard = self.routing_gate.read().unwrap();
        let details = self.get_agent_details_from_registry(agent_id)?;
        if *details.agent_state.lock().unwrap() != AgentState::Active {
            return Err(RegistryError::AgentDisconnecting);
        }
        self.registry().unsubscribe(agent_id, topic)
    }

    /// Main method for publishing to the ring. Everyone eventually gets here.
    /// Message doesn't have to be a reference because the buffer takes ownership anyways
    /// Return message on error (e.g. buffer full or no current recipient) so agent still keeps ownership
    pub fn publish(&self, agent_id: &Uuid, message: Message) -> Result<(), (RegistryError, Message)> {
        // Verify message is viable
        if message.topic == MessageTopic::Undefined {
            eprintln!("Message is unusable");
            return Err((RegistryError::InvalidMessage, message));
        }
        let routing_guard = self.routing_gate.read().unwrap();

        let snapshot = match self.make_publish_snapshot(agent_id, &message) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err((error, message));
            },
        };

        // don't write or disconnect normally if this agent is currently draining
        // keep the state so this agent can't unregister while still writing
        let state = snapshot.agent_details.agent_state.lock().unwrap();
        if *state != AgentState::Active {
            return Err((RegistryError::AgentDisconnecting, message));
        }

        match self.buffer.try_write(message, snapshot.reader_mask) {
            Ok(()) => {
                drop(state);
                drop(routing_guard);
                    
                // Notify all subscribers for this topic
                // TODO: Figure out if it's worth notifying the original publisher as well
                for subscriber_event in snapshot.reader_events {
                    subscriber_event.notify(1);
                }
            },
            Err((error, message)) => {
                return Err((self.map_buffer_error(error), message));
            }
        }
        Ok(())   
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
        let mut topic_reader_mask;
        let mut events: Vec<Arc<Event>> = Vec::new();
        let agent_details: Arc<AgentDetails>;

        if let Some(subscribers) = registry.get_subscribers_for(&message.topic) { 
            // Only write message to buffer if there's someone listening
            if subscribers.is_empty() {
                return Err(RegistryError::NoSubscriberForTopic);
            }
            topic_reader_mask = registry.create_mask_from_subscribers(&message.topic)?;
            // Remove publisher from reader mask - to prevent hanging notification - see below
            agent_details = registry.get_agent_details(agent_id)?;
            topic_reader_mask &= !agent_details.agent_bit;

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
        Ok(PublishSnapshot { reader_mask: topic_reader_mask, reader_events: events, agent_details: agent_details})
    }

    fn map_buffer_error(&self, error: RingBufferError) -> RegistryError {
        match error {
            RingBufferError::BufferEmpty => RegistryError::NoAvailableRead,
            RingBufferError::NoRecipient => RegistryError::NoSubscriberForTopic,
            RingBufferError::BufferFull => RegistryError::BufferPressure,
            RingBufferError::InvalidMetaData => RegistryError::InvalidMetaData
        }
    }

    pub fn current_write_cursor(&self) -> u64 {
        self.buffer.current_write_cursor()
    }

}