use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{eprintln};
use uuid::Uuid;

use crate::message::{Message, MessageTopic};
use crate::buffer::RingBuffer;
use crate::subscribe::{Registry, RegistryError};
use crate::agent::Agent;

// Broker - to deal with Buffer operations and the subscription registry
// It owns handles to both (Arcs/shared pointers)
// Agents go through the broker for publishing/writing while the broker handles the notification stream
// Agent Registration also goes through the broker
pub struct Broker {
    buffer: RingBuffer<Message>,
    registry: Mutex<Registry>, // so registry can be updated when agents subscribe/unsubscribe to topics
}

impl Broker {

    pub fn new(buffer: RingBuffer<Message>) -> Arc<Self> {
        Arc::new(Self { 
            buffer, 
            registry: Registry::new().into(),
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
        self.registry().unregister(agent_id)
    }

    /// Subscribe an agent to a topic
    pub fn subscribe(&self, agent_id: &Uuid, topic: MessageTopic) -> Result<(), RegistryError> {
        self.registry().subscribe(agent_id, topic)
    }

    /// Unsubscribe an agent from a topic
    pub fn unsubscribe(&self, agent_id: &Uuid, topic: MessageTopic) -> Result<(), RegistryError> {
        self.registry().unsubscribe(agent_id, topic)
    }

    /// Main method for publishing to the ring. Everyone eventually gets here.
    /// Message doesn't have to be a reference because the buffer takes ownership anywats
    pub fn publish(&self, agent_id: &Uuid, message: Message) -> Result<(), RegistryError> {
        // Keep mutex alive for the duration of the function
        let registry = self.registry();
        if !registry.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        
        // Verify message is viable
        if message.topic == MessageTopic::Undefined {
            eprintln!("Message is unusable");
            return Err(RegistryError::InvalidMessage);
        }

        if let Some(subscribers) = registry.get_subscribers_for(&message.topic) { 
            // Only write message to buffer if there's someone listening
            if self.buffer.try_write(message, subscribers.len()).is_err() {
                return Err(RegistryError::PublishError);
            };
            
            // Notify all subscribers for this topic
            // TODO: Figure out if it's worth notifying the original publisher as well
            for subscriber_uuid in subscribers {
                if *subscriber_uuid != *agent_id {
                    registry.notify_subscriber(subscriber_uuid);
                }
            }
        } else {
            return Err(RegistryError::NoSubscriberForTopic);
        }
        
        Ok(())   
    }

    pub fn read(&self, agent_id: &Uuid) -> Result<Message, RegistryError> {
        // Keep mutex alive for the duration of the function
        let registry = self.registry();
        if !registry.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        
        // keep looping until we get the message we desire
        // right now it's a sequential read, and read cursors differ
        loop {
            // Get agent details including read_cursor - shouldn't panic because agent exists
            // in loop because read_cursor should change if current slot is useless
            let details = registry.get_agent_details(agent_id).unwrap();
            let current_cursor = details.read_cursor.load(Ordering::Acquire);
            if let Ok(message) = self.buffer.try_read(&details.read_cursor) {                
                if details.subscriptions.contains(&message.topic) {
                    // slot is readable, acknowledge
                    let message_clone = message.clone();
                    if self.buffer.update_after_read(current_cursor).is_ok() {
                        // Update read_cursor
                        details.read_cursor.store(current_cursor + 1, Ordering::Release);
                        return Ok(message_clone);
                    } else {
                        return Err(RegistryError::ReadError);
                    }
                }
                // Update read_cursor regardless of subscription
                details.read_cursor.store(current_cursor + 1, Ordering::Release);
                continue; // try to read again
            } else {
                return Err(RegistryError::ReadError);
            } 
        }
    }

    /// So I don't have to type lock.unwrap a thousand times
    /// '_ means "Rust, infer the lifetime here" --> guard lives as long as the mutex borrow
    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap()
    }

}