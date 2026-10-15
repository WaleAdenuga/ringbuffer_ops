use std::{collections::{HashMap}, eprintln, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};
use uuid::Uuid;
use crate::message::{MessageTopic};
use event_listener::{Event};
use crate::agent::Agent;

#[derive(Debug)]
pub struct AgentDetails {
    pub read_cursor: AtomicU64,
    pub listener: Arc<Event>,
    pub agent_bit: u64,
    pub agent_state: Mutex<AgentState>, // created for lifecycle - and handling race conditions
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    Active,
    Draining,
    Removed,
}

impl AgentDetails {
    pub fn new(read_cursor: AtomicU64, listener: Arc<Event>, agent_bit: u64) -> Self {
        Self { 
            read_cursor, 
            listener,
            agent_bit,
            agent_state: AgentState::Active.into(),
        }
    }
}

#[derive(Debug)]
pub struct Registry {
    // map of all agents subscribed to a topic
    registry: HashMap<MessageTopic, Vec<Uuid>>,
    agents: HashMap<Uuid, Arc<AgentDetails>>, // Keep register of all agents

    // free/allocated bits
    // 1 - position is occupied
    // 0 - position is free
    allocated_bits: u64,
}

#[derive(Debug)]
pub enum RegistryError {
    InvalidMetaData,
    InvalidMessage,
    DualRegistration,
    DualTopicSubscription,
    PublishError,
    NoAvailableRead,
    DrainError,
    NoSubscriberForTopic,
    AgentNotRecognized,
    AgentNotSubscribed,
    AgentNotReachable,
    AgentDisconnecting,
    BufferPressure,

    NoAvailablePosition,
}

impl Registry {

    pub fn new() -> Self {
        Self { 
            registry: HashMap::new(), 
            agents: HashMap::new(),
            allocated_bits: 0,  // 
        }
    }

    /// Handle a request to subscribe to a topic
    pub fn subscribe(&mut self, agent_id: &Uuid, topic: MessageTopic) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        if topic == MessageTopic::Undefined {
            eprintln!("Please define a valid message topic");
            return Err(RegistryError::InvalidMetaData);
        }

        // Add to internal registry - But first prevent dual subscription
        if let Some(value) = self.registry.get(&topic) {
            if value.contains(agent_id) {
                return Err(RegistryError::DualTopicSubscription);
            }
        }
        // Add to internal registry if agent update is successful
        self.registry.entry(topic).or_insert_with(Vec::new).push(*agent_id);
        Ok(()) //agent_state is dropped
    }

    pub fn unsubscribe(&mut self, agent_id: &Uuid, topic: MessageTopic) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        if topic == MessageTopic::Undefined {
            eprintln!("Please define a valid message topic");
            return Err(RegistryError::InvalidMetaData);
        }

        // Remove from internal registry
        if let Some(subscribers) = self.registry.get_mut(&topic) {
            // retain removes all elements for which the function returns false
            subscribers.retain(|uuid| *uuid != *agent_id);
        }
        Ok(())
    }
    
    pub fn get_subscribers_for(&self, topic: &MessageTopic) -> Option<&Vec<Uuid>> {
        self.registry.get(topic)
    }

    /// Returns the list of agents subscribed to the given topic at that point in time
    pub fn create_mask_from_subscribers(&self, topic: &MessageTopic) -> Result<u64, RegistryError> {
        let mut topicmask: u64 = 0;
        // Get subscribers for given topic
        if let Some(subscribers) = self.registry.get(topic) {
            for subscriber in subscribers {
                // agent_bit returns that agent's index in the fixed u64 agent list right
                // topic mask or that index should give the position of all readers for this topic
                let Some(agent) = self.agents.get(subscriber) else {
                    return Err(RegistryError::AgentNotRecognized);
                };
                topicmask |= agent.agent_bit;
            }
        } else {
            return Err(RegistryError::NoSubscriberForTopic);
        }
        Ok(topicmask)
    }

    pub fn notify_subscriber(&self, subscriber_uuid: &Uuid) {
        // Get subscriber event
        let agent_details = self.agents.get(subscriber_uuid).unwrap();
        (*agent_details.listener).notify(1); // Only 1 listener per event so only 1 notification
    }

    pub fn agent_sanity(&self, id: &Uuid) -> bool {
        if self.agents.contains_key(id) {
            return true;
        }
        return false
    }

    pub fn register(&mut self, agent: &Agent) -> Result<(), RegistryError> {
        // Ignore duplicate entries
        if self.agents.contains_key(&agent.id) {
            return Err(RegistryError::DualRegistration);
        }

        let agent_bit = self.allocate_bit()?;
        let agent_cursor = AtomicU64::new(agent.broker.current_write_cursor());
        let agent_details = Arc::new(AgentDetails::new(agent_cursor,agent.clone().event, agent_bit));
        self.agents.insert(agent.id, agent_details);
        Ok(())
    }

    /// Allocate a bit when an agent registers
    /// Finds first available position
    fn allocate_bit(&mut self) -> Result<u64, RegistryError> {
        if self.allocated_bits == u64::MAX {
            return Err(RegistryError::NoAvailablePosition);
        }

        // First 0 bit, and make that position this agent's mask
        let bit_index = self.allocated_bits.trailing_ones();
        let bit = 1u64 << bit_index;

        // Take bit
        self.allocated_bits |= bit;
        Ok(bit)
    }

    pub fn start_unregister(&mut self, agent_id: &Uuid) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        // Remove from internal mappings
        for (_, value) in self.registry.iter_mut() {
            // retain removes all elements for which the function returns false
            value.retain(|uuid| *uuid != *agent_id); 
        }
        Ok(())
    }

    pub fn finish_unregister(&mut self, agent_id: &Uuid) -> Result<(), RegistryError> {
        // remove returns the value that was present at the key
        let details = self.agents.remove(agent_id).ok_or(RegistryError::AgentNotRecognized)?;
        self.deallocate_bit(details.agent_bit);
        Ok(())
    }

    /// Deallocate bit when agent de-registers
    /// Flip bit and make it available for next agent
    /// Can't just deallocate in case someone else takes bit ownership - has to be called by the safeguard maybe?
    /// Leave dead agent bits until the safeguard clears them?
    fn deallocate_bit(&mut self, bit: u64) {
        self.allocated_bits &= !bit;
    }

    pub fn get_agent_details(&self, agent_id: &Uuid) -> Result<Arc<AgentDetails>, RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        
        // shouldn't panic since we check sanity above
        Ok(self.agents.get(agent_id).unwrap().clone())
    }

    pub fn update_read_cursor(&mut self, agent_id: &Uuid, new_cursor: u64) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        if let Some(details) = self.agents.get_mut(agent_id) {
            details.read_cursor.store(new_cursor, Ordering::Release);
        }

        Ok(())
    }

}