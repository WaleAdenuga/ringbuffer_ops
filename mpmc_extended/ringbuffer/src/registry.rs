use std::{collections::{HashMap, HashSet}, eprintln, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};
use uuid::Uuid;
use event_listener::{Event};
use crate::{agent::Agent, message::{TopicDescriptor, TopicId, TopicVisibility}};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRecord {
    pub descriptor: TopicDescriptor,
    pub creator_id: Uuid,
    pub subscribers: Vec<Uuid>, // receive notifications and can publish if topic is public

    //V2 - add allowed_agents for private visibility handling
}

impl TopicRecord {
    /// Only to be used when creating a topic for the first time
    pub fn new(descriptor: TopicDescriptor, agent_id: &Uuid) -> Self {
        Self { 
            descriptor, 
            creator_id: *agent_id, 
            subscribers: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct Registry {
    // map of all agents subscribed to a topic
    registry: HashMap<TopicId, TopicRecord>,
    agents: HashMap<Uuid, Arc<AgentDetails>>, // Keep register of all agents -> Arc because broker needs to clone details while its registry lock is dropped
    routable_agents: HashSet<Uuid>, // agents that can be spoken to. The guarantee is they're all active 

    // free/allocated bits
    // 1 - position is occupied
    // 0 - position is free
    allocated_agent_bits: u64,
    allocated_topic_ids: [bool; 256], // no need to make atomic, only one registry per broker
}

#[derive(Debug, PartialEq, Eq)]
pub enum RegistryError {
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
    TopicNotAvailable,
    TopicNotRecognized,
    TopicCapacityReached,
    DualTopicCreation,
    PermissionDenied,
    MessageTypeNotSupported,
    NoAgentsAvailable,
}

impl Registry {

    pub fn new() -> Self {
        Self { 
            registry: HashMap::new(), 
            agents: HashMap::new(),
            routable_agents: HashSet::new(),
            allocated_agent_bits: 0,
            allocated_topic_ids: [false; 256],
        }
    }

    /// Check if topic is recognized
    pub fn topic_sanity(&self, topic_id: TopicId) -> bool {
        if let Some(_) = self.registry.get(&topic_id) {
            return true;
        } else {
            return false;
        }
    }

    /// Returns listeners for all routable agents except the one provided in agent_id
    pub fn return_all_agent_listeners(&self, agent_bit: u64) -> Vec<Arc<Event>> {
        self.routable_agents.iter()
            .map(|uuid| self.get_agent_details(uuid).unwrap())
            .filter(|details| details.agent_bit != agent_bit)
            .map(|details| details.listener.clone())
            .collect()
    }

    /// For broadcast reasons and topic creation - Notify all routable agents
    pub fn create_mask_for_all_agents(&self) -> u64 {
        let mut topicmask: u64 = 0;

        let agent_bits: Vec<u64> = self.routable_agents.iter()
            .map(|uuid| self.get_agent_details(uuid).unwrap().agent_bit)
            .collect();
        
        for bit in agent_bits {
            topicmask |= bit;
        }

        topicmask
    }

    /// Handle a request to subscribe to a topic
    /// You can only subscribe to a public topic (To enforce in V2)
    pub fn subscribe(&mut self, agent_id: &Uuid, topic_id: TopicId) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        if !self.topic_sanity(topic_id) {
            eprintln!("Please subscribe to an existing topic");
            return Err(RegistryError::TopicNotAvailable);
        }

        // Add to internal registry - But first prevent dual subscription
        // Topic already exists before it can be updates
        if let Some(value) = self.registry.get_mut(&topic_id) {
            if value.subscribers.contains(agent_id) {
                return Err(RegistryError::DualTopicSubscription);
            }
            value.subscribers.push(*agent_id);
        }
        Ok(()) //agent_state is dropped
    }

    pub fn unsubscribe(&mut self, agent_id: &Uuid, topic_id: TopicId) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        if !self.topic_sanity(topic_id) {
            eprintln!("Please subscribe to an existing topic");
            return Err(RegistryError::TopicNotAvailable);
        }

        // Remove from internal registry
        if let Some(record) = self.registry.get_mut(&topic_id) {
            // retain removes all elements for which the function returns false
            record.subscribers.retain(|uuid| *uuid != *agent_id);
        }
        Ok(())
    }

    /// Returns all public topics
    pub fn all_topics(&self, agent_id: &Uuid) -> Result<Vec<TopicDescriptor>, RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        Ok(
            self.registry.values()
                .map(|record| record.descriptor.clone())
                .filter(|descriptor| descriptor.visibility == TopicVisibility::Public)
                .collect()
        )
    }

    /// Returns a topic descriptor if their id is public
    pub fn find_topic(&self, agent_id: &Uuid, topic_id: &TopicId) -> Result<TopicDescriptor, RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }

        if let Some(record) = self.registry.get(topic_id) {
            if record.descriptor.visibility == TopicVisibility::Public {
                return Ok(record.descriptor.clone());
            }
        }
        Err(RegistryError::TopicNotRecognized)
    }

    pub fn create_topic(&mut self, agent_id: &Uuid, name: String, schema: String, visibility: &TopicVisibility, version: &u32) -> Result<TopicDescriptor, RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }

        // Add to internal registry and allocate unique topic id
        let topic_id = self.allocate_topic_id()?;
        let descriptor = TopicDescriptor::new(topic_id, name, schema, *visibility, *version);
        let record = TopicRecord::new(descriptor.clone(), agent_id);
        self.registry.insert(topic_id, record);

        Ok(descriptor.clone())
    }

    /// Allocate a given topic id based on first free index
    fn allocate_topic_id(&mut self) -> Result<TopicId, RegistryError> {
        // returns index of the first time it sees a false. false implies that the position is free
        let index = self.allocated_topic_ids
            .iter()
            .position(|allocated| !allocated)
            .ok_or(RegistryError::TopicCapacityReached)?;

        self.allocated_topic_ids[index] = true;
        Ok(TopicId(index as u8))
    }

    /// Deallocate id assigned to given topic
    fn release_topic_id(&mut self, topic_id: &TopicId) {
        self.allocated_topic_ids[topic_id.value() as usize] = false;
    }

    /// Deletes a topic - can only be done by the creator
    pub fn abort_topic_creation(&mut self, agent_id: &Uuid, topic_id: &TopicId) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        // Remove from internal registry
        if let Some(record) = self.registry.get(&topic_id) {
            if *agent_id != record.creator_id {
                return Err(RegistryError::PermissionDenied);
            }
        }
        // release assigned topic id
        self.registry.remove(topic_id).ok_or(RegistryError::TopicNotRecognized)?;
        self.release_topic_id(topic_id);
        Ok(())
    }
    
    pub fn get_subscribers_for(&self, topic_id: &TopicId) -> Option<&Vec<Uuid>> {
        if let Some(record) = self.registry.get(topic_id) {
            return Some(&record.subscribers);
        }
        None
    }

    /// Returns the list of agents subscribed to the given topic at that point in time
    pub fn create_mask_from_subscribers(&self, topic: &TopicId) -> Result<u64, RegistryError> {
        let mut topicmask: u64 = 0;
        // Get subscribers for given topic
        if let Some(record) = self.registry.get(topic) {
            for subscriber in &record.subscribers {
                // agent_bit returns that agent's index in the fixed u64 agent list right
                // topic mask or that index should give the position of all readers for this topic
                let Some(agent) = self.agents.get(&subscriber) else {
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
        self.routable_agents.insert(agent.id);
        Ok(())
    }

    /// Allocate a bit when an agent registers
    /// Finds first available position
    fn allocate_bit(&mut self) -> Result<u64, RegistryError> {
        if self.allocated_agent_bits == u64::MAX {
            return Err(RegistryError::NoAvailablePosition);
        }

        // First 0 bit, and make that position this agent's mask
        let bit_index = self.allocated_agent_bits.trailing_ones();
        let bit = 1u64 << bit_index;

        // Take bit
        self.allocated_agent_bits |= bit;
        Ok(bit)
    }

    /// Remove agent from future topic registry and routable options
    pub fn start_unregister(&mut self, agent_id: &Uuid) -> Result<(), RegistryError> {
        if !self.agent_sanity(agent_id) {
            eprintln!("Agent is not recognized. Are you registered?");
            return Err(RegistryError::AgentNotRecognized);
        }
        // Remove from internal mappings
        for (_, record) in self.registry.iter_mut() {
            // retain removes all elements for which the function returns false
            record.subscribers.retain(|uuid| *uuid != *agent_id); 
        }
        // Remove from routable - so that it doesn't receive any other notifications
        self.routable_agents.remove(agent_id);
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
    /// Can't just deallocate in case someone else takes bit creator_idship - has to be called by the safeguard maybe?
    /// Leave dead agent bits until the safeguard clears them?
    fn deallocate_bit(&mut self, bit: u64) {
        self.allocated_agent_bits &= !bit;
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

    pub fn return_owner_for_topic(&self, topic_id: &TopicId) -> Option<&Uuid> {
        if !self.topic_sanity(*topic_id) {
            eprintln!("Please subscribe to an existing topic");
            return None;
        }
        Some(&self.registry.get(topic_id).unwrap().creator_id)
    }

}