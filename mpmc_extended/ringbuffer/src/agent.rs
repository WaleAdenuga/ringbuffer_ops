use std::{sync::Arc, time::Duration};
use uuid::Uuid;
use event_listener::{Event, Listener};
use crate::{broker::Broker, message::{Message, TopicDescriptor, TopicKey, TopicVisibility}, registry::RegistryError};

// Agents can both publish and consume - no need for explicit Subscriber struct
pub struct Agent {
    pub(crate) id: Uuid,
    pub(crate) event: Arc<Event>,
    pub(crate) journal_event: Arc<Event>,
    pub(crate) broker: Arc<Broker>,
}

impl Agent {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { 
            id: Uuid::new_v4(),
            event: Arc::new(Event::new()),
            journal_event: Arc::new(Event::new()),
            broker,
        }
    }

    /// Blocking - waits until one message is available
    pub fn recv(&mut self) -> Result<Message, RegistryError> {
        loop {
            // Read first to see if there's already a message for you
            match self.read() {
                Ok(message) => return Ok(message),
                Err(RegistryError::NoAvailableRead) => {},
                Err(error) => return Err(error),
            }

            let listener = self.event.listen();

            // Read after listener creation to see if there's something we might have missed
            // wait will unblock when notification comes in - loop will try read again and return the message in first block
            match self.read() {
                Ok(message) =>  return Ok(message),
                Err(RegistryError::NoAvailableRead) => {
                    // have a timeout to not permanently block reading - in case of a producer hole
                    // i.e. write A claims slot 0 but descheduled before write, 
                    // writer B writes slot 1, reader gets alert
                    // reader sees slot 0 empty, assumes buffer is empty and returns early
                    // and then would block permanently
                    listener.wait_timeout(Duration::from_millis(30)); 
                },
                Err(error) => return Err(error),
            }
        }
    }

    /// Blocking - waits until one message is available
    pub fn recv_journal(&mut self) -> Result<Vec<Message>, RegistryError> {
        loop {
            // Read first to see if there's already a message for you
            match self.recover_from_journal() {
                Ok(message) => return Ok(message),
                Err(RegistryError::NoAvailableRead) => {},
                Err(error) => return Err(error),
            }

            let listener = self.journal_event.listen();

            // Read after listener creation to see if there's something we might have missed
            // wait will unblock when notification comes in - loop will try read again and return the message in first block
            match self.recover_from_journal() {
                Ok(message) =>  return Ok(message),
                Err(RegistryError::NoAvailableRead) => listener.wait(),
                Err(error) => return Err(error),
            }
        }
    }

    /// Non-blocking: one message if available
    pub fn read(&mut self) -> Result<Message, RegistryError> {
        self.broker.read(&self.id)
    }

    /// Non-blocking: returns however many messages for this agent are currently journaled
    /// This function should normally only be called when the journal listener is signalled
    pub fn recover_from_journal(&mut self) -> Result<Vec<Message>, RegistryError> {
        self.broker.read_from_journal(&self.id)
    }

    pub fn publish(&self, message: Message) -> Result<(), (RegistryError, Message)> {
        self.broker.publish(&self.id, message)
    }

    pub fn subscribe(&self, topic_key: TopicKey) -> Result<(), RegistryError> {
        self.broker.subscribe(&self.id, topic_key)
    }

    pub fn unsubscribe(&self, topic_key: TopicKey) -> Result<(), RegistryError> {
        self.broker.unsubscribe(&self.id, topic_key)
    }

    pub fn get_all_topics(&self) -> Result<Vec<TopicDescriptor>, RegistryError> {
        self.broker.get_all_topics(&self.id)
    }

    pub fn find_topic(&self, topic_key: TopicKey) -> Result<TopicDescriptor, RegistryError> {
        self.broker.find_topic(&self.id, &topic_key)
    }

    /// TopicDescriptor returns a key that can be used to used in future operations: subscription, publishing, etc
    pub fn create_topic(&self, name: String, schema: String, visibility: TopicVisibility, version: u32) -> Result<TopicDescriptor, RegistryError> {
        self.broker.create_topic(&self.id, name, schema, &visibility, &version)
    }

    pub fn delete_topic(&self, topic_key: TopicKey) -> Result<(), RegistryError> {
        self.broker.delete_topic(&self.id, topic_key)
    }

    pub fn set_topic_visibility(&self, topic_key: TopicKey, visibility: TopicVisibility) -> Result<TopicDescriptor, RegistryError> {
        self.broker.set_topic_visibility(&self.id, &topic_key, &visibility)
    }

    pub fn unregister(&self) -> Result<(), RegistryError> {
        self.broker.unregister(&self.id)
    }
}