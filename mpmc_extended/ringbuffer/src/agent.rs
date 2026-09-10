use std::{sync::Arc};
use uuid::Uuid;
use event_listener::{Event, Listener};
use crate::{broker::Broker, message::{Message, MessageTopic}, subscribe::RegistryError};

// Agents can both publish and consume - no need for explicit Subscriber struct
#[derive(Clone)]
pub struct Agent {
    pub id: Uuid,
    pub event: Arc<Event>,
    pub broker: Arc<Broker>,
}

impl Agent {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { 
            id: Uuid::new_v4(),
            event: Arc::new(Event::new()),
            broker,
        }
    }

    pub fn wait(&self) {
        let listener = self.event.listen();

        // Block until notification (notify()) comes in
        listener.wait();
    }

    pub fn read(&self) -> Result<Message, RegistryError> {
        self.broker.read(&self.id)
    }

    pub fn publish(&self, message: Message) -> Result<(), RegistryError> {
        self.broker.publish(&self.id, message)
    }

    pub fn subscribe(&self, topic: MessageTopic) -> Result<(), RegistryError> {
        self.broker.subscribe(&self.id, topic)
    }

    pub fn unsubscribe(&self, topic: MessageTopic) -> Result<(), RegistryError> {
        self.broker.unsubscribe(&self.id, topic)
    }

    pub fn unregister(&self) -> Result<(), RegistryError> {
        self.broker.unregister(&self.id)
    }
}