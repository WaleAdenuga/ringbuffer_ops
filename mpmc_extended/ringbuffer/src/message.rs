use serde::{Serialize, Deserialize};

const MAX_PAYLOAD_SIZE: usize = 300;

// transparent means this wrapper has the exact memory layout and ABI of its one non-zero-sized field
// essentially - treat it as one byte - no extra bytes, no hidden wrapper layout
#[repr(transparent)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone, Copy)]
pub struct TopicId(pub u8);

impl TopicId {
    pub fn new(value: u8) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u8 {
        self.0
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone, Copy)]
pub enum TopicVisibility {
    Public,
    Private,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone)]
pub struct TopicDescriptor {
    pub id: TopicId,
    pub name: String,
    pub schema_name: String,
    pub visibility: TopicVisibility,
    pub version: u32,
}

impl TopicDescriptor {
    pub fn new(id: TopicId, name: String, schema_name: String, visibility: TopicVisibility, version: u32) -> Self {
        Self { id, name, schema_name, visibility, version }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone, Copy)]
pub enum MessageKind {
    Data,
    TopicCreated,
    TopicDeleted,
    AccessControl,
}

// Payload that'll be added to the buffer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub topic_id: TopicId,
    pub kind: MessageKind,
    pub message_id: u64,
    pub version: u8,
    pub payload_len: usize,
    pub payload: [u8; MAX_PAYLOAD_SIZE] // should we define different payload types per message topic?
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageError {
    InvalidMetaData,
    InvalidPayload
}

impl Message {

    pub fn new(topic_id: TopicId, kind: MessageKind, message_id: u64, version: u8) -> Result<Self, MessageError> {  
        if message_id <= 0 || (version <= 0 || version > 3) {
            return Err(MessageError::InvalidMetaData);
        }
        
        Ok(Message { 
            topic_id, 
            kind,
            message_id, 
            version,
            payload_len: 0, 
            payload: [0; MAX_PAYLOAD_SIZE]
        })
    }

    /// Creates a message with topic ID 0, data message kind and default metadata
    pub fn empty() -> Result<Self, MessageError> {
        Ok(Self { 
            topic_id: TopicId(0),
            kind: MessageKind::Data, 
            message_id: u64::MAX, 
            version: 0, 
            payload_len: 0, 
            payload: [0;MAX_PAYLOAD_SIZE] 
        })
    }

    pub fn set_payload(&mut self, payload: Vec<u8>) -> Result<(), MessageError> {
        if payload.is_empty() || payload.len() > MAX_PAYLOAD_SIZE {
            return Err(MessageError::InvalidPayload);
        }
        // We can't trust that the vector is exactly three hundred bytes though
        let mut h_payload: [u8; MAX_PAYLOAD_SIZE] = [0; MAX_PAYLOAD_SIZE];
        h_payload[0..payload.len()].copy_from_slice(&payload);

        self.payload = h_payload;
        self.payload_len = payload.len(); // so we know how much to read
        Ok(())
    }

    pub fn set_topic(&mut self, topic: TopicId) {
        self.topic_id = topic
    }
}

// Testing
#[cfg(test)] // only compile when running tests
mod tests {
use std::{assert_eq, matches};

use super::*; // parent module

    #[test]
    fn new_message() {
        let message = Message::new(TopicId(1), MessageKind::Data, 5, 1);
        assert!(matches!(message, Ok(_)));
    }

    #[test]
    fn new_message_topic() {
        let message = Message::new(TopicId(1), MessageKind::TopicCreated, 5, 1);
        assert!(matches!(message, Ok(_)));
    }

    #[test]
    fn invalid_metadata() {
        let message = Message::new(TopicId(1), MessageKind::Data, 0, 1);
        assert!(matches!(message, Err(MessageError::InvalidMetaData)));
    }

    #[test]
    fn invalid_metadata_again() {
        let message = Message::new(TopicId(1), MessageKind::Data, 5, 5);
        assert!(matches!(message, Err(MessageError::InvalidMetaData)));
    }

    #[test]
    fn invalid_payload() {
        let message = Message::new(TopicId(1), MessageKind::Data, 2, 1);
        assert!(matches!(message, Ok(_)));
        assert!(matches!(message.unwrap().set_payload(Vec::new()), Err(MessageError::InvalidPayload))); 
    }

    #[test]
    fn valid_payload() {
        let mut message = Message::new(TopicId(1), MessageKind::Data, 2, 1);
        assert!(matches!(message, Ok(_)));
        let vector = [5; MAX_PAYLOAD_SIZE];
        assert!(matches!(message.as_mut().unwrap().set_payload(Vec::from(vector)), Ok(_)));

        assert_eq!(*vector.to_vec(), message.unwrap().payload); 
    }

    #[test]
    fn valid_payload_len() {
        let mut message = Message::new(TopicId(1), MessageKind::Data, 2, 1);
        assert!(matches!(message, Ok(_)));
        let vector = [5; 25];
        assert!(matches!(message.as_mut().unwrap().set_payload(Vec::from(vector)), Ok(_)));

        // Compare given payload to expected payload length
        assert_eq!(vector, message.as_ref().unwrap().payload[0..message.as_ref().unwrap().payload_len]); 
    }
}