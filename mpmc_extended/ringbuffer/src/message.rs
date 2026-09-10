use std::{println};
use serde::{Serialize, Deserialize};

const MAX_PAYLOAD_SIZE: usize = 300;

// Possible message topics for agents to subscribe to
#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Eq, Clone, Copy)]
pub enum MessageTopic {
    Personalization,
    Pictures,
    Videos,
    Shopping,
    Movies,
    Sports,
    Technology,
    Undefined
}

// Payload that'll be added to the buffer
#[derive(Debug, Clone, Copy)]
pub struct Message {
    pub topic: MessageTopic,
    pub message_id: u64,
    pub version: u8,
    pub payload_len: usize,
    pub payload: [u8; MAX_PAYLOAD_SIZE] // should we define different payload types per message topic?
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MessageError {
    InvalidTopic,
    InvalidMetaData,
    InvalidPayload
}

impl Message {

    pub fn new(topic: MessageTopic, message_id: u64, version: u8) -> Result<Self, MessageError> {  
        
        if topic == MessageTopic::Undefined {
            println!("Please provide a valid topic");
            return Err(MessageError::InvalidTopic);   
        }

        if message_id <= 0 || (version <= 0 || version > 3) {
            return Err(MessageError::InvalidMetaData);
        }
        
        Ok(Message { 
            topic, 
            message_id, 
            version,
            payload_len: 0, 
            payload: [0; MAX_PAYLOAD_SIZE]
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

    pub fn set_topic(&mut self, topic: MessageTopic) -> Result<(), MessageError> {
        if topic == MessageTopic::Undefined {
            println!("Warning: Setting topic as Undefined");
        } 
        self.topic = topic;
        Ok(())
    }
}

// Testing
#[cfg(test)] // only compile when running tests
mod tests {
use std::{assert_eq, matches};

use super::*; // parent module

    #[test]
    fn new_message() {
        let message = Message::new(MessageTopic::Pictures, 5, 1);
        assert!(matches!(message, Ok(_)));
    }

    #[test]
    fn invalid_topic() {
        let message = Message::new(MessageTopic::Undefined, 2, 1);
        assert!(matches!(message, Err(MessageError::InvalidTopic)));
    }

    #[test]
    fn invalid_metadata() {
        let message = Message::new(MessageTopic::Videos, 0, 1);
        assert!(matches!(message, Err(MessageError::InvalidMetaData)));
    }

    #[test]
    fn invalid_metadata_again() {
        let message = Message::new(MessageTopic::Videos, 5, 5);
        assert!(matches!(message, Err(MessageError::InvalidMetaData)));
    }

    #[test]
    fn invalid_payload() {
        let message = Message::new(MessageTopic::Videos, 2, 1);
        assert!(matches!(message, Ok(_)));

        assert!(matches!(message.unwrap().set_payload(Vec::new()), Err(MessageError::InvalidPayload))); 
    }

    #[test]
    fn valid_payload() {
        let mut message = Message::new(MessageTopic::Videos, 2, 1);
        assert!(matches!(message, Ok(_)));
        let vector = [5; MAX_PAYLOAD_SIZE];
        assert!(matches!(message.as_mut().unwrap().set_payload(Vec::from(vector)), Ok(_)));

        assert_eq!(*vector.to_vec(), message.unwrap().payload); 
    }

    #[test]
    fn valid_payload_len() {
        let mut message = Message::new(MessageTopic::Videos, 2, 1);
        assert!(matches!(message, Ok(_)));
        let vector = [5; 25];
        assert!(matches!(message.as_mut().unwrap().set_payload(Vec::from(vector)), Ok(_)));

        // Compare given payload to expected payload length
        assert_eq!(vector, message.as_ref().unwrap().payload[0..message.as_ref().unwrap().payload_len]); 
    }
}