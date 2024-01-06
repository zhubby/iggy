use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::topics::MAX_NAME_LENGTH;
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

/// `UpdateTopic` command is used to update a topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `message_expiry_secs` - optional message expiry in seconds, if `None` then messages will never expire.
/// - `max_topic_size_bytes` - optional maximum size of the topic in bytes, if `None` then topic size is unlimited.
///                            Can't be lower than segment size in the config.
/// - `replication_factor` - replication factor for the topic.
/// - `name` - unique topic name, max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct UpdateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Message expiry in seconds (optional), if `None` then messages will never expire.
    pub message_expiry_secs: Option<u32>,
    /// Max topic size in bytes (optional), if `None` then topic size is unlimited.
    /// Can't be lower than segment size in the config.
    pub max_topic_size_bytes: Option<u64>,
    /// Replication factor for the topic.
    pub replication_factor: u8,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
}

impl CommandPayload for UpdateTopic {}

impl Default for UpdateTopic {
    fn default() -> Self {
        UpdateTopic {
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            message_expiry_secs: None,
            max_topic_size_bytes: None,
            replication_factor: 1,
            name: "topic".to_string(),
        }
    }
}

impl Validatable<Error> for UpdateTopic {
    fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidTopicName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidTopicName);
        }

        if self.replication_factor == 0 {
            return Err(Error::InvalidReplicationFactor);
        }

        Ok(())
    }
}

impl FromStr for UpdateTopic {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 6 {
            return Err(Error::InvalidCommand);
        }
        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<Identifier>()?;
        let message_expiry_secs = match parts[2].parse::<u32>()? {
            0 => None,
            size => Some(size),
        };
        let max_topic_size_bytes = match parts[3].parse::<u64>()? {
            0 => None,
            size => Some(size),
        };
        let replication_factor = parts[4].parse::<u8>()?;
        let name = parts[5].to_string();
        let command = UpdateTopic {
            stream_id,
            topic_id,
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for UpdateTopic {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let topic_id_bytes = self.topic_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(13 + stream_id_bytes.len() + topic_id_bytes.len() + self.name.len());
        bytes.extend(stream_id_bytes.clone());
        bytes.extend(topic_id_bytes.clone());
        match self.message_expiry_secs {
            Some(message_expiry_secs) => bytes.put_u32_le(message_expiry_secs),
            None => bytes.put_u32_le(0),
        }
        match self.max_topic_size_bytes {
            Some(max_topic_size_bytes) => bytes.put_u64_le(max_topic_size_bytes),
            None => bytes.put_u64_le(0),
        }
        bytes.put_u8(self.replication_factor);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.name.len() as u8);
        bytes.extend(self.name.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<UpdateTopic, Error> {
        if bytes.len() < 12 {
            return Err(Error::InvalidCommand);
        }
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..])?;
        position += topic_id.get_size_bytes() as usize;
        let message_expiry_secs = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let message_expiry_secs = match message_expiry_secs {
            0 => None,
            _ => Some(message_expiry_secs),
        };
        let max_topic_size_bytes =
            u64::from_le_bytes(bytes[position + 4..position + 12].try_into()?);
        let max_topic_size_bytes = match max_topic_size_bytes {
            0 => None,
            _ => Some(max_topic_size_bytes),
        };
        let replication_factor = bytes[position + 12];
        let name_length = bytes[position + 13];
        let name =
            from_utf8(&bytes[position + 14..(position + 14 + name_length as usize)])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }
        let command = UpdateTopic {
            stream_id,
            topic_id,
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for UpdateTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.message_expiry_secs.unwrap_or(0),
            self.max_topic_size_bytes.unwrap_or(0),
            self.replication_factor,
            self.name,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = UpdateTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            message_expiry_secs: Some(10),
            max_topic_size_bytes: Some(100),
            replication_factor: 1,
            name: "test".to_string(),
        };

        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = Identifier::from_bytes(&bytes[position..]).unwrap();
        position += topic_id.get_size_bytes() as usize;
        let message_expiry_secs =
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let message_expiry_secs = match message_expiry_secs {
            0 => None,
            _ => Some(message_expiry_secs),
        };
        let max_topic_size_bytes =
            u64::from_le_bytes(bytes[position + 4..position + 12].try_into().unwrap());
        let max_topic_size_bytes = match max_topic_size_bytes {
            0 => None,
            _ => Some(max_topic_size_bytes),
        };
        let replication_factor = bytes[position + 12];
        let name_length = bytes[position + 13];
        let name = from_utf8(&bytes[position + 14..position + 14 + name_length as usize])
            .unwrap()
            .to_string();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(message_expiry_secs, command.message_expiry_secs);
        assert_eq!(max_topic_size_bytes, command.max_topic_size_bytes);
        assert_eq!(replication_factor, command.replication_factor);
        assert_eq!(name.len() as u8, command.name.len() as u8);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let name = "test".to_string();
        let message_expiry_secs = 10;
        let max_topic_size_bytes = 100;
        let replication_factor = 1;

        let stream_id_bytes = stream_id.as_bytes();
        let topic_id_bytes = topic_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(5 + stream_id_bytes.len() + topic_id_bytes.len() + name.len());
        bytes.extend(stream_id_bytes);
        bytes.extend(topic_id_bytes);
        bytes.put_u32_le(message_expiry_secs);
        bytes.put_u64_le(max_topic_size_bytes);
        bytes.put_u8(replication_factor);

        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());

        let command = UpdateTopic::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.message_expiry_secs, Some(message_expiry_secs));
        assert_eq!(command.max_topic_size_bytes, Some(max_topic_size_bytes));
        assert_eq!(command.replication_factor, replication_factor);
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let message_expiry_secs = 10;
        let max_topic_size_bytes = 100;
        let replication_factor = 1;
        let name = "test".to_string();
        let input = format!(
            "{}|{}|{}|{}|{}|{}",
            stream_id,
            topic_id,
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
            name
        );
        let command = UpdateTopic::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.message_expiry_secs, Some(message_expiry_secs));
        assert_eq!(command.max_topic_size_bytes, Some(max_topic_size_bytes));
        assert_eq!(command.replication_factor, replication_factor);
        assert_eq!(command.name, name);
    }
}
