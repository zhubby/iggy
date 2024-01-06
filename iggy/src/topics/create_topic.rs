use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::Error;
use crate::identifier::Identifier;
use crate::topics::{MAX_NAME_LENGTH, MAX_PARTITIONS_COUNT};
use crate::utils::text;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::{from_utf8, FromStr};

/// `CreateTopic` command is used to create a new topic in a stream.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric).
/// - `partitions_count` - number of partitions in the topic, max value is 1000.
/// - `message_expiry_secs` - optional message expiry in seconds, if `None` then messages will never expire.
/// - `max_topic_size_bytes` - optional maximum size of the topic in bytes, if `None` then topic size is unlimited.
///                            Can't be lower than segment size in the config.
/// - `replication_factor` - replication factor for the topic.
/// - `name` - unique topic name, max length is 255 characters.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateTopic {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric).
    pub topic_id: u32,
    /// Number of partitions in the topic, max value is 1000.
    pub partitions_count: u32,
    /// Message expiry in seconds (optional), if `None` then messages will never expire.
    pub message_expiry_secs: Option<u32>,
    /// The optional maximum size of the topic in bytes.
    pub max_topic_size_bytes: Option<u64>,
    /// Replication factor for the topic.
    pub replication_factor: u8,
    /// Unique topic name, max length is 255 characters.
    pub name: String,
}

impl CommandPayload for CreateTopic {}

impl Default for CreateTopic {
    fn default() -> Self {
        CreateTopic {
            stream_id: Identifier::default(),
            topic_id: 1,
            partitions_count: 1,
            message_expiry_secs: None,
            max_topic_size_bytes: None,
            replication_factor: 1,
            name: "topic".to_string(),
        }
    }
}

impl Validatable<Error> for CreateTopic {
    fn validate(&self) -> Result<(), Error> {
        if self.topic_id == 0 {
            return Err(Error::InvalidTopicId);
        }

        if self.name.is_empty() || self.name.len() > MAX_NAME_LENGTH {
            return Err(Error::InvalidTopicName);
        }

        if !text::is_resource_name_valid(&self.name) {
            return Err(Error::InvalidTopicName);
        }

        if !(0..=MAX_PARTITIONS_COUNT).contains(&self.partitions_count) {
            return Err(Error::TooManyPartitions);
        }

        if self.replication_factor == 0 {
            return Err(Error::InvalidReplicationFactor);
        }

        Ok(())
    }
}

impl FromStr for CreateTopic {
    type Err = Error;
    fn from_str(input: &str) -> std::result::Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 7 {
            return Err(Error::InvalidCommand);
        }

        let stream_id = parts[0].parse::<Identifier>()?;
        let topic_id = parts[1].parse::<u32>()?;
        let partitions_count = parts[2].parse::<u32>()?;
        let message_expiry_secs = match parts[3].parse::<u32>().ok() {
            Some(message_expiry_secs) => match message_expiry_secs {
                0 => None,
                _ => Some(message_expiry_secs),
            },
            None => None,
        };
        let max_topic_size_bytes = match parts[4].parse::<u64>().ok() {
            Some(max_topic_size_bytes) => match max_topic_size_bytes {
                0 => None,
                _ => Some(max_topic_size_bytes),
            },
            None => None,
        };
        let replication_factor = parts[5].parse::<u8>()?;
        let name = parts[6].to_string();
        let command = CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl BytesSerializable for CreateTopic {
    fn as_bytes(&self) -> Vec<u8> {
        let stream_id_bytes = self.stream_id.as_bytes();
        let mut bytes = Vec::with_capacity(22 + stream_id_bytes.len() + self.name.len());
        bytes.extend(stream_id_bytes);
        bytes.put_u32_le(self.topic_id);
        bytes.put_u32_le(self.partitions_count);
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

    fn from_bytes(bytes: &[u8]) -> std::result::Result<CreateTopic, Error> {
        if bytes.len() < 18 {
            return Err(Error::InvalidCommand);
        }
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes)?;
        position += stream_id.get_size_bytes() as usize;
        let topic_id = u32::from_le_bytes(bytes[position..position + 4].try_into()?);
        let partitions_count = u32::from_le_bytes(bytes[position + 4..position + 8].try_into()?);
        let message_expiry_secs =
            match u32::from_le_bytes(bytes[position + 8..position + 12].try_into()?) {
                0 => None,
                size => Some(size),
            };
        let max_topic_size_bytes =
            match u64::from_le_bytes(bytes[position + 12..position + 20].try_into()?) {
                0 => None,
                size => Some(size),
            };
        let replication_factor = bytes[position + 20];
        let name_length = bytes[position + 21];
        let name =
            from_utf8(&bytes[position + 22..(position + 22 + name_length as usize)])?.to_string();
        if name.len() != name_length as usize {
            return Err(Error::InvalidCommand);
        }
        let command = CreateTopic {
            stream_id,
            topic_id,
            partitions_count,
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
            name,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for CreateTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}",
            self.stream_id,
            self.topic_id,
            self.partitions_count,
            self.message_expiry_secs.unwrap_or(0),
            self.max_topic_size_bytes.unwrap_or(0),
            self.replication_factor,
            self.name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = CreateTopic {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: 2,
            partitions_count: 3,
            message_expiry_secs: Some(10),
            max_topic_size_bytes: Some(100),
            replication_factor: 1,
            name: "test".to_string(),
        };
        let bytes = command.as_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(&bytes).unwrap();
        position += stream_id.get_size_bytes() as usize;
        let topic_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let partitions_count =
            u32::from_le_bytes(bytes[position + 4..position + 8].try_into().unwrap());
        let message_expiry_secs =
            match u32::from_le_bytes(bytes[position + 8..position + 12].try_into().unwrap()) {
                0 => None,
                size => Some(size),
            };
        let max_topic_size_bytes =
            match u64::from_le_bytes(bytes[position + 12..position + 20].try_into().unwrap()) {
                0 => None,
                time_secs => Some(time_secs),
            };
        let replication_factor = bytes[position + 20];
        let name_length = bytes[position + 21];
        let name = from_utf8(&bytes[position + 22..(position + 22 + name_length as usize)])
            .unwrap()
            .to_string();

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partitions_count, command.partitions_count);
        assert_eq!(message_expiry_secs, command.message_expiry_secs);
        assert_eq!(max_topic_size_bytes, command.max_topic_size_bytes);
        assert_eq!(replication_factor, command.replication_factor);
        assert_eq!(name.len() as u8, command.name.len() as u8);
        assert_eq!(name, command.name);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = 2u32;
        let partitions_count = 3u32;
        let name = "test".to_string();
        let message_expiry_secs = 10;
        let max_topic_size_bytes = 100;
        let replication_factor = 1;
        let stream_id_bytes = stream_id.as_bytes();
        let mut bytes = Vec::with_capacity(14 + stream_id_bytes.len() + name.len());
        bytes.extend(stream_id_bytes);
        bytes.put_u32_le(topic_id);
        bytes.put_u32_le(partitions_count);
        bytes.put_u32_le(message_expiry_secs);
        bytes.put_u64_le(max_topic_size_bytes);
        bytes.put_u8(replication_factor);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(name.len() as u8);
        bytes.extend(name.as_bytes());

        let command = CreateTopic::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
        assert_eq!(command.message_expiry_secs, Some(message_expiry_secs));
        assert_eq!(command.max_topic_size_bytes, Some(max_topic_size_bytes));
        assert_eq!(command.replication_factor, replication_factor);
        assert_eq!(command.name, name);
    }

    #[test]
    fn should_be_read_from_string() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = 2u32;
        let partitions_count = 3u32;
        let message_expiry_secs = 10;
        let max_topic_size_bytes = 100;
        let replication_factor = 1;
        let name = "test".to_string();
        let input = format!("{stream_id}|{topic_id}|{partitions_count}|{message_expiry_secs}|{max_topic_size_bytes}|{replication_factor}|{name}");
        let command = CreateTopic::from_str(&input);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partitions_count, partitions_count);
        assert_eq!(command.message_expiry_secs, Some(message_expiry_secs));
        assert_eq!(command.max_topic_size_bytes, Some(max_topic_size_bytes));
        assert_eq!(command.name, name);
    }
}
