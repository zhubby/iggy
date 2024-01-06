use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::cmd::utils::message_expiry::MessageExpiry;
use crate::identifier::Identifier;
use crate::topics::update_topic::UpdateTopic;
use anyhow::Context;
use async_trait::async_trait;
use tracing::{event, Level};

pub struct UpdateTopicCmd {
    update_topic: UpdateTopic,
    message_expiry_secs: Option<MessageExpiry>,
    max_topic_size_bytes: Option<u64>,
    replication_factor: u8,
}

impl UpdateTopicCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        name: String,
        message_expiry_secs: Option<MessageExpiry>,
        max_topic_size_bytes: Option<u64>,
        replication_factor: u8,
    ) -> Self {
        Self {
            update_topic: UpdateTopic {
                stream_id,
                topic_id,
                name,
                message_expiry_secs: match &message_expiry_secs {
                    None => None,
                    Some(value) => value.into(),
                },
                max_topic_size_bytes,
                replication_factor,
            },
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
        }
    }
}

#[async_trait]
impl CliCommand for UpdateTopicCmd {
    fn explain(&self) -> String {
        let expiry_text = match &self.message_expiry_secs {
            Some(value) => format!(" with message expire time: {}", value),
            None => String::from(""),
        };
        let max_size_text = match &self.max_topic_size_bytes {
            Some(value) => format!(" with max topic size: {}", value),
            None => String::from(""),
        };
        let replication_factor_text = match self.replication_factor {
            0 => String::from(""),
            _ => format!(" with replication factor: {}", self.replication_factor),
        };
        format!(
            "update topic with ID: {}, name: {}{expiry_text}{max_size_text}{replication_factor_text} in stream with ID: {}",
            self.update_topic.topic_id, self.update_topic.name, self.update_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        client
            .update_topic(&self.update_topic)
            .await
            .with_context(|| {
                format!(
                    "Problem updating topic (ID: {}, name: {}{}) in stream with ID: {}",
                    self.update_topic.topic_id,
                    self.update_topic.name,
                    match &self.message_expiry_secs {
                        Some(value) => format!(" and message expire time: {}", value),
                        None => String::from(""),
                    },
                    self.update_topic.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Topic with ID: {} updated name: {}{} in stream with ID: {}",
            self.update_topic.topic_id,
            self.update_topic.name,
            match &self.message_expiry_secs{
                Some(value) => format!(" and message expire time: {}", value),
                None => String::from(""),
            },
            self.update_topic.stream_id,
        );

        Ok(())
    }
}
