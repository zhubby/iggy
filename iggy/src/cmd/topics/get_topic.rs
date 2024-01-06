use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::identifier::Identifier;
use crate::topics::get_topic::GetTopic;
use crate::utils::timestamp::IggyTimestamp;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub struct GetTopicCmd {
    get_topic: GetTopic,
}

impl GetTopicCmd {
    pub fn new(stream_id: Identifier, topic_id: Identifier) -> Self {
        Self {
            get_topic: GetTopic {
                stream_id,
                topic_id,
            },
        }
    }
}

#[async_trait]
impl CliCommand for GetTopicCmd {
    fn explain(&self) -> String {
        format!(
            "get topic with ID: {} from stream with ID: {}",
            self.get_topic.topic_id, self.get_topic.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let topic = client.get_topic(&self.get_topic).await.with_context(|| {
            format!(
                "Problem getting topic with ID: {} in stream {}",
                self.get_topic.topic_id, self.get_topic.stream_id
            )
        })?;

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Topic id", format!("{}", topic.id).as_str()]);
        table.add_row(vec![
            "Created",
            IggyTimestamp::from(topic.created_at)
                .to_string("%Y-%m-%d %H:%M:%S")
                .as_str(),
        ]);
        table.add_row(vec!["Topic name", topic.name.as_str()]);
        table.add_row(vec!["Topic size", format!("{}", topic.size_bytes).as_str()]);
        table.add_row(vec![
            "Message expiry",
            match topic.message_expiry_secs {
                Some(value) => format!("{}", value),
                None => String::from("unlimited"),
            }
            .as_str(),
        ]);
        table.add_row(vec![
            "Max topic size",
            match topic.max_topic_size_bytes {
                Some(value) => format!("{}", value),
                None => String::from("unlimited"),
            }
            .as_str(),
        ]);
        table.add_row(vec![
            "Topic message count",
            format!("{}", topic.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Partitions count",
            format!("{}", topic.partitions_count).as_str(),
        ]);

        event!(target: PRINT_TARGET, Level::INFO,"{table}");

        Ok(())
    }
}
