use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::topic::Topic;
use iggy::error::Error;
use iggy::identifier::{IdKind, Identifier};
use iggy::utils::text;
use tracing::info;

impl Stream {
    pub fn get_topics_count(&self) -> u32 {
        self.topics.len() as u32
    }

    pub async fn create_topic(
        &mut self,
        id: u32,
        name: &str,
        partitions_count: u32,
        message_expiry_secs: Option<u32>,
        max_topic_size_bytes: Option<u64>,
        replication_factor: u8,
    ) -> Result<(), Error> {
        if self.topics.contains_key(&id) {
            return Err(Error::TopicIdAlreadyExists(id, self.stream_id));
        }

        let name = text::to_lowercase_non_whitespace(name);
        if self.topics_ids.contains_key(&name) {
            return Err(Error::TopicNameAlreadyExists(name, self.stream_id));
        }

        // TODO: check if max_topic_size_bytes is not lower than system.segment.size

        let topic = Topic::create(
            self.stream_id,
            id,
            &name,
            partitions_count,
            self.config.clone(),
            self.storage.clone(),
            message_expiry_secs,
            max_topic_size_bytes,
            replication_factor,
        )?;
        topic.persist().await?;
        info!("Created topic {}", topic);
        self.topics_ids.insert(name, id);
        self.topics.insert(id, topic);

        Ok(())
    }

    pub async fn update_topic(
        &mut self,
        id: &Identifier,
        name: &str,
        message_expiry_secs: Option<u32>,
        max_topic_size_bytes: Option<u64>,
        replication_factor: u8,
    ) -> Result<(), Error> {
        let topic_id;
        {
            let topic = self.get_topic(id)?;
            topic_id = topic.topic_id;
        }

        let updated_name = text::to_lowercase_non_whitespace(name);

        {
            if let Some(topic_id_by_name) = self.topics_ids.get(&updated_name) {
                if *topic_id_by_name != topic_id {
                    return Err(Error::TopicNameAlreadyExists(
                        updated_name.to_string(),
                        self.stream_id,
                    ));
                }
            }
        }

        let old_topic_name = {
            let topic = self.get_topic(id)?;
            topic.name.clone()
        };

        {
            self.topics_ids.remove(&old_topic_name.clone());
            self.topics_ids.insert(updated_name.clone(), topic_id);
            let topic = self.get_topic_mut(id)?;
            topic.name = updated_name;
            topic.message_expiry_secs = message_expiry_secs;
            for partition in topic.partitions.values_mut() {
                let mut partition = partition.write().await;
                partition.message_expiry_secs = message_expiry_secs;
                for segment in partition.segments.iter_mut() {
                    segment.message_expiry_secs = message_expiry_secs;
                }
            }
            topic.max_topic_size_bytes = max_topic_size_bytes;
            topic.replication_factor = replication_factor;

            topic.persist().await?;
            info!("Updated topic: {}", topic);
        }

        Ok(())
    }

    pub fn get_topics(&self) -> Vec<&Topic> {
        self.topics.values().collect()
    }

    pub fn get_topic(&self, identifier: &Identifier) -> Result<&Topic, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_topic_by_name(&identifier.get_string_value().unwrap()),
        }
    }

    pub fn get_topic_mut(&mut self, identifier: &Identifier) -> Result<&mut Topic, Error> {
        match identifier.kind {
            IdKind::Numeric => self.get_topic_by_id_mut(identifier.get_u32_value().unwrap()),
            IdKind::String => self.get_topic_by_name_mut(&identifier.get_string_value().unwrap()),
        }
    }

    fn get_topic_by_id(&self, id: u32) -> Result<&Topic, Error> {
        let topic = self.topics.get(&id);
        if topic.is_none() {
            return Err(Error::TopicIdNotFound(id, self.stream_id));
        }

        Ok(topic.unwrap())
    }

    fn get_topic_by_name(&self, name: &str) -> Result<&Topic, Error> {
        let topic_id = self.topics_ids.get(name);
        if topic_id.is_none() {
            return Err(Error::TopicNameNotFound(name.to_string(), self.stream_id));
        }

        self.get_topic_by_id(*topic_id.unwrap())
    }

    fn get_topic_by_id_mut(&mut self, id: u32) -> Result<&mut Topic, Error> {
        let topic = self.topics.get_mut(&id);
        if topic.is_none() {
            return Err(Error::TopicIdNotFound(id, self.stream_id));
        }

        Ok(topic.unwrap())
    }

    fn get_topic_by_name_mut(&mut self, name: &str) -> Result<&mut Topic, Error> {
        let topic_id = self.topics_ids.get(name);
        if topic_id.is_none() {
            return Err(Error::TopicNameNotFound(name.to_string(), self.stream_id));
        }

        Ok(self.topics.get_mut(topic_id.unwrap()).unwrap())
    }

    pub async fn delete_topic(&mut self, id: &Identifier) -> Result<Topic, Error> {
        let topic_id;
        let topic_name;
        {
            let topic = self.get_topic(id)?;
            topic_id = topic.topic_id;
            topic_name = topic.name.clone();
            if topic.delete().await.is_err() {
                return Err(Error::CannotDeleteTopic(topic.topic_id, self.stream_id));
            }
        }

        let topic = self.topics.remove(&topic_id).unwrap();
        self.topics_ids.remove(&topic_name);

        Ok(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::storage::tests::get_test_system_storage;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_get_topic_by_id_and_name() {
        let stream_id = 1;
        let stream_name = "test_stream";
        let topic_id = 2;
        let topic_name = "test_topic";
        let message_expiry_secs = Some(10);
        let max_topic_size_bytes = Some(100);
        let config = Arc::new(SystemConfig::default());
        let storage = Arc::new(get_test_system_storage());
        let mut stream = Stream::create(stream_id, stream_name, config, storage);
        stream
            .create_topic(
                topic_id,
                topic_name,
                1,
                message_expiry_secs,
                max_topic_size_bytes,
                1,
            )
            .await
            .unwrap();

        let topic = stream.get_topic(&Identifier::numeric(topic_id).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.name, topic_name);

        let topic = stream.get_topic(&Identifier::named(topic_name).unwrap());
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        assert_eq!(topic.topic_id, topic_id);
        assert_eq!(topic.name, topic_name);
    }
}
