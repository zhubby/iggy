use crate::streaming::batching::messages_batch::MessagesBatch;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::storage::SegmentStorage;
use bytes::{BufMut, Bytes};
use iggy::error::Error;
use iggy::models::messages::{Message, MessageState};
use std::sync::Arc;
use tracing::trace;

const EMPTY_MESSAGES: Vec<Message> = vec![];

impl Segment {
    pub fn get_messages_count(&self) -> u64 {
        if self.current_size_bytes == 0 {
            return 0;
        }

        self.current_offset - self.start_offset + 1
    }

    pub async fn get_messages(&self, mut offset: u64, count: u32) -> Result<Vec<Message>, Error> {
        if count == 0 {
            return Ok(EMPTY_MESSAGES);
        }
        //Ok(EMPTY_MESSAGES)

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let mut end_offset = offset + (count - 1) as u64;
        // In case that the partition messages buffer is disabled, we need to check the unsaved messages buffer
        if self.unsaved_messages.is_none() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let unsaved_batches = self.unsaved_messages.as_ref().unwrap();
        if unsaved_batches.is_empty() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let first_offset = unsaved_batches[0].base_offset;
        if end_offset < first_offset {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let last_offset = unsaved_batches[unsaved_batches.len() - 1].get_last_offset();
        if offset >= first_offset && end_offset <= last_offset {
            return self.load_messages_from_unsaved_buffer(offset, end_offset);
        }

        let mut messages = self.load_messages_from_disk(offset, end_offset).await?;
        let mut buffered_batches = self.load_messages_from_unsaved_buffer(offset, end_offset)?;
        //messages.append(&mut buffered_messages);

        Ok(messages)
    }

    pub async fn get_all_messages(&self) -> Result<Vec<Message>, Error> {
        self.get_messages(self.start_offset, self.get_messages_count() as u32)
            .await
    }

    pub async fn get_newest_messages_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<Arc<Message>>, Error> {
        /*
        let messages = self
            .storage
            .segment
            .load_newest_messages_by_size(self, size_bytes)
            .await?;

        Ok(messages)
        */
        let msgs: Vec<_> = EMPTY_MESSAGES.into_iter().map(Arc::new).collect();
        Ok(msgs)
    }

    fn load_messages_from_unsaved_buffer(
        &self,
        offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Message>, Error> {
        let relative_start_offset = offset - self.start_offset;
        let relative_end_offset = end_offset - self.start_offset;

        let unsaved_messages = self.unsaved_messages.as_ref().unwrap();
        let slice_start = unsaved_messages
            .iter()
            .rposition(|batch| batch.base_offset <= relative_start_offset)
            .unwrap_or(0);

        // Take only the batch when last_offset >= relative_end_offset and it's base_offset is <= relative_end_offset
        // otherwise take batches until the last_offset >= relative_end_offset and base_offset <= relative_start_offset
        let messages = unsaved_messages[slice_start..]
            .into_iter()
            .cloned()
            .filter(|batch| {
                batch.is_contained_or_overlapping_within_offset_range(relative_start_offset, relative_end_offset)
            })
            .map(|batch| batch.into_messages())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .filter(|msg| msg.offset >= offset && msg.offset <= end_offset)
            .collect();

        Ok(messages)
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Message>, Error> {
        trace!(
            "Loading messages from disk, segment start offset: {}, end offset: {}, current offset: {}...",
            start_offset,
            end_offset,
            self.current_offset
        );
        println!("{}", self.current_offset);

        if start_offset > end_offset
        //|| end_offset > self.current_offset
        {
            trace!(
                "Cannot load messages from disk, invalid offset range: {} - {}.",
                start_offset,
                end_offset
            );
            return Ok(EMPTY_MESSAGES);
        }

        let relative_start_offset = (start_offset - self.start_offset) as u32;
        let relative_end_offset = (end_offset - self.start_offset) as u32;

        let index_range =
            match self.load_highest_lower_bound_index(relative_start_offset, relative_end_offset) {
                Ok(range) => range,
                Err(_) => {
                    trace!(
                        "Cannot load messages from disk, index range not found: {} - {}.",
                        start_offset,
                        end_offset
                    );
                    return Ok(EMPTY_MESSAGES);
                }
            };

        self.load_messages_from_segment_file(&index_range, start_offset, end_offset)
            .await
    }

    async fn load_messages_from_segment_file(
        &self,
        index_range: &IndexRange,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Message>, Error> {
        let batches = self
            .storage
            .segment
            .load_messages(self, index_range)
            .await?;

        let messages = batches
            .into_iter()
            .map(|batch| batch.into_messages())
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .filter(|msg| msg.offset >= start_offset && msg.offset <= end_offset)
            .collect::<Vec<_>>();
        trace!(
            "Loaded {} messages from disk, segment start offset: {}, end offset: {}.",
            messages.len(),
            self.start_offset,
            self.current_offset
        );

        Ok(messages)
    }

    pub async fn append_messages(
        &mut self,
        messages: MessagesBatch,
        last_message_offset: u64,
    ) -> Result<(), Error> {
        if self.is_closed {
            return Err(Error::SegmentClosed(self.start_offset, self.partition_id));
        }

        // Does this even make sense? Maybe preallocate Array with 100 capacity
        // and then check whether we are reaching the capacity limit and resize it?
        if let Some(indexes) = &mut self.indexes {
            indexes.reserve(1);
        }

        if let Some(time_indexes) = &mut self.time_indexes {
            time_indexes.reserve(1);
        }

        // For now ignoring timestamp index, need to calculate max_timestamp first.
        self.store_offset_and_timestamp_index_for_batch(last_message_offset);
        let batch_size = messages.get_size_bytes();

        let unsaved_messages = self.unsaved_messages.get_or_insert_with(Vec::new);
        unsaved_messages.push(messages);
        self.current_size_bytes += batch_size;

        // Not the prettiest code. It's done this way to avoid repeatably
        // checking if indexes and time_indexes are Some or None.
        /*
        if self.indexes.is_some() && self.time_indexes.is_some() {
            for message in messages {
                let relative_offset = (message.offset - self.start_offset) as u32;

                self.indexes.as_mut().unwrap().push(Index {
                    relative_offset,
                    position: self.current_size_bytes,
                });

                self.time_indexes.as_mut().unwrap().push(TimeIndex {
                    relative_offset,
                    timestamp: message.timestamp,
                });

                self.current_size_bytes += message.get_size_bytes();
                self.current_offset = message.offset;
                unsaved_messages.push(message.clone());
            }
        } else if self.indexes.is_some() {
            for message in messages {
                let relative_offset = (message.offset - self.start_offset) as u32;

                self.indexes.as_mut().unwrap().push(Index {
                    relative_offset,
                    position: self.current_size_bytes,
                });

                self.current_size_bytes += message.get_size_bytes();
                self.current_offset = message.offset;
                unsaved_messages.push(message.clone());
            }
        } else if self.time_indexes.is_some() {
            for message in messages {
                let relative_offset = (message.offset - self.start_offset) as u32;

                self.time_indexes.as_mut().unwrap().push(TimeIndex {
                    relative_offset,
                    timestamp: message.timestamp,
                });

                self.current_size_bytes += message.get_size_bytes();
                self.current_offset = message.offset;
                unsaved_messages.push(message.clone());
            }
        } else {
            for message in messages {
                self.current_size_bytes += message.get_size_bytes();
                self.current_offset = message.offset;
                unsaved_messages.push(message.clone());
            }
        }
        */
        Ok(())
    }
    fn store_offset_and_timestamp_index_for_batch(&mut self, batch_last_offset: u64) {
        let relative_offset = (batch_last_offset - self.start_offset) as u32;
        match (&mut self.indexes, &mut self.time_indexes) {
            (Some(indexes), Some(time_indexes)) => {
                indexes.push(Index {
                    relative_offset,
                    position: self.current_size_bytes,
                });
                /*
                time_indexes.push(TimeIndex {
                    relative_offset,
                    timestamp: message.timestamp,
                });
                 */
            }
            (Some(indexes), None) => {
                indexes.push(Index {
                    relative_offset,
                    position: self.current_size_bytes,
                });
            }
            (None, Some(time_indexes)) => {
                /*
                time_indexes.push(TimeIndex {
                    relative_offset,
                    timestamp: message.timestamp,
                });
                 */
            }
            (None, None) => {}
        };

        // Regardless of whether caching of indexes and time_indexes is on
        // store them in the unsaved buffer
        self.unsaved_indexes.put_u32_le(relative_offset);
        self.unsaved_indexes.put_u32_le(self.current_size_bytes);
    }

    pub async fn persist_messages(
        &mut self,
        storage: Arc<dyn SegmentStorage>,
    ) -> Result<(), Error> {
        if self.unsaved_messages.is_none() {
            return Ok(());
        }

        let unsaved_messages = self.unsaved_messages.as_ref().unwrap();
        if unsaved_messages.is_empty() {
            return Ok(());
        }

        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id
        );

        let saved_bytes = storage.save_messages(self, unsaved_messages).await?;
        //let current_position = self.current_size_bytes - saved_bytes;

        storage.save_index(&self).await?;
        self.unsaved_indexes.clear();

        //storage.save_time_index(self, unsaved_messages).await?;
        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full().await {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.unsaved_messages = None;
        } else {
            self.unsaved_messages.as_mut().unwrap().clear();
        }

        Ok(())
    }
}
