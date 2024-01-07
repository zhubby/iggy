use crate::streaming::batching::METADATA_BYTES_LEN;
use bytes::{Buf, BufMut, Bytes};
use iggy::bytes_serializable::BytesSerializable;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::compression::compressor::{Compressor, GzCompressor};
use iggy::error::Error;
use iggy::models::messages::{Message, MessageState};
use std::collections::HashMap;

/*
 Attributes Byte Structure:
 | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
 ---------------------------------
 |CA |CA| U | U | U | U | U | U |

 Legend:
 CA - Compression Algorithm (Bits 0 and 1)
 U  - Unused (Bits 2 to 7)
*/
const COMPRESSION_ALGORITHM_SHIFT: u8 = 6;
const COMPRESSION_ALGORITHM_MASK: u8 = 0b11 << COMPRESSION_ALGORITHM_SHIFT;

#[derive(Debug, Clone)]
pub struct MessagesBatch {
    pub base_offset: u64,
    //TODO(numinex) - change this to u64
    pub length: u32,
    pub last_offset_delta: u32,
    attributes: u8,
    pub messages: Bytes,
}
pub struct MessagesBatchAttributes {
    pub compression_algorithm: CompressionAlgorithm,
}
impl MessagesBatchAttributes {
    pub fn new(compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            compression_algorithm,
        }
    }
    pub fn create(&self) -> u8 {
        let mut attributes: u8 = 0;

        let compression_bits = (self.compression_algorithm.as_code()
            << COMPRESSION_ALGORITHM_SHIFT)
            & COMPRESSION_ALGORITHM_MASK;
        attributes |= compression_bits;

        attributes
    }
    fn get_compression_algorithm_code(attributes: &u8) -> u8 {
        (attributes & COMPRESSION_ALGORITHM_MASK) >> 6
    }
}
impl MessagesBatch {
    pub fn new(
        base_offset: u64,
        length: u32,
        last_offset_delta: u32,
        attributes: u8,
        messages: Bytes,
    ) -> Self {
        Self {
            base_offset,
            length,
            last_offset_delta,
            attributes,
            messages,
        }
    }
    pub fn get_compression_algorithm(&self) -> Result<CompressionAlgorithm, Error> {
        let compression_algorithm =
            MessagesBatchAttributes::get_compression_algorithm_code(&self.attributes);
        CompressionAlgorithm::from_code(compression_algorithm)
    }
    //TODO - turn those two into a trait
    pub fn messages_to_batch(
        base_offset: u64,
        last_offset_delta: u32,
        attributes: u8,
        messages: Vec<Message>,
    ) -> Result<Self, Error> {
        let ca_code = MessagesBatchAttributes::get_compression_algorithm_code(&attributes);
        let compression_algorithm = CompressionAlgorithm::from_code(ca_code)?;

        let payload: Vec<_> = messages
            .into_iter()
            .flat_map(|message| message.as_bytes())
            .collect();
        let compressed_payload = match compression_algorithm {
            CompressionAlgorithm::None => payload,
            _ => {
                if payload.len() > compression_algorithm.min_data_size() {
                    // Let's use this simple heuristic for now,
                    // Later on, once we have proper compression metrics
                    // We can employ statistical analysis
                    let compression_ratio = 0.75;
                    let buffer_size = (payload.len() as f64 * compression_ratio) as usize;
                    let buffer = Vec::with_capacity(buffer_size);

                    match compression_algorithm {
                        CompressionAlgorithm::Gzip => {
                            GzCompressor::new().compress(payload, buffer)?
                        }
                        _ => unreachable!("Unsupported compression algorithm"),
                    }
                } else {
                    payload
                }
            }
        };

        let len = METADATA_BYTES_LEN + compressed_payload.len() as u32;
        Ok(Self::new(
            base_offset,
            len,
            last_offset_delta,
            attributes,
            Bytes::from(compressed_payload),
        ))
    }
    pub fn into_messages(self) -> Result<Vec<Message>, Error> {
        let compression_algorithm = &self.get_compression_algorithm()?;
        let mut messages = Vec::new();
        let mut buffer = self.messages;

        buffer = match compression_algorithm {
            CompressionAlgorithm::None => buffer,
            _ => {
                let compressor: Box<dyn Compressor> = match compression_algorithm {
                    CompressionAlgorithm::Gzip => Box::new(GzCompressor::new()),
                    _ => unreachable!("Unsupported compression algorithm"),
                };

                let compression_rate = 0.75;
                let buffer_size = (buffer.len() as f64 / compression_rate) as usize;
                let mut decompression_buffer = Vec::with_capacity(buffer_size);

                let buffer_vec = buffer.as_ref();
                compressor.decompress(buffer_vec, &mut decompression_buffer)?;
                Bytes::from(decompression_buffer)
            }
        };

        while buffer.remaining() > 0 {
            let offset = buffer.get_u64_le();
            let state_code = buffer.get_u8();
            let state = MessageState::from_code(state_code)?;
            let timestamp = buffer.get_u64_le();
            let id = buffer.get_u128_le();
            let checksum = buffer.get_u32_le();

            let headers_len = buffer.get_u32_le();
            let headers = if headers_len > 0 {
                let headers_payload = buffer.copy_to_bytes(headers_len as usize).to_vec();
                Some(HashMap::from_bytes(&headers_payload)?)
            } else {
                None
            };

            let length = buffer.get_u32_le();
            let payload = buffer.copy_to_bytes(length as usize);

            messages.push(Message {
                offset,
                state,
                timestamp,
                id,
                checksum,
                headers,
                length,
                payload,
            });
        }

        Ok(messages)
    }
    pub fn is_contained_or_overlapping_within_offset_range(&self, start_offset: u64, end_offset: u64) -> bool {
        (self.base_offset <= end_offset && self.get_last_offset() >= end_offset) ||
            (self.base_offset <= start_offset && self.get_last_offset() <= end_offset)
    }
    pub fn get_size_bytes(&self) -> u32 {
        return METADATA_BYTES_LEN + self.messages.len() as u32;
    }
    pub fn extend(&self, bytes: &mut Vec<u8>) {
        bytes.put_u64_le(self.base_offset);
        bytes.put_u32_le(self.length);
        bytes.put_u32_le(self.last_offset_delta);
        bytes.put_u8(self.attributes);
        bytes.extend(&self.messages);
    }
    pub fn get_last_offset(&self) -> u64 {
        self.base_offset + self.last_offset_delta as u64
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_create_attributes_with_gzip_compression_algorithm() {
        let attributes = MessagesBatchAttributes::new(CompressionAlgorithm::Gzip).create();
        let messages_batch = MessagesBatch::new(1337, 69, 420, attributes, Bytes::new());
        let compression_algorithm = messages_batch.get_compression_algorithm().unwrap();

        assert_eq!(compression_algorithm, CompressionAlgorithm::Gzip);
    }
}
