use crate::error::Error;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};

pub trait Compressor {
    fn compress(&self, data: Vec<u8>, compression_buffer: Vec<u8>) -> Result<Vec<u8>, Error>;
    fn decompress<'a>(
        &self,
        data: &'a [u8],
        decompression_buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], Error>;
}

pub struct GzCompressor {}
impl GzCompressor {
    pub fn new() -> Self {
        Self {}
    }
}
impl Compressor for GzCompressor {
    fn compress(&self, data: Vec<u8>, compression_buffer: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mut encoder = GzEncoder::new(compression_buffer, Compression::default());
        encoder.write_all(&data)?;
        Ok(encoder.finish()?)
    }
    fn decompress<'a>(
        &self,
        data: &'a [u8],
        decompression_buffer: &'a mut Vec<u8>,
    ) -> Result<&'a [u8], Error> {
        let mut decoder = GzDecoder::new(data);
        decoder.read_to_end(decompression_buffer)?;
        Ok(decompression_buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: &str = r#"
        (a stench is in 3 5)
         (a pit is nearby)
         (is the wumpus near)
         (Did you go to 3 8)
         (Yes -- Nothing is there)
        (Shoot -- Shoot left)
        (Kill the wumpus -- shoot up)))
        (defun ss (&optional (sentences *sentences*))
        "Run some test sentences, and count how many were¬
        not parsed."
        (count-if-not
        #'(lambda (s)
        (format t "~2&>>> ~(~{~a ~}~)~%"¬
        s)
        (write (second (parse s)) :pretty t))
        *sentences*))"#;

    #[test]
    fn test_gzip_compress() {
        let compressor = GzCompressor::new();
        let compression_buffer = Vec::new();
        let result = compressor.compress(DATA.as_bytes().to_owned(), compression_buffer);
        assert!(result.is_ok());
        let compressed = result.unwrap();
        assert_ne!(compressed.len(), DATA.len());
    }

    #[test]
    fn test_gzip_decompress() {
        let compressor = GzCompressor::new();
        let compression_buffer = Vec::new();
        let compressed_data = compressor
            .compress(DATA.as_bytes().to_vec(), compression_buffer)
            .unwrap();
        let mut decompression_buffer = Vec::new();
        let result = compressor.decompress(compressed_data.as_slice(), &mut decompression_buffer);

        assert!(result.is_ok());
        let decompressed = result.unwrap();
        assert_eq!(decompressed, DATA.as_bytes());
    }
}
