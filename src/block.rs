use std::{io::Read, num::NonZeroUsize, time::Instant};

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use hdrhistogram::Histogram;
use hex::ToHex;
use integer_encoding::{VarInt, VarIntReader, VarIntWriter};
use lru::LruCache;
use tracing::debug;

#[derive(PartialEq, Eq, Debug, Clone, Copy, Default)]
pub struct Location {
    pub block_id: usize,
    pub offset: usize,
}
impl Location {
    pub fn encode(&self) -> Vec<u8> {
        let a = self.block_id.required_space();
        let b = self.offset.required_space();
        let mut buf = vec![0; a + b];
        self.block_id.encode_var(&mut buf[..a]);
        self.offset.encode_var(&mut buf[a..]);
        buf
    }
    pub fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let mut cursor = std::io::Cursor::new(buf);
        let loc = Location {
            block_id: cursor.read_varint()?,
            offset: cursor.read_varint()?,
        };
        Ok(loc)
    }
}

#[async_trait]
pub trait BlockWriter {
    async fn append(&mut self, item: &[u8]) -> anyhow::Result<Location>;
    async fn flush(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait BlockReader {
    async fn fetch(&mut self, loc: &Location) -> anyhow::Result<Vec<u8>>;
}

pub struct S3BlockWriter {
    underlying: aws_sdk_s3::Client,
    buf: Vec<u8>,
    block_size: usize,
    bucket: String,
    prefix: String,
    cur: Location,
}
pub struct S3BlockWriterArgs {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    pub prefix: String,
    pub block_size: usize,
}
impl S3BlockWriter {
    pub fn new(args: S3BlockWriterArgs) -> Self {
        Self {
            underlying: args.client,
            buf: Vec::with_capacity(args.block_size),
            block_size: args.block_size,
            bucket: args.bucket,
            prefix: args.prefix,
            cur: Location::default(),
        }
    }
}

#[async_trait]
impl BlockWriter for S3BlockWriter {
    async fn append(&mut self, item: &[u8]) -> anyhow::Result<Location> {
        let size = item.len().required_space();
        if self.cur.offset + size + item.len() > self.block_size {
            self.flush().await?;
        }
        let loc = self.cur;
        self.buf.write_varint(item.len())?;
        self.buf.extend_from_slice(item);
        self.cur.offset += size + item.len();
        Ok(loc)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let compressed = zstd::bulk::compress(&self.buf, 0)?;
        self.buf.clear();
        let name: String = self.cur.block_id.encode_var_vec().encode_hex();
        debug!("pushing block {}", name);
        self.underlying
            .put_object()
            .bucket(&self.bucket)
            .key(format!("{}/{}", self.prefix, name))
            .body(ByteStream::from(compressed))
            .send()
            .await?;
        self.cur = Location {
            block_id: self.cur.block_id + 1,
            offset: 0,
        };
        Ok(())
    }
}

pub struct S3BlockReader {
    underlying: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    block_size: usize,
    cache: LruCache<usize, Option<Vec<u8>>>,
    stats: Histogram<u32>,
}
pub struct S3BlockReaderArgs {
    pub client: aws_sdk_s3::Client,
    pub bucket: String,
    pub prefix: String,
    pub block_size: usize,
    pub cache_size: usize,
}
impl S3BlockReader {
    pub fn new(args: S3BlockReaderArgs) -> Self {
        Self {
            underlying: args.client,
            bucket: args.bucket,
            prefix: args.prefix,
            block_size: args.block_size,
            cache: LruCache::new(NonZeroUsize::new(args.cache_size).unwrap()),
            stats: Histogram::new(5).unwrap(),
        }
    }
    async fn fetch_block(&mut self, block_id: usize) -> anyhow::Result<&[u8]> {
        let block = self.cache.get_or_insert_mut(block_id, || None);
        if block.is_none() {
            let name: String = block_id.encode_var_vec().encode_hex();
            debug!("fetching block {}", name);
            let start = Instant::now();
            let resp = self
                .underlying
                .get_object()
                .bucket(&self.bucket)
                .key(format!("{}/{}", self.prefix, name))
                .send()
                .await?;
            let compressed = resp.body.collect().await?.to_vec();
            let mut buf = vec![0; self.block_size];
            let size = zstd::bulk::decompress_to_buffer(&compressed, &mut buf)?;
            buf.truncate(size);
            block.replace(buf);
            self.stats
                .record(start.elapsed().as_micros() as u64)
                .unwrap();
        }
        Ok(block.as_ref().unwrap())
    }

    pub fn stats(&mut self) -> &Histogram<u32> {
        &self.stats
    }
}
#[async_trait]
impl BlockReader for S3BlockReader {
    async fn fetch(&mut self, loc: &Location) -> anyhow::Result<Vec<u8>> {
        let block = self.fetch_block(loc.block_id).await?;

        let mut cursor = std::io::Cursor::new(block);
        cursor.set_position(loc.offset as u64);
        let record_size: usize = cursor.read_varint()?;
        let mut record = vec![0; record_size];
        cursor.read_exact(&mut record)?;
        Ok(record)
    }
}
