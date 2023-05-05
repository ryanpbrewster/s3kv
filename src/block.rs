use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use hex::ToHex;
use integer_encoding::VarInt;

#[derive(PartialEq, Eq, Debug, Clone, Copy, Default)]
pub struct Location {
    block_id: u64,
    offset: usize,
}
impl Location {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(self.block_id.required_space() + self.offset.required_space());
        self.block_id.encode_var(&mut buf);
        self.offset.encode_var(&mut buf);
        buf
    }
}

#[async_trait]
pub trait BlockWriter {
    async fn append(&mut self, item: &[u8]) -> anyhow::Result<Location>;
    async fn flush(&mut self) -> anyhow::Result<()>;
}

pub trait BlockReader {
    fn fetch(&mut self, loc: &Location) -> anyhow::Result<&[u8]>;
}

pub struct S3BlockWriter {
    underlying: aws_sdk_s3::Client,
    buf: Vec<u8>,
    block_size: usize,
    prefix: String,
    cur: Location,
}
impl S3BlockWriter {
    pub fn new(client: aws_sdk_s3::Client, block_size: usize, prefix: String) -> Self {
        Self {
            underlying: client,
            buf: Vec::with_capacity(block_size),
            block_size,
            prefix,
            cur: Location::default(),
        }
    }
}

#[async_trait]
impl BlockWriter for S3BlockWriter {
    async fn append(&mut self, item: &[u8]) -> anyhow::Result<Location> {
        if self.cur.offset + item.len() > self.block_size {
            self.flush().await?;
        }
        let loc = self.cur;
        self.buf.extend_from_slice(item);
        self.cur.offset += item.len();
        Ok(loc)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let compressed = zstd::bulk::compress(&self.buf, 0)?;
        let name: String = self.cur.block_id.encode_var_vec().encode_hex();
        self.underlying
            .put_object()
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
