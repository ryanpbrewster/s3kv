use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client};
use clap::Parser;
use s3kv::block::{BlockWriter, S3BlockWriter};
use tracing::log::info;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    input: PathBuf,

    /// The AWS Region.
    #[arg(long)]
    region: String,

    /// The name of the bucket.
    #[arg(long)]
    bucket: String,

    #[arg(long)]
    prefix: String,

    #[arg(long, default_value_t = 1_000_000)]
    block_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::try_parse()?;

    let region_provider = RegionProviderChain::first_try(Region::new(args.region));
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let index_file = tempfile::NamedTempFile::new()?;
    let mut db_opts = rocksdb::Options::default();
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let mut index_writer = rocksdb::SstFileWriter::create(&db_opts);
    index_writer.open(index_file.path())?;

    let mut block_writer = S3BlockWriter::new(
        client.clone(),
        args.block_size,
        format!("{}/block", args.prefix),
    );

    info!("opening {:?}", args.input);
    let fin = BufReader::new(File::open(args.input)?);
    for line in fin.lines() {
        let line = line?;
        let loc = block_writer.append(line.as_bytes()).await?;

        let parsed: serde_json::Value = serde_json::from_str(&line)?;
        let primary_key = parsed
            .get("properties")
            .unwrap()
            .get("BLKLOT")
            .unwrap()
            .as_str()
            .unwrap();
        index_writer.put(primary_key, loc.encode())?;
    }
    block_writer.flush().await?;

    index_writer.finish()?;
    let index_body = ByteStream::read_from()
        .path(index_file.path())
        .build()
        .await?;
    client
        .put_object()
        .key(format!("{}/index/default.sst", args.prefix))
        .body(index_body)
        .send()
        .await?;

    Ok(())
}
