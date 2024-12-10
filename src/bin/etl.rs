use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client};
use clap::Parser;
use rocksdb::SstFileWriter;
use s3kv::{
    blob::{Blobstore, S3Client},
    block::{BlockWriter, S3BlockWriter, S3BlockWriterArgs},
};
use tracing::{debug, info};

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
    let shared_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);

    let db_dir = tempfile::TempDir::new()?;
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let db = rocksdb::DB::open(&db_opts, db_dir.path())?;

    let mut block_writer = S3BlockWriter::new(S3BlockWriterArgs {
        client: Box::new(
            S3Client {
                client: client.clone(),
                bucket: args.bucket.clone(),
            }
            .with_compression()
            .with_prefix(&format!("{}/block", args.prefix)),
        ),
        block_size: args.block_size,
    });

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
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.disable_wal(true);
        db.put_opt(primary_key, loc.encode(), &write_opts)?;
    }
    block_writer.flush().await?;
    db.flush()?;

    debug!("rewriting index");
    let index_file = tempfile::NamedTempFile::new()?;
    let mut index_writer = SstFileWriter::create(&db_opts);
    index_writer.open(index_file.path())?;
    for entry in db.iterator(rocksdb::IteratorMode::Start) {
        let (k, v) = entry?;
        index_writer.put(k, v)?;
    }
    index_writer.finish()?;
    debug!("pushing index default.sst");
    let index_body = ByteStream::read_from()
        .path(index_file.path())
        .build()
        .await?;
    client
        .put_object()
        .bucket(&args.bucket)
        .key(format!("{}/index/default.sst", args.prefix))
        .body(index_body)
        .send()
        .await?;

    Ok(())
}
