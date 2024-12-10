use std::io::Write;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{config::Region, Client};
use clap::Parser;
use rocksdb::{IteratorMode, ReadOptions};
use s3kv::{
    blob::{Blobstore, S3Client},
    block::{BlockReader, Location, S3BlockReader, S3BlockReaderArgs},
};
use tracing::debug;

#[derive(Debug, Parser)]
struct Args {
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

    #[arg(long)]
    start: Option<String>,

    #[arg(long)]
    end: Option<String>,

    #[arg(long, default_value_t = false)]
    keys_only: bool,

    #[arg(long, default_value_t = false)]
    quiet: bool,
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
    let mut blob = S3Client {
        client,
        bucket: args.bucket,
    }
    .with_prefix(&args.prefix);

    let db_dir = tempfile::TempDir::new()?;
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let db = rocksdb::DB::open(&db_opts, db_dir.path())?;

    debug!("downloading index default.sst");
    let index_body = blob.must_get("index/default.sst").await?;
    let mut index_file = tempfile::NamedTempFile::new()?;
    let _ = index_file.write(&index_body)?;
    debug!("ingesting index default.sst");
    db.ingest_external_file(vec![index_file.path()])?;

    let mut block_reader = S3BlockReader::new(S3BlockReaderArgs {
        client: Box::new(
            blob.with_prefix("block")
                .with_compression()
                .with_caching(16),
        ),
    });

    let mut read_opts = ReadOptions::default();
    if let Some(start) = args.start {
        read_opts.set_iterate_lower_bound(start.as_bytes());
    }
    if let Some(end) = args.end {
        read_opts.set_iterate_upper_bound(end.as_bytes());
    }
    for entry in db.iterator_opt(IteratorMode::Start, read_opts) {
        let (k, v) = entry?;
        let loc = Location::decode(&v)?;

        if args.keys_only {
            if !args.quiet {
                println!("{} --> {:?}", std::str::from_utf8(&k)?, loc);
            }
        } else {
            let record = block_reader.fetch(&loc).await?;
            if !args.quiet {
                println!(
                    "{} -> {}",
                    std::str::from_utf8(&k)?,
                    std::str::from_utf8(&record)?
                );
            }
        }
    }
    Ok(())
}
