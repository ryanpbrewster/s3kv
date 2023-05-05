use std::io::Write;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, Client};
use clap::Parser;
use rocksdb::{IteratorMode, ReadOptions};
use s3kv::block::{BlockReader, Location, S3BlockReader, S3BlockReaderArgs};
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::try_parse()?;

    let region_provider = RegionProviderChain::first_try(Region::new(args.region));
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let db_dir = tempfile::TempDir::new()?;
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let db = rocksdb::DB::open(&db_opts, db_dir.path())?;

    debug!("downloading index default.sst");
    let index_resp = client
        .get_object()
        .bucket(&args.bucket)
        .key(format!("{}/index/default.sst", args.prefix))
        .send()
        .await?;
    let index_body = index_resp.body.collect().await?;
    let mut index_file = tempfile::NamedTempFile::new()?;
    let _ = index_file.write(&index_body.to_vec())?;
    debug!("ingesting index default.sst");
    db.ingest_external_file(vec![index_file.path()])?;

    let mut block_reader = S3BlockReader::new(S3BlockReaderArgs {
        client: client.clone(),
        bucket: args.bucket.clone(),
        prefix: format!("{}/block", args.prefix),
        block_size: args.block_size,
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
            println!("{} --> {:?}", std::str::from_utf8(&k)?, loc);
        } else {
            let record = block_reader.fetch(&loc).await?;
            println!(
                "{} -> {}",
                std::str::from_utf8(&k)?,
                std::str::from_utf8(&record)?
            );
        }
    }

    Ok(())
}
