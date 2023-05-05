use std::{collections::HashSet, io::Write};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, Client};
use clap::Parser;
use hdrhistogram::Histogram;
use rocksdb::IteratorMode;
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
        cache_size: 1,
    });

    let mut block_ids = HashSet::new();
    for entry in db.iterator(IteratorMode::Start) {
        let (_, v) = entry?;
        let loc = Location::decode(&v)?;
        block_ids.insert(loc.block_id);
    }

    loop {
        for &block_id in &block_ids {
            let _ = block_reader
                .fetch(&Location {
                    block_id,
                    offset: 0,
                })
                .await?;
        }
        let stats: &Histogram<u32> = block_reader.stats();
        debug!(
            "fetches={} mean={} p99={}",
            stats.len(),
            stats.mean(),
            stats.value_at_quantile(0.99)
        );
    }
}
