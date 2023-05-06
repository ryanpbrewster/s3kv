use std::{collections::HashMap, io::Write, time::Instant};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, Client};
use clap::Parser;
use hdrhistogram::Histogram;
use rand::{seq::SliceRandom, SeedableRng};
use rocksdb::IteratorMode;
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

    #[arg(long, default_value_t = 0)]
    cache_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::try_parse()?;

    let region_provider = RegionProviderChain::first_try(Region::new(args.region));
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);
    let blob = S3Client {
        client,
        bucket: args.bucket,
        prefix: args.prefix,
    };

    let db_dir = tempfile::TempDir::new()?;
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let db = rocksdb::DB::open(&db_opts, db_dir.path())?;

    debug!("downloading index default.sst");
    let index_body = blob.must_get("index/default.sst").await?;
    let mut index_file = tempfile::NamedTempFile::new()?;
    let _ = index_file.write(&index_body.to_vec())?;
    debug!("ingesting index default.sst");
    db.ingest_external_file(vec![index_file.path()])?;

    let mut block_reader = S3BlockReader::new(S3BlockReaderArgs {
        client: blob.resolved("/block"),
        block_size: args.block_size,
        cache_size: args.cache_size,
    });

    let mut samples = HashMap::new();
    for entry in db.iterator(IteratorMode::Start) {
        let (k, v) = entry?;
        let loc = Location::decode(&v)?;
        samples.insert(loc.block_id, k.to_vec());
    }
    let samples: Vec<Vec<u8>> = samples.into_values().collect();

    let mut prng = rand::rngs::SmallRng::seed_from_u64(42);
    let mut hist: Histogram<u32> = Histogram::new(5)?;
    loop {
        for _ in 0..100 {
            let start = Instant::now();

            if let Some(v) = db.get(samples.choose(&mut prng).unwrap())? {
                let loc = Location::decode(&v)?;
                let _ = block_reader.fetch(&loc).await?;
            }

            hist.record(start.elapsed().as_nanos() as u64).unwrap();
        }
        debug!(
            "fetches={} mean={} p99={}",
            hist.len(),
            hist.mean(),
            hist.value_at_quantile(0.99)
        );
    }
}
