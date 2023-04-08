use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client};
use clap::Parser;
use tracing::log::info;

#[derive(Debug, Parser)]
struct Opt {
    /// The AWS Region.
    #[arg(long)]
    region: String,

    /// The name of the bucket.
    #[arg(long)]
    bucket: String,

    #[arg(long)]
    prefix: String,

    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    output: PathBuf,

    #[arg(long, default_value_t = false)]
    skip_s3: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Opt {
        region,
        bucket,
        prefix,
        input,
        output,
        skip_s3,
    } = Opt::parse();

    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let mut db = rocksdb::DB::open(&db_opts, output)?;

    let region_provider = RegionProviderChain::first_try(Region::new(region));
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    info!("opening {:?}", input);
    let fin = BufReader::new(File::open(input)?);
    for line in fin.lines() {
        let line = line?;
        let parsed: serde_json::Value = serde_json::from_str(&line)?;
        let digest = ring::digest::digest(&ring::digest::SHA256, line.as_bytes());
        let name = hex::encode(digest.as_ref());

        let primary_key = parsed
            .get("properties")
            .unwrap()
            .get("BLKLOT")
            .unwrap()
            .as_str()
            .unwrap();
        let write_opts = {
            let mut opts = rocksdb::WriteOptions::default();
            opts.disable_wal(true);
            opts
        };
        db.put_opt(primary_key, digest, &write_opts)?;

        if skip_s3 {
            client
                .put_object()
                .bucket(&bucket)
                .key(&format!("{}/{}", prefix, name))
                .body(ByteStream::from(line.into_bytes()))
                .send()
                .await
                .unwrap();
        }
    }

    Ok(())
}
