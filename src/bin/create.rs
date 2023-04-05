use std::{path::PathBuf, io::{BufReader, BufRead}, fs::File};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client};
use clap::Parser;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Opt {
        region,
        bucket,
        prefix,
        input,
    } = Opt::parse();

    let region_provider = RegionProviderChain::first_try(Region::new(region));

    println!();

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let fin = BufReader::new(File::open(input)?);
    for line in fin.lines() {
        let line = line?;
        let digest = ring::digest::digest(&ring::digest::SHA256, line.as_bytes());
        let key = format!("{}/{}", prefix, hex::encode(digest.as_ref()));
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(line.into_bytes()))
            .send()
            .await
            .unwrap();
    }

    Ok(())
}
