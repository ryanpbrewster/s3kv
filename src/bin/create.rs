/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#![allow(clippy::result_large_err)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client, Error};
use clap::Parser;
use rand::{rngs::SmallRng, SeedableRng, RngCore};

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
    num_records: usize,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let Opt {
        region,
        bucket,
        prefix,
        num_records,
    } = Opt::parse();

    let region_provider = RegionProviderChain::first_try(Region::new(region));

    println!();

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let mut prng = SmallRng::seed_from_u64(42);

    for _ in 0 .. num_records {
        let mut data = vec![0; 1024];
        prng.fill_bytes(&mut data);
        let digest = ring::digest::digest(&ring::digest::SHA256, &data);
        let key = format!("{}/{}", prefix, hex::encode(digest.as_ref()));
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();
    }

    Ok(())
}
