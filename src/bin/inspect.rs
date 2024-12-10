/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#![allow(clippy::result_large_err)]

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{config::Region, meta::PKG_VERSION, Client, Error};
use clap::Parser;

#[derive(Debug, Parser)]
struct Opt {
    /// The AWS Region.
    #[arg(short, long)]
    region: String,

    /// The name of the bucket.
    #[arg(short, long)]
    bucket: String,

    /// Whether to display additional information.
    #[arg(short, long)]
    verbose: bool,
}

// Lists the objects in a bucket.
// snippet-start:[s3.rust.list-objects]
async fn show_objects(client: &Client, bucket: &str) -> Result<(), Error> {
    let resp = client.list_objects_v2().bucket(bucket).send().await?;

    for object in resp.contents() {
        println!("{}", object.key().unwrap_or_default());
    }

    Ok(())
}
// snippet-end:[s3.rust.list-objects]

/// Lists the objects in an Amazon S3 bucket.
/// # Arguments
///
/// * `-b BUCKET` - The name of the bucket.
/// * `-o OBJECT` - The name of the object in the bucket.
/// * `-n NAME` - The name of person.
/// * `[-r REGION]` - The Region in which the client is created.
///   If not supplied, uses the value of the **AWS_REGION** environment variable.
///   If the environment variable is not set, defaults to **us-west-2**.
/// * `[-v]` - Whether to display additional information.
#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();

    let Opt {
        region,
        bucket,
        verbose,
    } = Opt::parse();

    let region_provider = RegionProviderChain::first_try(Region::new(region));

    println!();

    if verbose {
        println!("S3 client version: {}", PKG_VERSION);
        println!(
            "Region:            {}",
            region_provider.region().await.unwrap().as_ref()
        );
        println!("Bucket:            {}", &bucket);
        println!();
    }

    let shared_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);

    show_objects(&client, &bucket).await
}
