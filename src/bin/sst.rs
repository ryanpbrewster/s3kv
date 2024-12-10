use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use tracing::info;

#[derive(Debug, Parser)]
struct Opt {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(name = "cat")]
    Cat {
        #[arg(long)]
        input: Vec<PathBuf>,
    },
    #[command(name = "compact")]
    Compact {
        #[arg(long)]
        input: PathBuf,
    },
    #[command(name = "make-sst")]
    MakeSst {
        #[arg(long)]
        input: PathBuf,

        #[arg(long)]
        output: PathBuf,
    },

    #[command(name = "merge")]
    Merge {
        #[arg(long)]
        input: Vec<PathBuf>,

        #[arg(long)]
        output: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let opt = Opt::parse();
    match opt.cmd {
        Command::Cat { input } => {
            for i in input {
                cat_db(i)?;
            }
        }
        Command::Compact { input } => compact_db(input)?,
        Command::MakeSst { input, output } => make_sst(input, output)?,
        Command::Merge { input, output } => merge_ssts(input, output)?,
    };
    Ok(())
}

fn cat_db(input: PathBuf) -> anyhow::Result<()> {
    let db = rocksdb::DB::open(&rocksdb::Options::default(), input)?;
    for item in db.iterator(rocksdb::IteratorMode::Start) {
        let (k, v) = item?;
        println!("{}={:?}", std::str::from_utf8(&k)?, v);
    }
    Ok(())
}

fn compact_db(input: PathBuf) -> anyhow::Result<()> {
    let db = rocksdb::DB::open(&rocksdb::Options::default(), input)?;
    let mut opts = rocksdb::CompactOptions::default();
    opts.set_bottommost_level_compaction(rocksdb::BottommostLevelCompaction::Force);
    db.compact_range_opt::<Vec<u8>, Vec<u8>>(None, None, &opts);
    Ok(())
}

fn make_sst(input: PathBuf, output: PathBuf) -> anyhow::Result<()> {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let mut db = rocksdb::SstFileWriter::create(&db_opts);
    db.open(output)?;

    info!("opening {:?}", input);
    let fin = BufReader::new(File::open(input)?);
    for line in fin.lines() {
        let line = line?;
        let parsed: serde_json::Value = serde_json::from_str(&line)?;
        let digest = ring::digest::digest(&ring::digest::SHA256, line.as_bytes());
        let primary_key = parsed
            .get("properties")
            .unwrap()
            .get("BLKLOT")
            .unwrap()
            .as_str()
            .unwrap();
        db.put(primary_key, digest)?;
    }
    db.finish()?;
    Ok(())
}

fn merge_ssts(inputs: Vec<PathBuf>, output: PathBuf) -> anyhow::Result<()> {
    let mut db_opts = rocksdb::Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
    let db = rocksdb::DB::open(&db_opts, output)?;
    db.ingest_external_file(inputs)?;
    Ok(())
}
