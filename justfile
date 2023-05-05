list-objects:
  source .aws/credentials && cargo run --bin inspect -- --region us-west-2 --bucket rpbtest

etl:
  source .aws/credentials && RUST_LOG=etl=debug,s3kv=debug cargo run --release --bin etl -- --region us-west-2 --bucket rpbtest --prefix $(date -u -Iseconds) --input data/nd.json
