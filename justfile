list-objects:
  source .aws/credentials && cargo run --bin inspect -- --region us-west-2 --bucket rpbtest

etl:
  source .aws/credentials && RUST_LOG=etl=debug,s3kv=debug cargo run --release --bin etl -- --region us-west-2 --bucket rpbtest --prefix $(date -u -Iseconds) --input data/nd.json --block-size 10000000

scan:
  source .aws/credentials && RUST_LOG=etl=debug,s3kv=debug cargo run --release --bin scan -- --region us-west-2 --bucket rpbtest --prefix "2023-05-05T19:01:42+00:00" --block-size 10000000
