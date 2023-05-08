list-objects:
  source .aws/credentials && cargo run --bin inspect -- --region us-west-2 --bucket rpbtest

etl:
  source .aws/credentials && RUST_LOG=etl=debug,s3kv=debug cargo run --release --bin etl -- --region us-west-2 --bucket rpbtest --prefix $(date -u -Iseconds) --input data/nd.json --block-size 10000000

scan:
  source .aws/credentials && RUST_LOG=scan=debug,s3kv=debug cargo run --release --bin scan -- --region us-west-2 --bucket rpbtest --prefix "2023-05-08T15:55:27+00:00" --block-size 10000000 --quiet

fetch-random:
  source .aws/credentials && RUST_LOG=fetch_random=debug,s3kv=debug cargo run --release --bin fetch_random -- --region us-west-2 --bucket rpbtest --prefix "2023-05-08T15:55:27+00:00" --block-size 10000000
