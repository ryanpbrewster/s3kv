list-objects:
  source .aws/credentials && cargo run --bin inspect -- --region us-west-2 --bucket rpbtest

etl:
  source .aws/credentials && cargo run --release --bin etl -- --region us-west-2 --bucket rpbtest
