list-objects:
  source .aws/credentials && cargo run --bin inspect -- --region us-west-2 --bucket rpbtest
