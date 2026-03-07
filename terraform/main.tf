provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "seng_3011_bkt" {
  bucket = "seng-3011-bkt-zayan-dev"

  tags = {
    Name        = "seng-3011-bkt"
    Environment = "dev"
  }
}
