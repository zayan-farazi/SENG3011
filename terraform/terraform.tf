terraform {
  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }

    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
  }

  backend "s3" {}

  required_version = ">= 1.2"
}
