terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-central-1"
}

# Raw XML from DB API lands here. Immutable. Never transformed.
# If Silver breaks, this is what you reprocess from.

resource "aws_s3_bucket" "bronze" {
  bucket = "db-pipeline-bronze-${data.aws_caller_identity.current.account_id}"
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Data older than 90 days moves to Glacier automatically.
# $0.023/GB → $0.004/GB. 83% cost reduction. Zero effort.
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "archive-to-glacier"
    status = "Enabled"
    filter {}

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.bucket
}

# ── S3 Silver bucket ─────────────────────────────────────────────────────────
# Quality-scored records land here as Apache Iceberg.
# Time-travel. Full audit history. Never deleted.

resource "aws_s3_bucket" "silver" {
  bucket = "db-pipeline-silver-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Glue Job ─────────────────────────────────────────────────────────────────
resource "aws_glue_job" "quality_scoring" {
  name         = "db-pipeline-quality-scoring"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "4.0"

  command {
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/glue_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--bronze_bucket" = aws_s3_bucket.bronze.bucket
    "--silver_bucket" = aws_s3_bucket.silver.bucket
    "--enable-glue-datacatalog" = "true"
    "--enable-job-insights"     = "true"
    "--datalake-formats"        = "iceberg"

  }

  max_retries = 0
  timeout     = 60
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.bucket
}

output "glue_job_name" {
  value = aws_glue_job.quality_scoring.name
}