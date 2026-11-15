terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      # < 5.86.0: from 5.86.0 the provider never finishes creating aws_s3_bucket_lifecycle_configuration against
      # LocalStack 3.8.1 (5.86.0 "inconsistent result after apply", 5.100.0 waits until its 3 min timeout); 5.85.0 works.
      version = "~> 5.0, < 5.86.0"
    }
  }
}

provider "aws" {
  region     = var.region
  access_key = "test"
  secret_key = "test"

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  skip_region_validation      = true

  endpoints {
    s3     = var.localstack_endpoint
    sqs    = var.localstack_endpoint
    events = var.localstack_endpoint
  }

  s3_use_path_style = true
}

# --------------------------------------------------
# S3
# --------------------------------------------------

resource "aws_s3_bucket" "store_products" {
  bucket = "${var.profile}-store-products"
}

# Browsers upload product import files straight to the bucket with a presigned PUT from stores-service (BR-CAT-17).
# allowed_headers is "*" because LocalStack 3.8.1 rejects the browser's preflight list "content-type,x-amz-tagging"
# (comma without a space) when the headers are named; the presigned signature still fixes the content type, length and
# tag, and only these origins and PUT are allowed.
resource "aws_s3_bucket_cors_configuration" "store_products" {
  bucket = aws_s3_bucket.store_products.id

  cors_rule {
    allowed_origins = var.web_origins
    allowed_methods = ["PUT"]
    allowed_headers = ["*"]
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}

# Uploaded import files are deleted after var.import_upload_retention_days (BR-CAT-17). stores-service tags every
# presigned upload purpose=import-upload; files copied by hand have no tag and are kept. A tag filter, not a prefix:
# import files live at {merchantId}/{storeId}/imports/, which has no fixed prefix.
resource "aws_s3_bucket_lifecycle_configuration" "store_products" {
  bucket = aws_s3_bucket.store_products.id

  rule {
    id     = "expire-import-uploads"
    status = "Enabled"

    filter {
      tag {
        key   = "purpose"
        value = "import-upload"
      }
    }

    expiration {
      days = var.import_upload_retention_days
    }
  }
}

# --------------------------------------------------
# SQS
# --------------------------------------------------

resource "aws_sqs_queue" "reservation_expire_queue" {
  name                       = "${var.profile}-reservation-expire-queue"
  visibility_timeout_seconds = 90
}

# --------------------------------------------------
# EventBridge
# --------------------------------------------------

resource "aws_cloudwatch_event_rule" "reservation_expire" {
  name                = "${var.profile}-reservation-expire"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "reservation_expire_queue" {
  rule = aws_cloudwatch_event_rule.reservation_expire.name
  arn  = aws_sqs_queue.reservation_expire_queue.arn

  target_id = "${var.profile}-reservation-expire-queue"

  depends_on = [
    aws_sqs_queue_policy.reservation_expire
  ]
}

# --------------------------------------------------
# SQS Policy
# --------------------------------------------------

resource "aws_sqs_queue_policy" "reservation_expire" {
  queue_url = aws_sqs_queue.reservation_expire_queue.url

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "AllowEventBridge"
        Effect = "Allow"

        Principal = {
          Service = "events.amazonaws.com"
        }

        Action = "sqs:SendMessage"

        Resource = aws_sqs_queue.reservation_expire_queue.arn

        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_cloudwatch_event_rule.reservation_expire.arn
          }
        }
      }
    ]
  })
}