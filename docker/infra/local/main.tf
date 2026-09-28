terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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

# --------------------------------------------------
# SQS
# --------------------------------------------------

resource "aws_sqs_queue" "reservation_expire_queue" {
  name = "${var.profile}-reservation-expire-queue"
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