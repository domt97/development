output "store_products_bucket_name" {
  description = "S3 bucket name for store products"
  value       = aws_s3_bucket.store_products.bucket
}

output "reservation_expire_queue_name" {
  description = "SQS queue name for reservation expiration"
  value       = aws_sqs_queue.reservation_expire_queue.name
}

output "reservation_expire_queue_url" {
  description = "SQS queue URL for reservation expiration"
  value       = aws_sqs_queue.reservation_expire_queue.url
}

output "reservation_expire_queue_arn" {
  description = "SQS queue ARN for reservation expiration"
  value       = aws_sqs_queue.reservation_expire_queue.arn
}

output "reservation_expire_rule_name" {
  description = "EventBridge reservation expiration rule name"
  value       = aws_cloudwatch_event_rule.reservation_expire.name
}

output "reservation_expire_rule_arn" {
  description = "EventBridge reservation expiration rule ARN"
  value       = aws_cloudwatch_event_rule.reservation_expire.arn
}