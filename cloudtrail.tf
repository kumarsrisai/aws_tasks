# Resource to create Cloudtrail
resource "aws_cloudtrail" "trail" {
  name                       = "virginia_cloudtrail"
  depends_on                 = [aws_s3_bucket_policy.cloudtrail_bucket_policy, aws_iam_role.cloudtrail_cloudwatch_events_role] 
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail_cloudwatch_events_role.arn
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail_log_group.arn}:*" 
  enable_log_file_validation = "true"
  enable_logging             = "true"
  is_multi_region_trail      = "true"
  s3_bucket_name             = aws_s3_bucket.example2.bucket
  include_global_service_events = true
  event_selector {
    read_write_type           = "All"
    include_management_events = true

    data_resource {
      type   = "AWS::S3::Object"     
      values = ["arn:aws:s3:::${aws_s3_bucket.example2.id}/"]
    }
  }
}