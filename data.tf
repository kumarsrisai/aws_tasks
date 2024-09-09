data "aws_iam_policy_document" "example1" {
  statement {
    sid    = "AWSCloudTrailWrite"
    effect = "Allow"
    actions   = ["s3:PutObject"]
    resources = [
      "arn:aws:s3:::${aws_s3_bucket.example2.id}/*",
    ]
    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }
    principals {
      type = "Service"
      identifiers = ["cloudtrail.amazon.com"]
    }
  }
}

resource "aws_s3_bucket_policy" "cloudtrail_bucket_policy" {
  bucket = aws_s3_bucket.data_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AWSCloudTrailWrite"
        Effect    = "Allow"
        Principal = {
          Service = "cloudtrail.amazonaws.com"
        }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.data_bucket.arn}/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      }
    ]
  })
}



# data "aws_s3_bucket" "existing_bucket" {
#   bucket = "ddsl-processed-developer"
# }

