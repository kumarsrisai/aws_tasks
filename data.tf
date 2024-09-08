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
  bucket = aws_s3_bucket.example2.id
  policy = data.aws_iam_policy_document.example1.json
}

