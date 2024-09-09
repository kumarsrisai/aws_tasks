#To create KMS resource 
resource "aws_kms_key" "ddsl_kms" {
  description             = "KMS key for encrypting S3 bucket data"
  deletion_window_in_days = 30
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "KMS policy",
  "Statement": [
    {
      "Sid": "Enable IAM User Permissions",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::590183849298:root"
      },
      "Action": "kms:*",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": [
        "kms:Encrypt*",
        "kms:Decrypt*",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:Describe*"
      ],
      "Resource": "*"
    }
  ]
}
POLICY
}

output "kms_key_arn" {
  value = aws_kms_key.ddsl_kms.arn
}


#To create KMS Alias
resource "aws_kms_alias" "ddsl_kms_key" {
  name          = "alias/ddsl_kms_key_alias"
  target_key_id = aws_kms_key.ddsl_kms.id
}