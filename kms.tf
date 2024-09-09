#To create KMS resource 
resource "aws_kms_key" "ddsl_kms" {
  description             = "Data Domain AWS Key"
  enable_key_rotation     = true
  key_usage               = "ENCRYPT_DECRYPT"
  tags = {
    Name = "ddsl_kms_key"    
  }
}

#To create KMS Alias
resource "aws_kms_alias" "ddsl_kms_key" {
  name          = "alias/ddsl_kms_key_alias"
  target_key_id = aws_kms_key.ddsl_kms.id
}