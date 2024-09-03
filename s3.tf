# S3 bucket to store Raw Data
resource "aws_s3_bucket" "example1" {
  bucket = var.s3_bucket1
  # Prevent accidental deletion of this S3 bucket
  lifecycle {
    prevent_destroy = false
  }
   tags = {
    Name        = var.s3_bucket1
    Environment = "Dev"
  }
}

resource "aws_s3_object" "bucket1_folder1" {
    bucket = aws_s3_bucket.example1.bucket
    key = "batch_landing/"
}

resource "aws_s3_object" "bucket1_folder2" {
    bucket = aws_s3_bucket.example1.bucket
    key = "batch_archived/"
}

resource "aws_s3_object" "bucket1_folder3" {
    bucket = aws_s3_bucket.example1.bucket
    key = "event_landing/"
}


# S3 bucket to store Segregated Data
resource "aws_s3_bucket" "example2" {
  bucket = var.s3_bucket2
  # Prevent accidental deletion of this S3 bucket
  lifecycle {
    prevent_destroy = false
  }
   tags = {
    Name        = var.s3_bucket2
    Environment = "Dev"
  }
}

resource "aws_s3_object" "bucket2_folder" {
    bucket = aws_s3_bucket.example2
    key = "batch_splitted"
}

resource "aws_s3_object" "bucket2_subfolder1" {
    bucket = aws_s3_bucket.example2
    key = "batch_splitted/good/"
}

resource "aws_s3_object" "bucket2_subfolder2" {
    bucket = aws_s3_bucket.example2
    key = "batch_splitted/bad/"
}

# S3 bucket to store DQ1 Data
resource "aws_s3_bucket" "example3" {
  bucket = var.s3_bucket3
  # Prevent accidental deletion of this S3 bucket
  lifecycle {
    prevent_destroy = false
  }
   tags = {
    Name        = var.s3_bucket3
    Environment = "Dev"
  }
}

resource "aws_s3_object" "bucket3_folder" {
    bucket = aws_s3_bucket.example3
    key = "batch_dq"
}

resource "aws_s3_object" "bucket3_subfolder1" {
    bucket = aws_s3_bucket.example3
    key = "batch_dq/good/"
}

resource "aws_s3_object" "bucket3_subfolder2" {
    bucket = aws_s3_bucket.example3
    key = "batch_dq/bad/"
}

# S3 bucket to store DQ1 Data
resource "aws_s3_bucket" "example4" {
  bucket = var.s3_bucket4
  # Prevent accidental deletion of this S3 bucket  
  lifecycle {
    prevent_destroy = false
  }
   tags = {
    Name        = var.s3_bucket4
    Environment = "Dev"
  }
}

resource "aws_s3_object" "bucket4_folder" {
    bucket = aws_s3_bucket.example4
    key = "batch_processed"
}

resource "aws_s3_object" "bucket4_subfolder" {
    bucket = aws_s3_bucket.example4
    key = "batch_processed/accounting/"
}

resource "aws_s3_object" "bucket4_folder" {
    bucket = aws_s3_bucket.example4
    key = "event_processed"
}

resource "aws_s3_object" "bucket4_subfolder" {
    bucket = aws_s3_bucket.example4
    key = "event_processed/accounting/"
}



resource "aws_s3_bucket" "example5" {
  bucket = var.s3_bucket5
  # Prevent accidental deletion of this S3 bucket  
  lifecycle {
    prevent_destroy = false
  }
   tags = {
    Name        = var.s3_bucket5
    Environment = "Dev"
  }
}

resource "aws_s3_object" "bucket5_folder" {
    bucket = aws_s3_bucket.example5
    key = "batch_ods"
}

resource "aws_s3_object" "bucket5_subfolder" {
    bucket = aws_s3_bucket.example5
    key = "batch_ods/bain_account/"
}

resource "aws_s3_object" "bucket5_folder" {
    bucket = aws_s3_bucket.example5
    key = "event_ods"
}

resource "aws_s3_object" "bucket5_subfolder" {
    bucket = aws_s3_bucket.example5
    key = "event_ods/bian_ods/"
}