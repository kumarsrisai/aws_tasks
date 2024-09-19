resource "aws_athena_database" "example" {
  name   = "ddsl_athena_developer_db"
  bucket = aws_s3_bucket.data_bucket.bucket
}


resource "aws_athena_workgroup" "example" {
  name = "ddsl_athena_developer_workgroup"
  configuration {
    enforce_workgroup_configuration = true
  }
}
