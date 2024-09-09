resource "aws_athena_database" "example" {
  name   = "my_athena_db"
  bucket = aws_s3_bucket.data_bucket.bucket
}

# resource "aws_athena_database" "example" {
#   name   = "example_database"
#   bucket = aws_s3_bucket.data_bucket[0].bucket
# }


resource "aws_athena_workgroup" "example" {
  name = "ddsl_athena_workgroup"
  configuration {
    enforce_workgroup_configuration = true
  }
}
