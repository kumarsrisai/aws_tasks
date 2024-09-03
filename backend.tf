terraform {
  backend "s3" {
    bucket         = "cg-statefile-storage"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "cg-statefile-table"
  }
}
 