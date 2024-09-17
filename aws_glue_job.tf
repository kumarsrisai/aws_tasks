#AWS Glue job for a Python script
resource "aws_glue_job" "segregate" {
  name = "Segregate"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"  
  number_of_workers = "2.0"
  worker_type = "G.1X"
  command {
    #name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.example1.bucket}/segregate.py"
    python_version = "3"
  }
   default_arguments = {    
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job_log_group.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
  }
  execution_property {
    max_concurrent_runs = 10 
  }
}

#AWS Glue job for a Py script
resource "aws_glue_job" "data_quality1" {
  name = "Data-Quality_File_Level_Check-dev"
  role_arn = aws_iam_role.glue_role.arn
  max_capacity = "1.0"
  glue_version = "4.0"
  command {
    #name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.example1.bucket}/dq1.py"
    python_version = "3"
  }
   default_arguments = {    
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.data_quality_log_group1.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
  }
  execution_property {
    max_concurrent_runs = 10 
  }
}

#AWS Glue job for a Py script
resource "aws_glue_job" "data_quality2" {
  name = "Record_Level_DQ-dev"
  role_arn = aws_iam_role.glue_role.arn
  max_capacity = "1.0"
  glue_version = "4.0"
  command {
    #name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.example1.bucket}/dq2.py"
    python_version = "3"
  }
   default_arguments = {    
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.data_quality_log_group2.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
  }
  execution_property {
    max_concurrent_runs = 10 
  }
}

#AWS Glue job for a Py script
resource "aws_glue_job" "data_history" {
  name          = "History_Load-dev"
  role_arn      = aws_iam_role.glue_role.arn
  glue_version  = "4.0"
  number_of_workers = 2
  worker_type   = "G.1X"
  
  command {
    script_location = "s3://${aws_s3_bucket.example1.bucket}/history-load.py"
    python_version  = "3"
  }

  default_arguments = {    
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.data_lineage.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
    "--job-language"                     = "Python 3"
    "--scriptLocation"                   = "s3://${aws_s3_bucket.example1.bucket}/history-load.py"
  }


  execution_property {
    max_concurrent_runs = 10
  }

  # Add lifecycle block to avoid idempotency issues
  lifecycle {
    ignore_changes = [
      name, # This will prevent issues if job already exists
    ]
  }

}
#AWS Glue job for a Python script
resource "aws_glue_job" "data_transpose" {
  name = "rec_type_transpose"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"  
  number_of_workers = "2.0"
  worker_type = "G.1X"
  command {
    #name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.example1.bucket}/rec-type-transpose.py"
    python_version = "3"
  }
   default_arguments = {    
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job_log_group.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
  }
  execution_property {
    max_concurrent_runs = 10 
  }
}




# #AWS Glue job for a Python script
# resource "aws_glue_job" "example" {
#   name = "DDSL_Glue_job"
#   role_arn = aws_iam_role.gluerole.arn
#   glue_version = "4.0"  
#   number_of_workers = "2.0"
#   worker_type = "G.1X"
#   command {
#     #name            = "pythonshell"
#     script_location = "s3://${aws_s3_bucket.example1.bucket}/segregate.py"
#     python_version = "3"
#   }
#    default_arguments = {    
#     "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_job_log_group.name
#     "--enable-continuous-cloudwatch-log" = "true"
#     "--enable-continuous-log-filter"     = "true"
#     "--enable-metrics"                   = ""
#   }
# }