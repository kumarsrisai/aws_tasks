#AWS Glue job for a Python script
resource "aws_glue_job" "example" {
  name = "DDSL_Glue_job"
  role_arn = aws_iam_role.gluerole.arn
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
}

#AWS Glue job for a Py script
resource "aws_glue_job" "data_quality1" {
  name = "DDSL_Dataquality1_job"
  role_arn = aws_iam_role.gluerole.arn
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
}

#AWS Glue job for a Py script
resource "aws_glue_job" "data_quality2" {
  name = "DDSL_Dataquality2_job"
  role_arn = aws_iam_role.gluerole.arn
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
}

#AWS Glue job for a Py script
resource "aws_glue_job" "data_lineage" {
  name = "DDSL_Datalineage_job"
  role_arn = aws_iam_role.gluerole.arn  
  glue_version = "4.0"
  number_of_workers = "2.0"
  worker_type = "G.1X"
  command {
    #name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.example1.bucket}/lineage.py"
    python_version = "3"
  }
   default_arguments = {    
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.data_lineage.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
    "--job-language"                     = "Python 3"
    "--scriptLocation"                   = "s3://${aws_s3_bucket.example1.bucket}/lineage.py"
    "--extra-jars"                       = "s3://${aws_s3_bucket.example1.bucket}/openlineage-spark_2.12-1.13.1.jar,"
    "--user-jars-first"                  = "true" 
    "--encryption-type"                  = ""
  }
}