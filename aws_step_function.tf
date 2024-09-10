resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "ddsl-sfn-state-machine-developer" 
  
  role_arn = aws_iam_role.iam_for_sfn.arn
  type     = "STANDARD"
  definition = <<EOF
{
    "Comment": "To trigger events from s3 to stepfunction to AWS Glue",
    "StartAt": "GlueJob",
    "States": {
        "GlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "${aws_glue_job.example.name}",
                "Arguments": {
                    "--rec_type": "9001",  // Replace with actual value
                    "--env": "dev"  // Replace with actual environment value (e.g., 'dev' or 'prod')
                }
            },
            "Next": "Passed Record Job run"
        },
        "Passed Record Job run": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "${aws_glue_job.data_quality1.name}",
                "Arguments": {
                    "--rec_type": "9001",  // Replace with actual value
                    "--env": "dev"  // Replace with actual environment value (e.g., 'dev' or 'prod')
                }
            },
            "Next": "Checksum Record Job run"
        },
        "Checksum Record Job run": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "${aws_glue_job.data_quality2.name}",
                "Arguments": {
                    "--rec_type": "",  // Replace with actual value
                    "--env": "dev"  // Replace with actual environment value (e.g., 'dev' or 'prod')
                }
            },
            "End": true
        }
    }
}
EOF
  logging_configuration {  
    log_destination = "${aws_cloudwatch_log_group.stepfunction_log_group.arn}:*"
    include_execution_data = true
    level = "ALL"  
  }
}
