resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "ddsl-sfn-state-machine-developer"
  role_arn = aws_iam_role.iam_for_sfn.arn
  type     = "STANDARD"
  definition = <<EOF
{
    "Comment": "To trigger events from S3 to StepFunction to AWS Glue",
    "StartAt": "Segregate",
    "States": {
        "Segregate": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "${aws_glue_job.segregate.name}",
                "Arguments": {
                   
                    "--env": "dev"
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
                    
                    "--env": "dev"
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
                    "--rec_type": "rec_type_9000",
                    "--env": "dev"
                }
            },
            "Next": "History level validation Job run"
        },
        "History level validation Job run": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "${aws_glue_job.data_history.name}",
                "Arguments": {
                    "--rec_type": "rec_type_9000",
                    "--env": "dev"
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
