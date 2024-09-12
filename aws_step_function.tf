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
                    # "--rec_type": "9001",
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
                    # "--rec_type": "9001",
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
                    "--rec_type": "9001",
                    "--env": "dev"
                }
            },
            "Next": "Checksum Parallel"
        },
        "Checksum Parallel": {
            "Type": "Parallel",
            "Branches": [
                {
                    "StartAt": "Checksum Record Job 1",
                    "States": {
                        "Checksum Record Job 1": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "${aws_glue_job.data_quality2.name}",
                                "Arguments": {
                                    "--rec_type": "9001",
                                    "--env": "dev"
                                }
                            },
                            "End": true
                        }
                    }
                },
                {
                    "StartAt": "Checksum Record Job 2",
                    "States": {
                        "Checksum Record Job 2": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "${aws_glue_job.data_quality3.name}",
                                "Arguments": {
                                    "--rec_type": "9001",
                                    "--env": "dev"
                                }
                            },
                            "End": true
                        }
                    }
                }
            ],
            "Next": "Record_Level_DQ_dev1 Job run"
        },
        "Record_Level_DQ_dev1 Job run": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": "${aws_glue_job.data_history.name}",  
                "Arguments": {
                    "--rec_type": "9001",
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
