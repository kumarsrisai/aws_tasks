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
                    "--rec_type": "rec_type_9000",
                    "--env": "dev"
                }
            },
            "Next": "Checksum Record Job run"
        },
        "Checksum Record Job run":{
            "Type": "Parallel",
            "Branches": [
              {
                "StartAt": "Record level validations for 9000",
                "States": {
                  "Record level validations for 9000": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9000",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9000"
                  },
                  "load updates for 9000": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9000",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9002",
                "States": {
                  "Record level validations for 9002": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9002",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9002"
                  },
                  "History updates for 9002": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9002",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9004",
                "States": {
                  "Record level validations for 9004": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9004",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9004"
                  },
                  "History updates for 9004": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9004",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9005",
                "States": {
                  "Record level validations for 9005": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9005",
                        "--env": "dev"
                      }
                    },
                    "Next": "History load for 9005"
                  },
                  "History load for 9005": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_load.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9005",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "Next": "Transposed for 9005"
                  },
                  "Transposed for 9005": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_transpose.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9005",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9005"
                  },
                  "History updates for 9005": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9005",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9006",
                "States": {
                  "Record level validations for 9006": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9006",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9006"
                  },
                  "History updates for 9006": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9006",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9009",
                "States": {
                  "Record level validations for 9009": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9009",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9009"
                  },
                  "History updates for 9009": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9009",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9012",
                "States": {
                  "Record level validations for 9012": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9012",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9012"
                  },
                  "History updates for 9012": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9012",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9019",
                "States": {
                  "Record level validations for 9019": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9019",
                        "--env": "dev"
                      }
                    },
                    "Next": "History load for 9019"
                  },
                  "History load for 9019": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_load.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9019",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "Next": "Transposed for 9019"
                  },
                  "Transposed for 9019": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_transpose.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9019",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9019"
                  },
                  "History updates for 9019": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9019",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9020",
                "States": {
                  "Record level validations for 9020": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9020",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9020"
                  },
                  "History updates for 9020": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9020",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              },
              {
                "StartAt": "Record level validations for 9031",
                "States": {
                  "Record level validations for 9031": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_quality2.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9031",
                        "--env": "dev"
                      }
                    },
                    "Next": "History updates for 9031"
                  },
                  "History updates for 9031": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                      "JobName": "${aws_glue_job.data_update.name}",
                      "Arguments": {
                        "--rec_type": "rec_type_9031",
                        "--env": "dev",
                        "--datalake-formats": "hudi",
                        "-Djdk.attach.allowAttachSelf": "true",
                        "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED --conf spark.sql.shuffle.partitions=5"
                      }
                    },
                    "End": true
                  }
                }
              }
            ],
            "Next": "FinalState"
          },
          "FinalState":{
          "Type":"Succeed"
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
