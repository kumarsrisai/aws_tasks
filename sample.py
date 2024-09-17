{
  "Type": "Parallel",
  "Branches": [
    {
      "StartAt": "Record level validations for 9000",
      "States": {
        "Record level validations for 9000": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9000",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9000"
        },
        "History updates for 9000": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9000",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9002",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9002"
        },
        "History updates for 9002": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9002",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9004",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9004"
        },
        "History updates for 9004": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9004",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9005",
              "--env": "cg_dev"
            }
          },
          "Next": "History load for 9005"
        },
        "History load for 9005": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "${aws_glue_job.data_history.name}",
            "Arguments": {
              "--rec_type": "rec_type_9005",
              "--env": "cg_dev",
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
            "JobName": "sb_Rec_type_Transpose",
            "Arguments": {
              "--rec_type": "rec_type_9005",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9005"
        },
        "History updates for 9005": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9005",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9006",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9006"
        },
        "History updates for 9006": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9006",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9009",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9009"
        },
        "History updates for 9009": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9009",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9012",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9012"
        },
        "History updates for 9012": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9012",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9019",
              "--env": "cg_dev"
            }
          },
          "Next": "History load for 9019"
        },
        "History load for 9019": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "${aws_glue_job.data_history.name}",
            "Arguments": {
              "--rec_type": "rec_type_9019",
              "--env": "cg_dev",
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
            "JobName": "sb_Rec_type_Transpose",
            "Arguments": {
              "--rec_type": "rec_type_9019",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9019"
        },
        "History updates for 9019": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9019",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9020",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9020"
        },
        "History updates for 9020": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9020",
              "--env": "cg_dev",
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
            "JobName": "test_file_exist_rec_level_ check",
            "Arguments": {
              "--rec_type": "rec_type_9031",
              "--env": "cg_dev"
            }
          },
          "Next": "History updates for 9031"
        },
        "History updates for 9031": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "sb_History_Updates-dev1_v2",
            "Arguments": {
              "--rec_type": "rec_type_9031",
              "--env": "cg_dev",
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
}