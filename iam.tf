#To create KMS Policy 
resource "aws_kms_key_policy" "ddsl_kms_policy" {
  key_id = aws_kms_key.ddsl_kms.arn
  policy = jsonencode({
    "Version" = "2012-10-17"
    "Id" = "KMS policy"
    "Statement" = [
     {
            "Sid": "Enable IAM User Permissions",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::014498661566:root"
            },
            "Action": "kms:*",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "logs.us-east-1.amazonaws.com"
            },
            "Action": [
                "kms:Encrypt*",
                "kms:Decrypt*",
                "kms:ReEncrypt*",
                "kms:GenerateDataKey*",
                "kms:Describe*"
            ],
            "Resource": [
              "arn:aws:kms:us-east-1:014498661566:key/*"
            ]
            "Condition": {
                "ArnLike": {
                    "kms:EncryptionContext:aws:logs:arn": "arn:aws:logs:us-east-1:014498661566:*"
                }
            }
               }    
                ]
   
  })
}

#Cloudwatch -# Resource creation for IAM role for Cloudwatch
resource "aws_iam_role" "cloudtrail_cloudwatch_events_role" {
  name               = "cloudtrail_cloudwatch_events_role"
  path               = "/"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# Cloudwatch -Resource creation for IAM role policy for Cloudwatch
resource "aws_iam_role_policy" "aws_iam_role_policy_cloudTrail_cloudWatch" {
  name = "cloudTrail-cloudWatch-policy"
  role = aws_iam_role.cloudtrail_cloudwatch_events_role.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AWSCloudTrailCreateLogStream2014110",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream"
            ],
            "Resource": [
                "${aws_cloudwatch_log_group.cloudtrail_log_group.arn}:*"
            ]
        },
        {
            "Sid": "AWSCloudTrailPutLogEvents20141101",
            "Effect": "Allow",
            "Action": [
                "logs:PutLogEvents"
            ],
            "Resource": [
                "${aws_cloudwatch_log_group.cloudtrail_log_group.arn}:*"
            ]
        }
    ]
}
EOF
}

#Eventbridge - Creating a IAM role for Eventbridge
resource "aws_iam_role" "eventbridge_role" {
  name               = "Eventbridgerole"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "events.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

#Eventbridge - Creating a IAM Role policy for Eventbridge
resource "aws_iam_role_policy" "eventbridge_policy" {
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "TrustEventsToStoreLogEvent",
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource": "arn:aws:logs:${var.region}:${var.aws_account_id}:log-group:/aws/events/eventbridgelogs:*"
      },
      {
        "Sid": "AllowStepFunctionAccess",
        "Effect": "Allow",
        "Action": [
          "states:StartExecution",
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
          "events:PutEvents"
        ],
        "Resource": "arn:aws:states:*:*:stateMachine:*"
      },
      {
        "Sid": "AllowCloudWatchAccess",
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicies",
          "logs:DescribeLogGroups",
          "logs:CreateLogDelivery"
        ],
        "Resource": [
          "arn:aws:logs:${var.region}:${var.aws_account_id}:log-group:/aws/events/eventbridge.logs:*"
        ]
      }
    ]
  })
}


# Eventbridge - Create IAM policy for AWS Step Function to invoke-stepfunction-role-created-from-cloudwatch
resource "aws_iam_policy" "policy_invoke_eventbridge" {
  name        = "stepFunctionSampleEventBridgeInvocationPolicy" 
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
             "Action": [ 
                "states:StartExecution",
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule",
                "events:PutEvents" 
                ],
            "Resource": [ "arn:aws:states:*:*:stateMachine:*" ]
        }
     ]
   
}
EOF           
}

#Eventbridge - AWS resource for Eventbridge policy attachment
resource "aws_iam_policy_attachment" "eventbridge_policy_attachment" {
  name = "eventbridge_policy"
  roles = [aws_iam_role.eventbridge_role.name]
  policy_arn = aws_iam_policy.policy_invoke_eventbridge.arn
}

# Stepfunction - Create IAM role for AWS Step Function
 resource "aws_iam_role" "iam_for_sfn" {
  name = "stepfunction_role"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17"
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "sts:AssumeRole",        
        "Principal": {
          "Service": [ "states.amazonaws.com","events.amazonaws.com" ]
        }              
      },     
    ]
  })
}

#Stepfunction - Create IAM policy for AWS Step function
resource "aws_iam_policy" "stepfunction_invoke_gluejob_policy" {
  name = "ddsl_stepfunction_dev_policy"
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
          "Sid" : "AllowCWAndGlueAccess",
            "Effect": "Allow",
            "Action": [                
                "logs:CreateLogDelivery",
                "logs:CreateLogStream",
                "logs:GetLogDelivery",
                "logs:UpdateLogDelivery",
                "logs:DeleteLogDelivery",
                "logs:ListLogDeliveries",
                "logs:PutLogEvents",
                "logs:PutResourcePolicy",
                "logs:DescribeResourcePolicies",
                "logs:DescribeLogGroups",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun"   
            ],
            "Resource": "*"                     
        },       
        {
          "Sid" : "AllowEventBridgeAccess",
          "Effect": "Allow",
          "Action": [
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule",
                "events:PutEvents"                              
            ],
            "Resource": [
               "arn:aws:events:${var.region}:${var.aws_account_id}:rule/s3_put_object_event"
            ]
        },
        {
          "Sid" : "AllowStateMachineAccess",
            "Effect": "Allow",
            "Action": [
                "states:StartExecution",
                "states:DescribeExecution",
                "states:StopExecution"                              
            ],                    
            "Resource": [ 
              "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:ddsl-sfn-state-machine",
              "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:ddsl-sfn-state-machine-lineage",
              ]    
        }                
     ]
     tags = merge(var.tags,{
      name = lower(join("-", [var.appname, "statemachine", "role-policy", var.environment]))
     })    
})
}
#Stepfunction - AWS resource for stepfunction policy attachment
resource "aws_iam_policy_attachment" "stepfunction_policy_attachment" {
  name = "ddsl-stestepfunction-dev-policy-attachment"
  roles = [aws_iam_role.iam_for_sfn.name]
  policy_arn = aws_iam_policy.stepfunction_invoke_gluejob_policy.arn
}

# AWS Glue - IAM Resource for Gluejob
resource "aws_iam_role" "glue_role" {
  name = "gluerole"
  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "glue.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

#AWS Glue -IAM Glue policy
resource "aws_iam_policy" "ddsl_glue_job_policy" {
  name = "ddsl-glue-${var.environment}-policy"
  
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "AllowGlueIAMKMSLogsAccess",
        "Effect": "Allow",
        "Action": [
          "glue:CreateDatabase",
          "glue:DeleteDatabase",
          "glue:GetDatabase",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetTable",
          "glue:GetTableVersion",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchUpdatePartition",
          "iam:ListRoles",
          "iam:ListUsers",
          "iam:ListGroups",
          "iam:ListRolePolicies",
          "iam:GetRole",
          "iam:GetRolePolicy",
          "iam:ListAttachedRolePolicies",
          "kms:ListAliases",
          "kms:DescribeKey",
          "kms:Encrypt*",
          "kms:Decrypt*",
          "kms:GenerateDataKey",
          "logs:AssociateKmsKey",
          "logs:GetLogEvents",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ],
        "Resource": "*"
      },
      {
        "Sid": "AllowS3Access",
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:List*",
          "s3:*Object*",
          "s3:Get*",
          "s3:CopyObject"
        ],
        "Resource": [
          "arn:aws:s3:::${var.s3_bucket1}/*",
          "arn:aws:s3:::${var.s3_bucket2}/*",
          "arn:aws:s3:::${var.s3_bucket3}/*",
          "arn:aws:s3:::${var.s3_bucket4}/*",
          "arn:aws:s3:::${var.s3_bucket5}/*",
          "arn:aws:s3:::${var.s3_bucket1}",
          "arn:aws:s3:::${var.s3_bucket2}",
          "arn:aws:s3:::${var.s3_bucket3}",
          "arn:aws:s3:::${var.s3_bucket4}",
          "arn:aws:s3:::${var.s3_bucket5}"
        ]
      }
    ]
  })

  tags = merge(var.tags, {
    Name = lower(join("-", [var.appname, "glue", "role-policy", var.environment]))
  })
}

# AWS Glue - AWS resource for Glue policy attachment
resource "aws_iam_policy_attachment" "gluejob_policy_attachment" {
  name       = "ddsl-glue-${var.environment}-policy-attachment"
  roles      = [aws_iam_role.glue_role.name]
  policy_arn = aws_iam_policy.ddsl_glue_job_policy.arn
}

# AWS Glue - AWS resource for service role
resource "aws_iam_policy_attachment" "AWSGlueServiceRole" {
  name       = "AWSGlueServiceRole"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  roles      = [aws_iam_role.glue_role.name]
}

# MSK - Create IAM role for AWS MSK
resource "aws_iam_role" "iam_for_msk" {
  name = "msk_role"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": "kafka.amazonaws.com"
        }
      }
    ]
  })
}

# MSK - IAM Policy for MSK
resource "aws_iam_policy" "msk_policy" {
  name        = "ddsl-msk-${var.environment}-policy"
  description = "IAM policy for MSK access"
  
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowMSKClusterAccess",
        Effect = "Allow",
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:CreateTopic",
          "kafka-cluster:DescribeCluster"
        ],
        Resource = "arn:aws:kafka:${var.region}:${var.aws_account_id}:cluster/ddsl-kafka-dev/*"
      },
      {
        Sid    = "AllowKafkaTopicAccess",
        Effect = "Allow",
        Action = [
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:WriteDataIdempotently",
          "kafka-cluster:AlterTopic",
          "kafka-cluster:AlterTopicDynamicConfiguration",
          "kafka-cluster:AlterClusterDynamicConfiguration",
          "kafka-cluster:DescribeTopicDynamicConfiguration",
          "kafka-cluster:DescribeClusterDynamicConfiguration",
          "kafka-cluster:ReadData"
        ],
        Resource = "arn:aws:kafka:${var.region}:${var.aws_account_id}:topic/ddsl-kafka-dev/*"
      },
      {
        Sid    = "AllowKafkaClusterGroupAccess",
        Effect = "Allow",
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ],
        Resource = "arn:aws:kafka:${var.region}:${var.aws_account_id}:group/ddsl-kafka-dev/*"
      }
    ]
  })

  tags = merge(var.tags, {
    Name = lower(join("-", [var.appname, "msk", "policy", var.environment]))
  })
}

#MSK Resource for IAM Policy Attachement

# MSK - AWS Resource for IAM Policy Attachment
resource "aws_iam_role_policy_attachment" "msk_policy_attachment" {
  role       = aws_iam_role.iam_for_msk.name
  policy_arn  = aws_iam_policy.msk_policy.arn
}

