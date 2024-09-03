# #Cloudwatch -# Resource creation for IAM role for Cloudwatch
# resource "aws_iam_role" "cloudtrail_cloudwatch_events_role" {
#   name               = "cloudtrail_cloudwatch_events_role"
#   path               = "/"
#   assume_role_policy = <<EOF
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Principal": {
#         "Service": "cloudtrail.amazonaws.com"
#       },
#       "Action": "sts:AssumeRole"
#     }
#   ]
# }
# EOF
# }


# # Cloudwatch -Resource creation for IAM role policy for Cloudwatch
# resource "aws_iam_role_policy" "aws_iam_role_policy_cloudTrail_cloudWatch" {
#   name = "cloudTrail-cloudWatch-policy"
#   role = aws_iam_role.cloudtrail_cloudwatch_events_role.id

#   policy = <<EOF
# {
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Sid": "AWSCloudTrailCreateLogStream2014110",
#             "Effect": "Allow",
#             "Action": [
#                 "logs:CreateLogStream"
#             ],
#             "Resource": [
#                 "${aws_cloudwatch_log_group.cloudtrail_log_group.arn}:*"
#             ]
#         },
#         {
#             "Sid": "AWSCloudTrailPutLogEvents20141101",
#             "Effect": "Allow",
#             "Action": [
#                 "logs:PutLogEvents"
#             ],
#             "Resource": [
#                 "${aws_cloudwatch_log_group.cloudtrail_log_group.arn}:*"
#             ]
#         }
#     ]
# }
# EOF
# }