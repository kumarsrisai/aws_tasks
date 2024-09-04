# #Resource to create Cloudwatch log group for logging the cloudtrail logs
# resource "aws_cloudwatch_log_group" "cloudtrail_log_group" {
#   name = "tokyo_cloudtrail_log"
#   tags = {
#     Name = "Cloudwatch for backuping CloudTrail"    
#   }
# }

# #Resource creation for AWS Cloud Watch log group
# resource "aws_cloudwatch_log_group" "eventbridge_log_group" {
#   name = "/aws/events/eventbridgelogs"

#   tags = {
#     Environment = "Dev"
#     Application = "POC"
#   }
# }

# #Resource creation for AWS Cloud Watch log group for AWS Step Function
# resource "aws_cloudwatch_log_group" "stepfunction_log_group" {
#   name = "/aws/events/stepfunctionlogs"  
# }

# #Resource creation for AWS Cloud Watch log group for AWS Glue
# resource "aws_cloudwatch_log_group" "glue_job_log_group" {
#   name = "/aws/events/gluejoblogs"  
# }

# #Resource creation for AWS Cloud Watch log group for Data quality 1
# resource "aws_cloudwatch_log_group" "data_quality_log_group1" {
#   name = "/aws/events/dataquality1logs"  
# }

# #Resource creation for AWS Cloud Watch log group for Data quality 2
# resource "aws_cloudwatch_log_group" "data_quality_log_group2" {
#   name = "/aws/events/dataquality2logs"  
# }

# #Resource creation for AWS Cloud Watch log group for Data Linage
# resource "aws_cloudwatch_log_group" "data_lineage" {
#   name = "/aws/events/datalienage"  
# }