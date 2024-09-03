# resource "aws_cloudwatch_event_rule" "event_from_s3" {
#   name = "s3_put_object_event"
#   description = "Capture s3 object creation event"
#   event_pattern = jsonencode(
#     {
#         "source": ["aws.s3"],
#         "detail-type": ["Object Created"],
#         "detail":{
#             "bucket": {
#                 "name": ["${aws_s3_bucket.example1.id}"]
#             }
#         }
#     }
#   )  
# }

