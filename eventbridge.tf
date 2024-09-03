# Resource rule creation for Eventbridge to send events from s3 to Eventbridge
resource "aws_cloudwatch_event_rule" "event_from_s3" {
  name = "s3_put_object_event"
  description = "Capture s3 object creation event"
  event_pattern = jsonencode(
    {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail":{
            "bucket": {
                "name": ["${aws_s3_bucket.example1.id}"]
            }
        }
    }
  )  
}
# Resource to trigger step function every 2 mins 
resource "aws_cloudwatch_event_rule" "step_function_trigger_event_rule" {
  name                = "trigger-step-function"
  description         = "Trigger every 2 min"
  schedule_expression = "rate(2 minutes)"  
  #event_bus_name     = "scheduled_stepfunction_trigger" 
  event_bus_name      = "default"
  event_pattern = jsonencode(
    {
        "source": ["aws.s3"],
        "detail-type": ["Object Created"],
        "detail":{
            "bucket": {
                "name": ["${aws_s3_bucket.example1.id}"]
            }
        }
    }
  )  
}

#Resource creation for AWS Cloud Watch event target to store the events in target cloudwatch
resource "aws_cloudwatch_event_target" "cloudwatch_target" {
  target_id = "cloudwatchtarget"
  rule = aws_cloudwatch_event_rule.event_from_s3.name
  arn  = "${aws_cloudwatch_log_group.eventbridge_log_group.arn}" 
  input_transformer {
    input_paths = {
      object = "$.detail.object.key",
      bucket   = "$.detail.bucket.name",
      timestamp = "$.time"
    }
    input_template = <<EOF
   {
      "timestamp":<timestamp>,
      "message":"<object> uploaded in <bucket> at <timestamp>"
   } 
    EOF
  }
}

resource "aws_cloudwatch_event_permission" "allow_s3_cloudwatch_permission" {
  principal      = "590183849298"
  statement_id   = "AllowSameAccountRole"
  action         = "events:PutEvents"  
}

#Resource creation for AWS Cloud Watch event target to store the events in target stepfunction 
resource "aws_cloudwatch_event_target" "stepfunction_target" {
  target_id = "stepfunctiontarget"
  rule = aws_cloudwatch_event_rule.event_from_s3.name
  #arn  = "${aws_cloudwatch_log_group.stepfunction_log_group.arn}" 
  arn  = "${aws_sfn_state_machine.sfn_state_machine.arn}"
  role_arn = aws_iam_role.iam_for_sfn.arn
}