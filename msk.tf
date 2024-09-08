resource "aws_msk_configuration" "msk_configuration_dev" {
  name             = "msk-configuration-dev"
  kafka_versions   = ["2.8.0"]
  server_properties = <<PROPERTIES
auto.create.topics.enable = true
log.retention.hours = 168
PROPERTIES
}

resource "aws_msk_cluster" "example" {
  cluster_name           = "ddsl-kafka-dev"
  kafka_version          = "2.8.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = ["subnet-012e8b38d36b72f5d", "subnet-08df7c4a7cfc4e6d8"]
    security_groups = [aws_security_group.msk_sg.id]

    # ebs_volume_size = 1000  # EBS volume size for broker node
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.msk_configuration_dev.arn
    revision = aws_msk_configuration.msk_configuration_dev.latest_revision
  }
}
