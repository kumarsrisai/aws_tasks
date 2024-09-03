
variable "region" {
  description = "AWS region"
  default     = "us-east-1"
}

# variable "name" {
#   description = "The name of the Glue catalogue database"
#   type        = string
#   default     = "tokyo_glue_catalogue"
#   }

# variable "description" {
#   description = "DB description"
#   type        = string
#   default     = "Demo DB"
# }

# variable "catalogue_id" {
#   description = "ID of the Glue catalogue to create the database"
#   type        = string
#   default     = ""
# }

# variable "location_uri" {
#   description = "The location of the db"
#   type        = string  
#   default     = "null" 
# }

# variable "parameters" {
#   description = "A map of key-value pairs that define parameters and properties of the database"
#   type        = map(string)
#   default     = {}
# }

# variable "target_database" {
#   description = "Configuration block for a target db"
#   default     = []
#   type = list(object({
#     catalogue_id = string,
#     database_name = string
#   }))
# }

# variable "database_name" {
#   description = "Glue db for results updation"
#   type        = string
#   default     = "null" 
# }

# variable "glue_crawler_description" {
#   description = "Description of the crawler"
#   type        = string
#   default     = "Tokyo Glue Crawler"
# }

# variable "role" {
#   description = "IAM role for the crawler"
#   type        = string
#   default     = "null" 
# }


variable "s3_bucket1" {
  description = "name of the bucket1"
  type = string
  default = "ddsl-raw-developer"
}

variable "s3_bucket2" {
  description = "name of the bucket2"
  type = string
  default = "ddsl-raw-extended-developer"
}

variable "s3_bucket3" {
  description = "name of the bucket3"
  type = string
  default = "ddsl-dq-devloper"
}

variable "s3_bucket4" {
  description = "name of the bucket4"
  type = string
  default = "ddsl-procedded-developer"
}

variable "s3_bucket5" {
  description = "name of the bucket5"
  type = string
  default = "ddsl-odsl-domain-developer"
}
