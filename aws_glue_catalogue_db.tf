resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  catalog_id = var.catalogue_id
  description  = var.description
  location_uri = var.location_uri
  name         = var.name 
  parameters   = var.parameters
}