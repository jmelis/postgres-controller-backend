data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = var.project_name
  cidr = "10.0.0.0/16"

  azs              = local.azs
  public_subnets   = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
  database_subnets = ["10.0.201.0/24", "10.0.202.0/24", "10.0.203.0/24"]

  create_database_subnet_group       = true
  database_subnet_group_name         = "${var.project_name}-db"
  create_database_subnet_route_table = true

  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Project = var.project_name
  }
}
