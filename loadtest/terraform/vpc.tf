data "aws_availability_zones" "available" {
  state = "available"

  # Exclude local zones.
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

  azs             = local.azs
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  # Database subnets for RDS — no direct internet access.
  database_subnets                   = ["10.0.201.0/24", "10.0.202.0/24", "10.0.203.0/24"]
  create_database_subnet_group       = true
  database_subnet_group_name         = "${var.project_name}-db"
  create_database_subnet_route_table = true

  enable_nat_gateway = true
  single_nat_gateway = true # Cost-saving for test infra.

  enable_dns_hostnames = true
  enable_dns_support   = true

  # Tags required for EKS subnet auto-discovery.
  public_subnet_tags = {
    "kubernetes.io/role/elb"                        = 1
    "kubernetes.io/cluster/${var.project_name}-eks"  = "shared"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"                = 1
    "kubernetes.io/cluster/${var.project_name}-eks"  = "shared"
  }

  tags = {
    Project = var.project_name
  }
}
