# -----------------------------------------------------------------------------
# General
# -----------------------------------------------------------------------------

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name prefix used for all resources"
  type        = string
  default     = "pgctl-loadtest"
}

# -----------------------------------------------------------------------------
# EC2
# -----------------------------------------------------------------------------

variable "ec2_instance_type" {
  description = "EC2 instance type for the load test harness"
  type        = string
  default     = "m5.xlarge"
}

variable "ec2_key_name" {
  description = "EC2 key pair name for SSH access (must already exist in the region)"
  type        = string
}

# -----------------------------------------------------------------------------
# Aurora
# -----------------------------------------------------------------------------

variable "rds_instance_class" {
  description = "Aurora instance class (used for both writer and reader instances)"
  type        = string
  default     = "db.r6g.large"
}

variable "rds_engine_version" {
  description = "Aurora PostgreSQL engine version"
  type        = string
  default     = "16.8"
}

variable "aurora_reader_count" {
  description = "Number of Aurora reader instances (0 = writer only, 1+ = Multi-AZ with readers)"
  type        = number
  default     = 1
}
