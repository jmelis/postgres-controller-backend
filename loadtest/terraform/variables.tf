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
# RDS
# -----------------------------------------------------------------------------

variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.r6g.large"
}

variable "rds_allocated_storage" {
  description = "Allocated storage in GB for the RDS instance"
  type        = number
  default     = 100
}

variable "rds_iops" {
  description = "Provisioned IOPS for gp3 storage"
  type        = number
  default     = 3000
}

variable "rds_multi_az" {
  description = "Enable Multi-AZ deployment for RDS"
  type        = bool
  default     = true
}

variable "rds_engine_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "16.4"
}

variable "rds_storage_throughput" {
  description = "Storage throughput in MiBps for gp3 (minimum 125)"
  type        = number
  default     = 125
}
