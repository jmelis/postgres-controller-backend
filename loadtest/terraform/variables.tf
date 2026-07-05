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
# EKS
# -----------------------------------------------------------------------------

variable "eks_node_instance_type" {
  description = "EC2 instance type for the EKS managed node group"
  type        = string
  default     = "m5.2xlarge"
}

variable "eks_node_count" {
  description = "Desired number of EKS worker nodes"
  type        = number
  default     = 2
}

variable "eks_node_min_count" {
  description = "Minimum number of EKS worker nodes"
  type        = number
  default     = 1
}

variable "eks_node_max_count" {
  description = "Maximum number of EKS worker nodes"
  type        = number
  default     = 4
}

variable "kubernetes_version" {
  description = "Kubernetes version for the EKS cluster"
  type        = string
  default     = "1.30"
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
