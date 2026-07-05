# -----------------------------------------------------------------------------
# EKS
# -----------------------------------------------------------------------------

output "eks_cluster_name" {
  description = "Name of the EKS cluster"
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API server endpoint"
  value       = module.eks.cluster_endpoint
}

output "eks_oidc_provider_arn" {
  description = "ARN of the OIDC provider for IRSA"
  value       = module.eks.oidc_provider_arn
}

output "kubeconfig_update_command" {
  description = "AWS CLI command to update your kubeconfig"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

# -----------------------------------------------------------------------------
# RDS
# -----------------------------------------------------------------------------

output "rds_endpoint" {
  description = "RDS instance endpoint (host:port)"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_hostname" {
  description = "RDS instance hostname"
  value       = aws_db_instance.postgres.address
}

output "rds_port" {
  description = "RDS instance port"
  value       = aws_db_instance.postgres.port
}

output "rds_connection_string" {
  description = "PostgreSQL connection string"
  value       = "postgresql://pgctl:${random_password.rds_master.result}@${aws_db_instance.postgres.endpoint}/pgctl"
  sensitive   = true
}

output "rds_master_password_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the RDS master password"
  value       = aws_secretsmanager_secret.rds_master_password.arn
}

output "rds_identifier" {
  description = "RDS instance identifier (for CloudWatch dimension filter)"
  value       = aws_db_instance.postgres.identifier
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${var.project_name}"
}

# -----------------------------------------------------------------------------
# VPC
# -----------------------------------------------------------------------------

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}
