# -----------------------------------------------------------------------------
# EC2
# -----------------------------------------------------------------------------

output "ec2_instance_ip" {
  description = "Public IP of the load test harness instance"
  value       = aws_instance.harness.public_ip
}

output "ssh_command" {
  description = "SSH command to connect to the harness instance"
  value       = "ssh -i ~/.ssh/${var.ec2_key_name}.pem ec2-user@${aws_instance.harness.public_ip}"
}

# -----------------------------------------------------------------------------
# RDS
# -----------------------------------------------------------------------------

output "rds_endpoint" {
  description = "RDS instance endpoint (host:port)"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_connection_string" {
  description = "PostgreSQL connection string"
  value       = "postgresql://pgctl:${random_password.rds_master.result}@${aws_db_instance.postgres.endpoint}/pgctl"
  sensitive   = true
}

output "rds_identifier" {
  description = "RDS instance identifier (for CloudWatch dimension filter)"
  value       = aws_db_instance.postgres.identifier
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${var.project_name}"
}
