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
# Aurora
# -----------------------------------------------------------------------------

output "rds_endpoint" {
  description = "Aurora cluster writer endpoint (host:port)"
  value       = "${aws_rds_cluster.postgres.endpoint}:${aws_rds_cluster.postgres.port}"
}

output "rds_connection_string" {
  description = "PostgreSQL connection string (Aurora cluster writer endpoint)"
  value       = "postgresql://pgctl:${urlencode(random_password.rds_master.result)}@${aws_rds_cluster.postgres.endpoint}:${aws_rds_cluster.postgres.port}/pgctl"
  sensitive   = true
}

output "rds_identifier" {
  description = "Aurora writer instance identifier (for CloudWatch dimension filter)"
  value       = aws_rds_cluster_instance.postgres[0].identifier
}

output "aurora_cluster_identifier" {
  description = "Aurora cluster identifier"
  value       = aws_rds_cluster.postgres.cluster_identifier
}

output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${var.project_name}"
}
