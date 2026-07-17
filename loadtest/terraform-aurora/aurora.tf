# -----------------------------------------------------------------------------
# Security group: allow PostgreSQL traffic only from the EC2 harness.
# -----------------------------------------------------------------------------

resource "aws_security_group" "rds" {
  name_prefix = "${var.project_name}-aurora-"
  description = "Allow PostgreSQL inbound from EC2 harness only"
  vpc_id      = module.vpc.vpc_id

  tags = {
    Name = "${var.project_name}-aurora"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group_rule" "rds_ingress_ec2" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  description              = "PostgreSQL from EC2 harness"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = aws_security_group.ec2.id
}

resource "aws_security_group_rule" "rds_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.rds.id
}

# -----------------------------------------------------------------------------
# Aurora PostgreSQL cluster
# -----------------------------------------------------------------------------

resource "random_password" "rds_master" {
  length           = 32
  special          = true
  override_special = "!#$%^&*()-_=+"
}

resource "aws_secretsmanager_secret" "rds_master_password" {
  name                    = "${var.project_name}/aurora-master-password"
  description             = "Master password for the ${var.project_name} Aurora cluster"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "rds_master_password" {
  secret_id     = aws_secretsmanager_secret.rds_master_password.id
  secret_string = random_password.rds_master.result
}

resource "aws_rds_cluster_parameter_group" "aurora" {
  name_prefix = "${var.project_name}-aurora-pg16-cluster-"
  family      = "aurora-postgresql16"
  description = "Aurora PostgreSQL 16 cluster params for ${var.project_name}"

  parameter {
    name  = "log_min_duration_statement"
    value = "500"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_parameter_group" "aurora" {
  name_prefix = "${var.project_name}-aurora-pg16-"
  family      = "aurora-postgresql16"
  description = "Aurora PostgreSQL 16 instance params for ${var.project_name}"

  parameter {
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements"
    apply_method = "pending-reboot"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_rds_cluster" "postgres" {
  cluster_identifier = "${var.project_name}-aurora"

  engine         = "aurora-postgresql"
  engine_version = var.rds_engine_version

  database_name   = "pgctl"
  master_username = "pgctl"
  master_password = random_password.rds_master.result

  db_subnet_group_name            = module.vpc.database_subnet_group_name
  vpc_security_group_ids          = [aws_security_group.rds.id]
  db_cluster_parameter_group_name = aws_rds_cluster_parameter_group.aurora.name

  storage_type      = "aurora-iopt1"
  storage_encrypted = true

  backup_retention_period = 1
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  tags = {
    Name = "${var.project_name}-aurora"
  }
}

resource "aws_rds_cluster_instance" "postgres" {
  count = 1 + var.aurora_reader_count

  identifier         = "${var.project_name}-aurora-${count.index}"
  cluster_identifier = aws_rds_cluster.postgres.id

  engine         = aws_rds_cluster.postgres.engine
  engine_version = aws_rds_cluster.postgres.engine_version
  instance_class = var.rds_instance_class

  db_parameter_group_name = aws_db_parameter_group.aurora.name

  publicly_accessible          = false
  performance_insights_enabled = true
  apply_immediately            = true

  tags = {
    Name = "${var.project_name}-aurora-${count.index}"
    Role = count.index == 0 ? "writer" : "reader"
  }
}
