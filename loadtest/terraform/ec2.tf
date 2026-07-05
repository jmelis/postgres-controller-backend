# -----------------------------------------------------------------------------
# Security group for the EC2 harness instance.
# -----------------------------------------------------------------------------

resource "aws_security_group" "ec2" {
  name_prefix = "${var.project_name}-ec2-"
  description = "Allow SSH inbound, all outbound for load test harness"
  vpc_id      = module.vpc.vpc_id

  tags = {
    Name = "${var.project_name}-ec2"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group_rule" "ec2_ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  description       = "SSH access"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.ec2.id
}

resource "aws_security_group_rule" "ec2_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.ec2.id
}

# -----------------------------------------------------------------------------
# IAM role for the EC2 instance (CloudWatch Agent metrics push).
# -----------------------------------------------------------------------------

resource "aws_iam_role" "ec2" {
  name = "${var.project_name}-ec2"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_cloudwatch" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project_name}-ec2"
  role = aws_iam_role.ec2.name
}

# -----------------------------------------------------------------------------
# EC2 instance — Amazon Linux 2023, public subnet, runs the harness binary.
# -----------------------------------------------------------------------------

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "harness" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.ec2_instance_type
  key_name               = var.ec2_key_name
  subnet_id              = module.vpc.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name

  associate_public_ip_address = true

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = <<-USERDATA
    #!/bin/bash
    set -ex

    # Install CloudWatch Agent.
    dnf install -y amazon-cloudwatch-agent

    # Create directories for the harness.
    mkdir -p /opt/loadtest/results
    USERDATA

  tags = {
    Name = "${var.project_name}-harness"
  }
}
