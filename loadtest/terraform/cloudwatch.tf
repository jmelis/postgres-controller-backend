# -----------------------------------------------------------------------------
# IAM role for CloudWatch Agent sidecar (EKS Pod Identity).
# Allows pushing metrics scraped from the harness's /metrics endpoint.
# -----------------------------------------------------------------------------

resource "aws_iam_role" "cloudwatch_agent" {
  name = "${var.project_name}-cw-agent"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "pods.eks.amazonaws.com"
        }
        Action = [
          "sts:AssumeRole",
          "sts:TagSession",
        ]
      }
    ]
  })

  tags = {
    Name = "${var.project_name}-cw-agent"
  }
}

resource "aws_iam_role_policy_attachment" "cloudwatch_agent" {
  role       = aws_iam_role.cloudwatch_agent.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

resource "aws_eks_pod_identity_association" "cloudwatch_agent" {
  cluster_name    = module.eks.cluster_name
  namespace       = "pgctl-loadtest"
  service_account = "pgctl-loadtest"
  role_arn        = aws_iam_role.cloudwatch_agent.arn
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard — RDS metrics + pgctl/loadtest metrics in one view.
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "loadtest" {
  dashboard_name = var.project_name

  dashboard_body = jsonencode({
    widgets = concat(
      # --- Row 1: RDS health overview ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 0
          width  = 12
          height = 6
          properties = {
            title  = "RDS CPU Utilization"
            region = var.region
            metrics = [
              ["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", aws_db_instance.postgres.identifier]
            ]
            stat   = "Average"
            period = 60
            yAxis  = { left = { min = 0, max = 100 } }
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 0
          width  = 12
          height = 6
          properties = {
            title  = "RDS Freeable Memory"
            region = var.region
            metrics = [
              ["AWS/RDS", "FreeableMemory", "DBInstanceIdentifier", aws_db_instance.postgres.identifier]
            ]
            stat   = "Average"
            period = 60
            yAxis  = { left = { min = 0 } }
          }
        },
      ],
      # --- Row 2: RDS IOPS ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 6
          width  = 12
          height = 6
          properties = {
            title  = "RDS Read / Write IOPS"
            region = var.region
            metrics = [
              ["AWS/RDS", "ReadIOPS", "DBInstanceIdentifier", aws_db_instance.postgres.identifier],
              ["AWS/RDS", "WriteIOPS", "DBInstanceIdentifier", aws_db_instance.postgres.identifier],
            ]
            stat   = "Average"
            period = 60
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 6
          width  = 12
          height = 6
          properties = {
            title  = "RDS Disk Queue Depth"
            region = var.region
            metrics = [
              ["AWS/RDS", "DiskQueueDepth", "DBInstanceIdentifier", aws_db_instance.postgres.identifier]
            ]
            stat   = "Average"
            period = 60
          }
        },
      ],
      # --- Row 3: RDS connections + throughput ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 12
          width  = 12
          height = 6
          properties = {
            title  = "RDS Database Connections"
            region = var.region
            metrics = [
              ["AWS/RDS", "DatabaseConnections", "DBInstanceIdentifier", aws_db_instance.postgres.identifier]
            ]
            stat   = "Average"
            period = 60
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 12
          width  = 12
          height = 6
          properties = {
            title  = "RDS Read / Write Throughput"
            region = var.region
            metrics = [
              ["AWS/RDS", "ReadThroughput", "DBInstanceIdentifier", aws_db_instance.postgres.identifier],
              ["AWS/RDS", "WriteThroughput", "DBInstanceIdentifier", aws_db_instance.postgres.identifier],
            ]
            stat   = "Average"
            period = 60
          }
        },
      ],
      # --- Row 4: RDS replication + swap ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 18
          width  = 12
          height = 6
          properties = {
            title  = "RDS Replica Lag"
            region = var.region
            metrics = [
              ["AWS/RDS", "ReplicaLag", "DBInstanceIdentifier", aws_db_instance.postgres.identifier]
            ]
            stat   = "Maximum"
            period = 60
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 18
          width  = 12
          height = 6
          properties = {
            title  = "RDS Swap Usage"
            region = var.region
            metrics = [
              ["AWS/RDS", "SwapUsage", "DBInstanceIdentifier", aws_db_instance.postgres.identifier]
            ]
            stat   = "Average"
            period = 60
            yAxis  = { left = { min = 0 } }
          }
        },
      ],
      # --- Row 5: pgctl write RPS + latency (from CW Agent) ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 24
          width  = 12
          height = 6
          properties = {
            title  = "pgctl Write RPS (by bucket)"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest,bucket_id} MetricName=\"pgctl_writer_writes_total\"', 'Sum', 60)", id = "writes", label = "" }]
            ]
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 24
          width  = 12
          height = 6
          properties = {
            title  = "pgctl Write Latency p99"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest} MetricName=\"pgctl_writer_write_duration_seconds\"', 'p99', 60)", id = "p99", label = "" }]
            ]
          }
        },
      ],
      # --- Row 6: Harness metrics ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 30
          width  = 12
          height = 6
          properties = {
            title  = "Harness Write RPS (by phase)"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest,phase} MetricName=\"loadtest_writes_total\"', 'Sum', 60)", id = "hwrites", label = "" }]
            ]
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 30
          width  = 12
          height = 6
          properties = {
            title  = "Harness Errors (by type)"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest,phase,error_type} MetricName=\"loadtest_errors_total\"', 'Sum', 60)", id = "herr", label = "" }]
            ]
          }
        },
      ],
      # --- Row 7: Verifier + watcher ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 36
          width  = 12
          height = 6
          properties = {
            title  = "Verifier Violations"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest} MetricName=\"pgctl_verifier_violations_total\"', 'Sum', 60)", id = "vv", label = "" }]
            ]
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 36
          width  = 12
          height = 6
          properties = {
            title  = "Watcher Poll Duration p99"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest} MetricName=\"pgctl_watcher_poll_duration_seconds\"', 'p99', 60)", id = "poll", label = "" }]
            ]
          }
        },
      ],
      # --- Row 8: Delivery latency + lease ---
      [
        {
          type   = "metric"
          x      = 0
          y      = 42
          width  = 12
          height = 6
          properties = {
            title  = "Canary Delivery Latency p99"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest} MetricName=\"pgctl_verifier_canary_delivery_seconds\"', 'p99', 60)", id = "canary", label = "" }]
            ]
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 42
          width  = 12
          height = 6
          properties = {
            title  = "Lease Acquisitions"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest} MetricName=\"pgctl_lease_acquisitions_total\"', 'Sum', 60)", id = "lease", label = "" }]
            ]
          }
        },
      ]
    )
  })
}
