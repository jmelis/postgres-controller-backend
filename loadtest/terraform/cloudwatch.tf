# -----------------------------------------------------------------------------
# CloudWatch Dashboard — RDS metrics + pgctl/loadtest metrics in one view.
# The EC2 instance profile (ec2.tf) already has CloudWatchAgentServerPolicy.
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
            title  = "pgctl Write RPS"
            region = var.region
            metrics = [
              [{ expression = "SEARCH('{pgctl-loadtest,phase,gvk} MetricName=\"pgctl_writer_writes_total\"', 'Sum', 60)", id = "writes", label = "" }]
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
      # --- Row 8: Delivery latency ---
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
      ]
    )
  })
}
