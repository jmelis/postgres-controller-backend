module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "${var.project_name}-eks"
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # Allow public access to the API server for convenience during load tests.
  cluster_endpoint_public_access = true

  # Cluster access — let the caller admin the cluster.
  enable_cluster_creator_admin_permissions = true

  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    eks-pod-identity-agent = {
      most_recent = true
    }
  }

  eks_managed_node_groups = {
    loadtest = {
      name = "${var.project_name}-ng"

      instance_types = [var.eks_node_instance_type]
      capacity_type  = "ON_DEMAND"

      min_size     = var.eks_node_min_count
      max_size     = var.eks_node_max_count
      desired_size = var.eks_node_count

      # Place nodes in private subnets; they reach the internet via NAT.
      subnet_ids = module.vpc.private_subnets

      labels = {
        role = "loadtest"
      }

      tags = {
        Project = var.project_name
      }
    }
  }

  tags = {
    Project = var.project_name
  }
}
