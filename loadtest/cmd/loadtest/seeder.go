package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

var awsRegions = []string{
	"us-east-1", "us-east-2", "us-west-1", "us-west-2",
	"eu-west-1", "eu-west-2", "eu-central-1",
	"ap-southeast-1", "ap-northeast-1",
}

var instanceTypes = []string{
	"m6a.xlarge", "m6a.2xlarge", "m6a.4xlarge",
	"m5.xlarge", "m5.2xlarge",
	"c6a.xlarge", "c6a.2xlarge",
	"r6a.xlarge", "r6a.2xlarge",
}

var ocpVersions = []string{
	"4.16.20", "4.16.18", "4.16.15",
	"4.15.30", "4.15.28",
}

const (
	gvkHostedCluster = "hypershift.openshift.io/v1beta1/HostedCluster"
	gvkNodePool      = "hypershift.openshift.io/v1beta1/HostedNodePool"
)

// hostedClusterSpec produces a realistic HostedCluster spec payload.
func hostedClusterSpec(idx int) map[string]any {
	region := awsRegions[idx%len(awsRegions)]
	ver := ocpVersions[idx%len(ocpVersions)]
	clusterID := fmt.Sprintf("cluster-%06d", idx)

	return map[string]any{
		"creatorARN": fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-Installer-Role", 100000000000+idx),
		"hostedCluster": map[string]any{
			"platform": map[string]any{
				"type": "AWS",
				"aws": map[string]any{
					"region":                region,
					"cloudProviderConfigRef": map[string]any{"name": "aws-provider-config"},
					"resourceTags": []map[string]string{
						{"key": "red-hat-managed", "value": "true"},
						{"key": "red-hat-clustertype", "value": "rosa"},
						{"key": "api.openshift.com/id", "value": clusterID},
						{"key": "api.openshift.com/environment", "value": "production"},
						{"key": "api.openshift.com/legal-entity-id", "value": fmt.Sprintf("le-%06d", idx%5000)},
					},
					"rolesRef": map[string]any{
						"ingressARN":              fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-Ingress-Role", 100000000000+idx),
						"imageRegistryARN":         fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-ImageRegistry-Role", 100000000000+idx),
						"storageARN":              fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-Storage-Role", 100000000000+idx),
						"networkARN":              fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-Network-Role", 100000000000+idx),
						"kubeCloudControllerARN":  fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-KubeCloud-Role", 100000000000+idx),
						"nodePoolManagementARN":   fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-NodePool-Role", 100000000000+idx),
						"controlPlaneOperatorARN": fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-CPO-Role", 100000000000+idx),
					},
					"endpointAccess": "Public",
				},
			},
			"networking": map[string]any{
				"clusterNetwork": []map[string]string{{"cidr": "10.128.0.0/14"}},
				"serviceNetwork": []map[string]string{{"cidr": "172.30.0.0/16"}},
				"machineNetwork": []map[string]string{{"cidr": "10.0.0.0/16"}},
				"networkType":    "OVNKubernetes",
			},
			"release": map[string]any{
				"image": fmt.Sprintf("quay.io/openshift-release-dev/ocp-release:%s-multi", ver),
			},
			"issuerURL": fmt.Sprintf("https://d%08x.cloudfront.net/%s", idx, clusterID),
			"etcd": map[string]any{
				"managementType": "Managed",
				"managed": map[string]any{
					"storage": map[string]any{
						"type":              "PersistentVolume",
						"persistentVolume":  map[string]any{"size": "8Gi", "storageClassName": "gp3-csi"},
					},
				},
			},
			"services": []map[string]any{
				{"service": "APIServer", "servicePublishingStrategy": map[string]any{"type": "LoadBalancer"}},
				{"service": "OAuthServer", "servicePublishingStrategy": map[string]any{"type": "Route"}},
				{"service": "Konnectivity", "servicePublishingStrategy": map[string]any{"type": "Route"}},
				{"service": "Ignition", "servicePublishingStrategy": map[string]any{"type": "Route"}},
				{"service": "OVNSbDb", "servicePublishingStrategy": map[string]any{"type": "Route"}},
			},
			"fips":              false,
			"controllerAvailabilityPolicy": "HighlyAvailable",
			"infrastructureAvailabilityPolicy": "HighlyAvailable",
			"dns": map[string]any{
				"baseDomain":         "hyperfleet.io",
				"publicZoneID":       fmt.Sprintf("Z%010d", idx),
				"privateZoneID":      fmt.Sprintf("Z%010dP", idx),
			},
			"pullSecret": map[string]any{"name": "pull-secret"},
			"sshKey":     map[string]any{"name": "ssh-key"},
			"configuration": map[string]any{
				"apiServer": map[string]any{
					"audit": map[string]any{"profile": "Default"},
				},
			},
		},
	}
}

// hostedClusterStatus produces a realistic HostedCluster status payload.
func hostedClusterStatus(idx int) map[string]any {
	ver := ocpVersions[idx%len(ocpVersions)]
	region := awsRegions[idx%len(awsRegions)]

	conditions := []map[string]any{
		{"type": "Ready", "status": "True", "reason": "AsExpected", "message": "All components are ready"},
		{"type": "Available", "status": "True", "reason": "AsExpected", "message": "Hosted control plane is available"},
		{"type": "Progressing", "status": "False", "reason": "AsExpected", "message": "Configuration is up to date"},
		{"type": "Degraded", "status": "False", "reason": "AsExpected", "message": "Hosted cluster is not degraded"},
		{"type": "InfrastructureReady", "status": "True", "reason": "AsExpected", "message": "Infrastructure is provisioned"},
		{"type": "KubeAPIServerAvailable", "status": "True", "reason": "AsExpected", "message": "Kube API server is available at the endpoint"},
		{"type": "EtcdAvailable", "status": "True", "reason": "AsExpected", "message": "Etcd cluster is operational"},
		{"type": "ValidHostedControlPlaneConfiguration", "status": "True", "reason": "AsExpected", "message": "Configuration passes validation"},
		{"type": "CloudResourcesDestroyed", "status": "False", "reason": "NotDeleting", "message": "Cluster is not being deleted"},
		{"type": "ExternalDNSReachable", "status": "True", "reason": "AsExpected", "message": "External DNS records are resolvable"},
		{"type": "ValidOIDCConfiguration", "status": "True", "reason": "AsExpected", "message": "OIDC provider configuration is valid"},
		{"type": "ValidReleaseImage", "status": "True", "reason": "AsExpected", "message": "Release image is valid and pullable"},
		{"type": "ReconciliationActive", "status": "True", "reason": "AsExpected", "message": "Reconciliation is active"},
		{"type": "ReconciliationSucceeded", "status": "True", "reason": "AsExpected", "message": "Reconciliation completed successfully"},
		{"type": "ClusterVersionAvailable", "status": "True", "reason": "AsExpected", "message": fmt.Sprintf("Cluster version %s is available", ver)},
		{"type": "ClusterVersionProgressing", "status": "False", "reason": "AsExpected", "message": "Cluster version is up to date"},
	}

	return map[string]any{
		"observedGeneration": idx + 1,
		"phase":              "Ready",
		"version":            ver,
		"controlPlaneEndpoint": map[string]any{
			"host": fmt.Sprintf("api.seed-%06d.%s.hyperfleet.io", idx, region),
			"port": 6443,
		},
		"conditions": conditions,
		"placementRef": map[string]any{
			"name":              fmt.Sprintf("placement-%s-%02d", region, idx%10),
			"managementCluster": fmt.Sprintf("mc-%s-%02d", region, idx%5),
		},
		"platform": map[string]any{
			"aws": map[string]any{
				"defaultWorkerSecurityGroupID": fmt.Sprintf("sg-%012x", idx),
			},
		},
		"oauthCallbackURLTemplate": fmt.Sprintf("https://oauth.seed-%06d.%s.hyperfleet.io:443/oauthcallback/[identity-provider-name]", idx, region),
	}
}

// hostedClusterMetadata produces realistic HostedCluster metadata (labels + annotations).
func hostedClusterMetadata(idx int) map[string]any {
	region := awsRegions[idx%len(awsRegions)]
	clusterID := fmt.Sprintf("cluster-%06d", idx)
	envs := []string{"production", "staging", "development"}

	return map[string]any{
		"labels": map[string]string{
			"hyperfleet.io/account-id":            fmt.Sprintf("acct-%06d", idx%5000),
			"hyperfleet.io/region":                region,
			"hyperfleet.io/environment":           envs[idx%len(envs)],
			"hyperfleet.io/cluster-id":            clusterID,
			"hyperfleet.io/management-cluster":    fmt.Sprintf("mc-%s-%02d", region, idx%5),
			"api.openshift.com/id":                clusterID,
			"api.openshift.com/legal-entity-id":   fmt.Sprintf("le-%06d", idx%5000),
			"api.openshift.com/name":              fmt.Sprintf("seed-%06d", idx),
		},
		"annotations": map[string]string{
			"hyperfleet.io/managed-by":               "rosa-hyperfleet-api",
			"hyperfleet.io/created-by":               fmt.Sprintf("arn:aws:iam::%012d:role/ManagedOpenShift-Installer-Role", 100000000000+idx),
			"hyperfleet.io/provisioner":               "hypershift",
			"hyperfleet.io/dns-base-domain":           "hyperfleet.io",
			"hyperfleet.io/billing-model":             "marketplace-aws",
			"hyperfleet.io/oidc-issuer-url":           fmt.Sprintf("https://d%08x.cloudfront.net/%s", idx, clusterID),
			"api.openshift.com/shard-id":              fmt.Sprintf("shard-%02d", idx%20),
		},
	}
}

// nodePoolSpec produces a realistic NodePool spec payload.
func nodePoolSpec(idx int) map[string]any {
	instType := instanceTypes[idx%len(instanceTypes)]
	ver := ocpVersions[idx%len(ocpVersions)]
	region := awsRegions[idx%len(awsRegions)]
	replicas := 2 + idx%5 // 2-6 replicas

	return map[string]any{
		"clusterName": fmt.Sprintf("seed-%06d", idx/5),
		"replicas":    replicas,
		"nodePool": map[string]any{
			"platform": map[string]any{
				"type": "AWS",
				"aws": map[string]any{
					"instanceType": instType,
					"rootVolume": map[string]any{
						"type":          "gp3",
						"size":          300,
						"iops":          3000,
						"throughput":    125,
						"encryptionKey": fmt.Sprintf("arn:aws:kms:%s:%012d:key/mrk-%032x", region, 100000000000+idx, idx),
					},
					"securityGroups": []map[string]any{
						{"id": fmt.Sprintf("sg-%012x", idx)},
						{"id": fmt.Sprintf("sg-%012x", idx+10000000)},
					},
					"subnetID":      fmt.Sprintf("subnet-%012x", idx),
					"instanceProfile": fmt.Sprintf("arn:aws:iam::%012d:instance-profile/ManagedOpenShift-Worker-Profile", 100000000000+idx),
				},
			},
		},
		"management": map[string]any{
			"autoRepair": true,
			"replace": map[string]any{
				"strategy":      "RollingUpdate",
				"rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 1},
			},
			"upgradeType": "Replace",
		},
		"release": map[string]any{
			"image": fmt.Sprintf("quay.io/openshift-release-dev/ocp-release:%s-multi", ver),
		},
		"autoScaling": map[string]any{
			"min": replicas,
			"max": replicas * 3,
		},
		"config": []map[string]any{
			{"apiVersion": "machineconfiguration.openshift.io/v1", "kind": "KubeletConfig",
				"spec": map[string]any{"kubeletConfig": map[string]any{"maxPods": 250, "podsPerCore": 10}}},
		},
	}
}

// nodePoolStatus produces a realistic NodePool status payload.
func nodePoolStatus(idx int) map[string]any {
	ver := ocpVersions[idx%len(ocpVersions)]
	replicas := 2 + idx%5

	conditions := []map[string]any{
		{"type": "Ready", "status": "True", "reason": "AsExpected", "message": "NodePool is ready"},
		{"type": "Available", "status": "True", "reason": "AsExpected", "message": "All replicas are available"},
		{"type": "AllMachinesReady", "status": "True", "reason": "AsExpected", "message": "All machines have joined the cluster"},
		{"type": "AllNodesHealthy", "status": "True", "reason": "AsExpected", "message": "All nodes are reporting healthy status"},
		{"type": "AutoscalingEnabled", "status": "True", "reason": "AsExpected", "message": "Autoscaling is enabled"},
		{"type": "UpdateManagementEnabled", "status": "True", "reason": "AsExpected", "message": "Update management is active"},
		{"type": "AutorepairEnabled", "status": "True", "reason": "AsExpected", "message": "Auto-repair is enabled for this node pool"},
		{"type": "ReconciliationActive", "status": "True", "reason": "AsExpected", "message": "Reconciliation loop is active"},
		{"type": "ValidMachineConfig", "status": "True", "reason": "AsExpected", "message": "Machine config is valid and applied"},
		{"type": "ValidReleaseImage", "status": "True", "reason": "AsExpected", "message": "Release image is valid and pullable"},
	}

	return map[string]any{
		"replicas":          replicas,
		"availableReplicas": replicas,
		"readyReplicas":     replicas,
		"version":           ver,
		"conditions":        conditions,
		"platform": map[string]any{
			"aws": map[string]any{
				"instanceProfile": fmt.Sprintf("arn:aws:iam::%012d:instance-profile/ManagedOpenShift-Worker-Profile", 100000000000+idx),
				"securityGroups": []map[string]any{
					{"id": fmt.Sprintf("sg-%012x", idx)},
				},
			},
		},
	}
}

// nodePoolMetadata produces realistic NodePool metadata (labels + annotations).
func nodePoolMetadata(idx int) map[string]any {
	instType := instanceTypes[idx%len(instanceTypes)]
	region := awsRegions[idx%len(awsRegions)]

	return map[string]any{
		"labels": map[string]string{
			"hyperfleet.io/cluster-name":   fmt.Sprintf("seed-%06d", idx/5),
			"hyperfleet.io/instance-type":  instType,
			"hyperfleet.io/region":         region,
			"hyperfleet.io/node-pool-name": fmt.Sprintf("workers-%06d", idx),
			"hyperfleet.io/account-id":     fmt.Sprintf("acct-%06d", (idx/5)%5000),
		},
		"annotations": map[string]string{
			"hyperfleet.io/managed-by":      "rosa-hyperfleet-api",
			"hyperfleet.io/billing-model":   "marketplace-aws",
			"hyperfleet.io/auto-repair":     "true",
		},
	}
}

// padToSize marshals data to JSON and pads with annotation-like entries
// until it reaches at least targetBytes. Returns as-is if already large enough.
func padToSize(data map[string]any, targetBytes int) json.RawMessage {
	out, _ := json.Marshal(data)
	if len(out) >= targetBytes || targetBytes <= 0 {
		return json.RawMessage(out)
	}

	padding := make(map[string]string)
	i := 0
	for len(out) < targetBytes {
		key := fmt.Sprintf("openshift.io/loadtest-padding-%04d", i)
		remaining := targetBytes - len(out) - len(key) - 20 // overhead for JSON encoding
		remaining = max(remaining, 10)
		remaining = min(remaining, 200)
		padding[key] = strings.Repeat("x", remaining)
		data["_padding"] = padding
		out, _ = json.Marshal(data)
		i++
	}
	return json.RawMessage(out)
}

// generatePayload produces a valid JSON blob of approximately sizeBytes.
// Used as fallback for GVKs without a realistic template.
func generatePayload(sizeBytes int, idx int) json.RawMessage {
	if sizeBytes <= 0 {
		return json.RawMessage(`{}`)
	}

	const overhead = 25
	dataLen := max(sizeBytes-overhead, 0)

	rawLen := max((dataLen*3)/4, 1)

	raw := make([]byte, rawLen)
	//nolint:gosec // non-cryptographic random for load test payloads
	_, _ = rand.Read(raw) //nolint:staticcheck // math/rand.Read is fine for non-crypto load test data
	encoded := base64.StdEncoding.EncodeToString(raw)

	if len(encoded) > dataLen {
		encoded = encoded[:dataLen]
	}

	payload := fmt.Sprintf(`{"data":"%s","idx":%d}`, encoded, idx)
	return json.RawMessage(payload)
}

// generateSpec returns a realistic spec payload for the given GVK, falling
// back to a base64-padded blob for unknown GVKs.
func generateSpec(gvk string, sizeBytes, idx int) json.RawMessage {
	switch gvk {
	case gvkHostedCluster:
		return padToSize(hostedClusterSpec(idx), sizeBytes)
	case gvkNodePool:
		return padToSize(nodePoolSpec(idx), sizeBytes)
	default:
		return generatePayload(sizeBytes, idx)
	}
}

// generateStatus returns a realistic status payload for the given GVK.
func generateStatus(gvk string, sizeBytes, idx int) json.RawMessage {
	switch gvk {
	case gvkHostedCluster:
		return padToSize(hostedClusterStatus(idx), sizeBytes)
	case gvkNodePool:
		return padToSize(nodePoolStatus(idx), sizeBytes)
	default:
		return generatePayload(sizeBytes, idx)
	}
}

// generateMetadata returns a realistic metadata payload for the given GVK.
func generateMetadata(gvk string, sizeBytes, idx int) json.RawMessage {
	switch gvk {
	case gvkHostedCluster:
		return padToSize(hostedClusterMetadata(idx), sizeBytes)
	case gvkNodePool:
		return padToSize(nodePoolMetadata(idx), sizeBytes)
	default:
		return generatePayload(sizeBytes, idx)
	}
}

// Seed populates the database with objects according to the config.
func Seed(ctx context.Context, conn *pgx.Conn, cfg *Config) error {
	wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
	totalSeeded := 0

	for _, gvkCfg := range cfg.Seed.GVKs {
		gvkSeeded := 0

		log.Printf("seeder: seeding %d objects for GVK %s",
			gvkCfg.Objects, gvkCfg.GVK)

		for i := 0; i < gvkCfg.Objects; i++ {
			spec := generateSpec(gvkCfg.GVK, gvkCfg.SpecSizeBytes, i)
			status := generateStatus(gvkCfg.GVK, gvkCfg.StatusSizeBytes, i)
			metadata := generateMetadata(gvkCfg.GVK, gvkCfg.MetadataSizeBytes, i)

			req := model.WriteRequest{
				GVK:       gvkCfg.GVK,
				Namespace: "loadtest-seed",
				Name:      fmt.Sprintf("seed-%s-%d", gvkCfg.GVK, i),
				Spec:      spec,
				Status:    status,
				Metadata:  metadata,
			}

			if _, err := wr.Write(ctx, req); err != nil {
				return fmt.Errorf("seed write (gvk=%s, obj=%d): %w",
					gvkCfg.GVK, i, err)
			}

			gvkSeeded++
			totalSeeded++

			if totalSeeded%1000 == 0 {
				log.Printf("seeder: progress %d objects seeded", totalSeeded)
			}
		}

		seedObjectsTotal.WithLabelValues(gvkCfg.GVK).Add(float64(gvkSeeded))
		log.Printf("seeder: completed %d objects for GVK %s", gvkSeeded, gvkCfg.GVK)
	}

	log.Printf("seeder: total %d objects seeded across %d GVKs", totalSeeded, len(cfg.Seed.GVKs))

	return nil
}
