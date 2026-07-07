package pgruntime

import "hash/fnv"

// NewBucketAssigner returns an assigner that hashes the namespace to determine
// the bucket. All resources in the same namespace (cluster) land in the same
// bucket, giving cluster-level affinity for operator sharding.
func NewBucketAssigner(bucketCount int) func(ns, name string) int {
	return func(ns, _ string) int {
		h := fnv.New32a()
		h.Write([]byte(ns))
		return int(h.Sum32() % uint32(bucketCount))
	}
}

// BucketSlice returns the bucket IDs owned by a specific replica.
// bucketCount must be divisible by replicaCount.
func BucketSlice(bucketCount, replicaCount, ordinal int) []int {
	perReplica := bucketCount / replicaCount
	start := ordinal * perReplica
	ids := make([]int, perReplica)
	for i := range ids {
		ids[i] = start + i
	}
	return ids
}

// AllBuckets returns all bucket IDs [0..bucketCount-1].
func AllBuckets(bucketCount int) []int {
	ids := make([]int, bucketCount)
	for i := range ids {
		ids[i] = i
	}
	return ids
}
