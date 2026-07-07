package pgruntime

import (
	"fmt"
	"testing"
)

func TestNewBucketAssigner_Deterministic(t *testing.T) {
	assign := NewBucketAssigner(32)

	for _, ns := range []string{"cluster-abc", "cluster-xyz", "_", ""} {
		first := assign(ns, "resource-1")
		for i := 0; i < 100; i++ {
			if got := assign(ns, fmt.Sprintf("resource-%d", i)); got != first {
				t.Errorf("assign(%q, resource-%d) = %d, want %d (same namespace must give same bucket)", ns, i, got, first)
			}
		}
	}
}

func TestNewBucketAssigner_Range(t *testing.T) {
	for _, bucketCount := range []int{1, 4, 32, 128} {
		assign := NewBucketAssigner(bucketCount)
		for i := 0; i < 1000; i++ {
			ns := fmt.Sprintf("cluster-%d", i)
			bucket := assign(ns, "x")
			if bucket < 0 || bucket >= bucketCount {
				t.Errorf("assign(%q) = %d, out of range [0, %d)", ns, bucket, bucketCount)
			}
		}
	}
}

func TestNewBucketAssigner_Distribution(t *testing.T) {
	bucketCount := 32
	assign := NewBucketAssigner(bucketCount)
	counts := make([]int, bucketCount)

	n := 10000
	for i := 0; i < n; i++ {
		ns := fmt.Sprintf("cluster-%d", i)
		counts[assign(ns, "x")]++
	}

	expected := n / bucketCount
	for b, c := range counts {
		if c < expected/3 || c > expected*3 {
			t.Errorf("bucket %d has %d items (expected ~%d), distribution is poor", b, c, expected)
		}
	}
}

func TestBucketSlice(t *testing.T) {
	tests := []struct {
		bucketCount  int
		replicaCount int
		ordinal      int
		want         []int
	}{
		{32, 2, 0, seq(0, 16)},
		{32, 2, 1, seq(16, 16)},
		{32, 4, 0, seq(0, 8)},
		{32, 4, 3, seq(24, 8)},
		{32, 1, 0, seq(0, 32)},
		{1, 1, 0, []int{0}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d/%d/ord%d", tt.bucketCount, tt.replicaCount, tt.ordinal), func(t *testing.T) {
			got := BucketSlice(tt.bucketCount, tt.replicaCount, tt.ordinal)
			if len(got) != len(tt.want) {
				t.Fatalf("BucketSlice(%d, %d, %d) = %v (len %d), want len %d", tt.bucketCount, tt.replicaCount, tt.ordinal, got, len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("BucketSlice[%d] = %d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestAllBuckets(t *testing.T) {
	for _, n := range []int{1, 4, 32} {
		got := AllBuckets(n)
		if len(got) != n {
			t.Fatalf("AllBuckets(%d) has length %d", n, len(got))
		}
		for i, v := range got {
			if v != i {
				t.Errorf("AllBuckets(%d)[%d] = %d, want %d", n, i, v, i)
			}
		}
	}
}

func seq(start, count int) []int {
	s := make([]int, count)
	for i := range s {
		s[i] = start + i
	}
	return s
}
