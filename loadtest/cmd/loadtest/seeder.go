package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
	"github.com/jmelis/postgres-controller-backend/internal/writer"
)

// generatePayload produces a valid JSON blob of approximately sizeBytes.
// The structure is {"data":"<base64>","idx":N}.
func generatePayload(sizeBytes int, idx int) json.RawMessage {
	if sizeBytes <= 0 {
		return json.RawMessage(`{}`)
	}

	// The JSON overhead for {"data":"...","idx":999999} is ~25 bytes.
	// We subtract overhead and generate enough base64 to fill the rest.
	const overhead = 25
	dataLen := sizeBytes - overhead
	if dataLen < 0 {
		dataLen = 0
	}

	// base64 encodes 3 bytes into 4 chars, so we need ~3/4 of dataLen raw bytes
	rawLen := (dataLen * 3) / 4
	if rawLen < 1 {
		rawLen = 1
	}

	raw := make([]byte, rawLen)
	//nolint:gosec // non-cryptographic random for load test payloads
	rand.Read(raw)
	encoded := base64.StdEncoding.EncodeToString(raw)

	// Trim to exact target
	if len(encoded) > dataLen {
		encoded = encoded[:dataLen]
	}

	payload := fmt.Sprintf(`{"data":"%s","idx":%d}`, encoded, idx)
	return json.RawMessage(payload)
}

// Seed populates the database with objects according to the config.
// It writes objects round-robin across buckets.
func Seed(ctx context.Context, conn *pgx.Conn, cfg *Config) error {
	numBuckets := cfg.Cluster.Buckets

	wr := writer.New(conn, nil).WithMetrics(libWriterMetrics)
	totalSeeded := 0

	for _, gvkCfg := range cfg.Seed.GVKs {
		gvkSeeded := 0
		totalForGVK := gvkCfg.ObjectsPerBucket * numBuckets

		log.Printf("seeder: seeding %d objects for GVK %s (%d per bucket x %d buckets)",
			totalForGVK, gvkCfg.GVK, gvkCfg.ObjectsPerBucket, numBuckets)

		objIdx := 0
		for i := 0; i < gvkCfg.ObjectsPerBucket; i++ {
			for b := 1; b <= numBuckets; b++ {
				spec := generatePayload(gvkCfg.SpecSizeBytes, objIdx)
				status := generatePayload(gvkCfg.StatusSizeBytes, objIdx)
				metadata := generatePayload(gvkCfg.MetadataSizeBytes, objIdx)

				req := model.WriteRequest{
					GVK:       gvkCfg.GVK,
					Namespace: "loadtest-seed",
					Name:      fmt.Sprintf("seed-%s-%d", gvkCfg.GVK, objIdx),
					BucketID:  b,
					Spec:      spec,
					Status:    status,
					Metadata:  metadata,
				}

				if _, err := wr.Write(ctx, req); err != nil {
					return fmt.Errorf("seed write (gvk=%s, obj=%d, bucket=%d): %w",
						gvkCfg.GVK, objIdx, b, err)
				}

				objIdx++
				gvkSeeded++
				totalSeeded++

				if totalSeeded%1000 == 0 {
					log.Printf("seeder: progress %d objects seeded", totalSeeded)
				}
			}
		}

		seedObjectsTotal.WithLabelValues(gvkCfg.GVK).Add(float64(gvkSeeded))
		log.Printf("seeder: completed %d objects for GVK %s", gvkSeeded, gvkCfg.GVK)
	}

	log.Printf("seeder: total %d objects seeded across %d GVKs", totalSeeded, len(cfg.Seed.GVKs))

	return nil
}
