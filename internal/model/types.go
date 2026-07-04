package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Resource struct {
	GVK               string
	Namespace         string
	Name              string
	UID               uuid.UUID
	BucketID          int
	GVKBucketSeq      int64
	ObjectVersion     int64
	Spec              json.RawMessage
	Status            json.RawMessage
	Metadata          json.RawMessage
	DeletionTimestamp *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type WriteRequest struct {
	GVK               string
	Namespace         string
	Name              string
	BucketID          int
	Spec              json.RawMessage
	Status            json.RawMessage
	Metadata          json.RawMessage
	DeletionTimestamp *time.Time
	ExpectedVersion   int64 // 0 for create, >0 for update
	LeaseHolder       string
	LeaseEpoch        int64
}

type WriteResult struct {
	Seq           int64
	ObjectVersion int64
	UID           uuid.UUID
}
