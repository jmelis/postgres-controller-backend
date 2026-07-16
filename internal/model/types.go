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
	TxidStamp         uint64
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
	Spec              json.RawMessage
	Status            json.RawMessage
	Metadata          json.RawMessage
	DeletionTimestamp *time.Time
	ExpectedVersion   int64 // 0 for create, >0 for update
	ForceWrite        bool  // skip no-op suppression; default false = suppress content-equal writes
}

type StatusWriteRequest struct {
	GVK             string
	Namespace       string
	Name            string
	Status          json.RawMessage
	ExpectedVersion int64
	ForceWrite      bool // skip no-op suppression; default false = suppress content-equal writes
}

type ObjectWriteRequest struct {
	GVK               string
	Namespace         string
	Name              string
	Spec              json.RawMessage
	Metadata          json.RawMessage
	DeletionTimestamp *time.Time
	ExpectedVersion   int64
	ForceWrite        bool
}

type WriteResult struct {
	Txid          uint64
	ObjectVersion int64
	UID           uuid.UUID
	Changed       bool // false when suppressed (content-equal write)
}
