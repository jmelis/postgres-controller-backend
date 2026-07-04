package reader

import "github.com/jmelisba/postgres-controller-backend/internal/model"

type EventType int

const (
	EventAdded EventType = iota
	EventModified
	EventDeleted
	EventBookmark
)

func (e EventType) String() string {
	switch e {
	case EventAdded:
		return "ADDED"
	case EventModified:
		return "MODIFIED"
	case EventDeleted:
		return "DELETED"
	case EventBookmark:
		return "BOOKMARK"
	default:
		return "UNKNOWN"
	}
}

type Event struct {
	Type     EventType
	Resource model.Resource
}
