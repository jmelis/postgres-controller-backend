package writer

import (
	"errors"
	"fmt"
)

var (
	ErrConflict      = errors.New("conflict: object version mismatch (409)")
	ErrAlreadyExists = errors.New("already exists: resource with this key already exists")
)

type AmbiguousCommitError struct {
	Cause     error
	GVK       string
	Namespace string
	Name      string
	Seq       int64
}

func (e *AmbiguousCommitError) Error() string {
	return fmt.Sprintf("ambiguous commit for %s/%s/%s seq=%d: %v",
		e.GVK, e.Namespace, e.Name, e.Seq, e.Cause)
}

func (e *AmbiguousCommitError) Unwrap() error {
	return e.Cause
}
