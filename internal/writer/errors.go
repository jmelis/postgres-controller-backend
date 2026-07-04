package writer

import (
	"errors"
	"fmt"

	"github.com/jmelisba/postgres-controller-backend/internal/model"
)

var (
	ErrFenceViolation = errors.New("fence violation: lease not held or epoch mismatch")
	ErrConflict       = errors.New("conflict: object version mismatch (409)")
)

type AmbiguousCommitError struct {
	Cause error
	Req   model.WriteRequest
	Seq   int64
}

func (e *AmbiguousCommitError) Error() string {
	return fmt.Sprintf("ambiguous commit for %s/%s/%s seq=%d: %v",
		e.Req.GVK, e.Req.Namespace, e.Req.Name, e.Seq, e.Cause)
}

func (e *AmbiguousCommitError) Unwrap() error {
	return e.Cause
}
