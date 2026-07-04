package writer

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// TxHooks allows tests to inject behavior at specific points in the write transaction.
// Production code passes nil.
type TxHooks interface {
	AfterFence(ctx context.Context, tx pgx.Tx) error
	AfterCounter(ctx context.Context, tx pgx.Tx, seq int64) error
	BeforeCommit(ctx context.Context, tx pgx.Tx) error
}
