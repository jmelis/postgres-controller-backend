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

type noopHooks struct{}

func (noopHooks) AfterFence(context.Context, pgx.Tx) error          { return nil }
func (noopHooks) AfterCounter(context.Context, pgx.Tx, int64) error { return nil }
func (noopHooks) BeforeCommit(context.Context, pgx.Tx) error        { return nil }
