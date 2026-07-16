package writer

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// TxHooks allows tests to inject behavior at specific points in the write transaction.
// Production code passes nil.
type TxHooks interface {
	AfterSuppressionCheck(ctx context.Context, tx pgx.Tx, suppressed bool) error
	AfterTxidAcquire(ctx context.Context, tx pgx.Tx, txid uint64) error
	BeforeCommit(ctx context.Context, tx pgx.Tx) error
}
