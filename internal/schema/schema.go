package schema

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func Migrate(ctx context.Context, conn *pgx.Conn) error {
	sql, err := migrationsFS.ReadFile("migrations/001_initial.sql")
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	_, err = conn.Exec(ctx, string(sql))
	if err != nil {
		return fmt.Errorf("apply migration: %w", err)
	}
	return nil
}
