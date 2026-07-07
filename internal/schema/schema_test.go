package schema_test

import (
	"context"
	"sort"
	"testing"

	"github.com/jmelis/postgres-controller-backend/test/testinfra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateCreatesAllTables(t *testing.T) {
	if testing.Short() {
		t.Skip("requires postgres")
	}

	db := testinfra.StartPostgres(t)
	conn := db.Connect(t)
	ctx := context.Background()

	rows, err := conn.Query(ctx,
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`)
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())

	sort.Strings(tables)
	expected := []string{
		"cluster_epoch",
		"compaction_horizon",
		"gvk_bucket_counters",
		"kubernetes_resources",
	}
	assert.Equal(t, expected, tables)

	var timelineID int64
	err = conn.QueryRow(ctx, "SELECT timeline_id FROM cluster_epoch").Scan(&timelineID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), timelineID)
}
