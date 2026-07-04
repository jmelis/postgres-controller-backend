package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrNotHolder    = errors.New("lease: not the current holder")
	ErrLeaseExpired = errors.New("lease: expired")
)

type Info struct {
	BucketID int
	Holder   string
	Epoch    int64
	Expires  time.Time
}

const (
	TableBucketSpecLeases   = "bucket_spec_leases"
	TableBucketStatusLeases = "bucket_status_leases"
)

var validTables = map[string]bool{
	TableBucketSpecLeases:   true,
	TableBucketStatusLeases: true,
}

type Manager struct {
	conn      *pgx.Conn
	holderID  string
	tableName string
}

func NewSpecManager(conn *pgx.Conn, holderID string) *Manager {
	return &Manager{conn: conn, holderID: holderID, tableName: TableBucketSpecLeases}
}

func NewStatusManager(conn *pgx.Conn, holderID string) *Manager {
	return &Manager{conn: conn, holderID: holderID, tableName: TableBucketStatusLeases}
}

// Acquire takes a lease on a bucket. If no lease exists or the existing lease
// has expired, it inserts/updates. Returns the new epoch.
func (m *Manager) Acquire(ctx context.Context, bucketID int, ttl time.Duration) (int64, error) {
	var epoch int64
	err := m.conn.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s (bucket_id, holder, epoch, expires_at)
		VALUES ($1, $2, 1, now() + $3::interval)
		ON CONFLICT (bucket_id) DO UPDATE
		SET holder = EXCLUDED.holder,
		    epoch = %s.epoch + 1,
		    expires_at = EXCLUDED.expires_at
		WHERE %s.expires_at < now()
		   OR %s.holder = EXCLUDED.holder
		RETURNING epoch`, m.tableName, m.tableName, m.tableName, m.tableName),
		bucketID, m.holderID, fmt.Sprintf("%d seconds", int(ttl.Seconds()))).Scan(&epoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("lease acquire bucket %d: held by another replica", bucketID)
		}
		return 0, fmt.Errorf("lease acquire bucket %d: %w", bucketID, err)
	}
	return epoch, nil
}

// Renew extends the TTL for a lease the caller currently holds.
func (m *Manager) Renew(ctx context.Context, bucketID int, ttl time.Duration) error {
	tag, err := m.conn.Exec(ctx, fmt.Sprintf(`
		UPDATE %s
		SET expires_at = now() + $1::interval
		WHERE bucket_id = $2 AND holder = $3`, m.tableName),
		fmt.Sprintf("%d seconds", int(ttl.Seconds())), bucketID, m.holderID)
	if err != nil {
		return fmt.Errorf("lease renew bucket %d: %w", bucketID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotHolder
	}
	return nil
}

// Release explicitly drops a lease (graceful shutdown).
func (m *Manager) Release(ctx context.Context, bucketID int) error {
	tag, err := m.conn.Exec(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE bucket_id = $1 AND holder = $2`, m.tableName),
		bucketID, m.holderID)
	if err != nil {
		return fmt.Errorf("lease release bucket %d: %w", bucketID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotHolder
	}
	return nil
}

// Grant is the coordinator's path: force-reassign a bucket to a new holder,
// bumping the epoch. The UPDATE takes an exclusive lock, which conflicts with
// any FOR SHARE lock held by an in-flight writer (I4 defense).
func (m *Manager) Grant(ctx context.Context, bucketID int, newHolder string, ttl time.Duration) (int64, error) {
	var epoch int64
	err := m.conn.QueryRow(ctx, fmt.Sprintf(`
		UPDATE %s
		SET holder = $1, epoch = epoch + 1, expires_at = now() + $2::interval
		WHERE bucket_id = $3
		RETURNING epoch`, m.tableName),
		newHolder, fmt.Sprintf("%d seconds", int(ttl.Seconds())), bucketID).Scan(&epoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("grant bucket %d: no existing lease", bucketID)
		}
		return 0, fmt.Errorf("grant bucket %d: %w", bucketID, err)
	}
	return epoch, nil
}

// Get reads the current lease info for a bucket.
func (m *Manager) Get(ctx context.Context, bucketID int) (*Info, error) {
	info := &Info{BucketID: bucketID}
	err := m.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT holder, epoch, expires_at FROM %s WHERE bucket_id = $1`, m.tableName),
		bucketID).Scan(&info.Holder, &info.Epoch, &info.Expires)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lease get bucket %d: %w", bucketID, err)
	}
	return info, nil
}

// BothManager acquires/renews/releases both spec and status leases for the
// same holder in a single transaction — the common case where one controller
// owns both sub-resources.
type BothManager struct {
	conn     *pgx.Conn
	holderID string
}

func NewBothManager(conn *pgx.Conn, holderID string) *BothManager {
	return &BothManager{conn: conn, holderID: holderID}
}

type BothEpochs struct {
	Spec   int64
	Status int64
}

func (b *BothManager) AcquireBoth(ctx context.Context, bucketID int, ttl time.Duration) (BothEpochs, error) {
	tx, err := b.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return BothEpochs{}, fmt.Errorf("acquire both begin: %w", err)
	}
	defer tx.Rollback(ctx)

	ttlStr := fmt.Sprintf("%d seconds", int(ttl.Seconds()))
	var specEpoch, statusEpoch int64

	for _, tbl := range []struct {
		name  string
		epoch *int64
	}{
		{TableBucketSpecLeases, &specEpoch},
		{TableBucketStatusLeases, &statusEpoch},
	} {
		err := tx.QueryRow(ctx, fmt.Sprintf(`
			INSERT INTO %s (bucket_id, holder, epoch, expires_at)
			VALUES ($1, $2, 1, now() + $3::interval)
			ON CONFLICT (bucket_id) DO UPDATE
			SET holder = EXCLUDED.holder,
			    epoch = %s.epoch + 1,
			    expires_at = EXCLUDED.expires_at
			WHERE %s.expires_at < now()
			   OR %s.holder = EXCLUDED.holder
			RETURNING epoch`, tbl.name, tbl.name, tbl.name, tbl.name),
			bucketID, b.holderID, ttlStr).Scan(tbl.epoch)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return BothEpochs{}, fmt.Errorf("acquire both bucket %d (%s): held by another replica", bucketID, tbl.name)
			}
			return BothEpochs{}, fmt.Errorf("acquire both bucket %d (%s): %w", bucketID, tbl.name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return BothEpochs{}, fmt.Errorf("acquire both commit: %w", err)
	}
	return BothEpochs{Spec: specEpoch, Status: statusEpoch}, nil
}

func (b *BothManager) RenewBoth(ctx context.Context, bucketID int, ttl time.Duration) error {
	tx, err := b.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("renew both begin: %w", err)
	}
	defer tx.Rollback(ctx)

	ttlStr := fmt.Sprintf("%d seconds", int(ttl.Seconds()))

	for _, tbl := range []string{TableBucketSpecLeases, TableBucketStatusLeases} {
		tag, err := tx.Exec(ctx, fmt.Sprintf(`
			UPDATE %s
			SET expires_at = now() + $1::interval
			WHERE bucket_id = $2 AND holder = $3`, tbl),
			ttlStr, bucketID, b.holderID)
		if err != nil {
			return fmt.Errorf("renew both bucket %d (%s): %w", bucketID, tbl, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("renew both bucket %d (%s): %w", bucketID, tbl, ErrNotHolder)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("renew both commit: %w", err)
	}
	return nil
}

func (b *BothManager) ReleaseBoth(ctx context.Context, bucketID int) error {
	tx, err := b.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("release both begin: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, tbl := range []string{TableBucketSpecLeases, TableBucketStatusLeases} {
		tag, err := tx.Exec(ctx, fmt.Sprintf(`
			DELETE FROM %s
			WHERE bucket_id = $1 AND holder = $2`, tbl),
			bucketID, b.holderID)
		if err != nil {
			return fmt.Errorf("release both bucket %d (%s): %w", bucketID, tbl, err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("release both bucket %d (%s): %w", bucketID, tbl, ErrNotHolder)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("release both commit: %w", err)
	}
	return nil
}
