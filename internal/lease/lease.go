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

type Manager struct {
	conn     *pgx.Conn
	holderID string
}

func NewManager(conn *pgx.Conn, holderID string) *Manager {
	return &Manager{conn: conn, holderID: holderID}
}

// Acquire takes a lease on a bucket. If no lease exists or the existing lease
// has expired, it inserts/updates. Returns the new epoch.
func (m *Manager) Acquire(ctx context.Context, bucketID int, ttl time.Duration) (int64, error) {
	var epoch int64
	err := m.conn.QueryRow(ctx, `
		INSERT INTO bucket_leases (bucket_id, holder, epoch, expires_at)
		VALUES ($1, $2, 1, now() + $3::interval)
		ON CONFLICT (bucket_id) DO UPDATE
		SET holder = EXCLUDED.holder,
		    epoch = bucket_leases.epoch + 1,
		    expires_at = EXCLUDED.expires_at
		WHERE bucket_leases.expires_at < now()
		   OR bucket_leases.holder = EXCLUDED.holder
		RETURNING epoch`,
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
	tag, err := m.conn.Exec(ctx, `
		UPDATE bucket_leases
		SET expires_at = now() + $1::interval
		WHERE bucket_id = $2 AND holder = $3`,
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
	tag, err := m.conn.Exec(ctx, `
		DELETE FROM bucket_leases
		WHERE bucket_id = $1 AND holder = $2`,
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
	err := m.conn.QueryRow(ctx, `
		UPDATE bucket_leases
		SET holder = $1, epoch = epoch + 1, expires_at = now() + $2::interval
		WHERE bucket_id = $3
		RETURNING epoch`,
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
	err := m.conn.QueryRow(ctx, `
		SELECT holder, epoch, expires_at FROM bucket_leases WHERE bucket_id = $1`,
		bucketID).Scan(&info.Holder, &info.Epoch, &info.Expires)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lease get bucket %d: %w", bucketID, err)
	}
	return info, nil
}
