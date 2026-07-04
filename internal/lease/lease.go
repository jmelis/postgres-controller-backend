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
	Domain   string
	Holder   string
	Epoch    int64
	Expires  time.Time
}

const (
	DomainSpec   = "spec"
	DomainStatus = "status"
)

type Manager struct {
	conn     *pgx.Conn
	holderID string
	domain   string
}

func NewSpecManager(conn *pgx.Conn, holderID string) *Manager {
	return &Manager{conn: conn, holderID: holderID, domain: DomainSpec}
}

func NewStatusManager(conn *pgx.Conn, holderID string) *Manager {
	return &Manager{conn: conn, holderID: holderID, domain: DomainStatus}
}

func (m *Manager) Acquire(ctx context.Context, bucketID int, ttl time.Duration) (int64, error) {
	var epoch int64
	err := m.conn.QueryRow(ctx, `
		INSERT INTO bucket_leases (bucket_id, domain, holder, epoch, expires_at)
		VALUES ($1, $2, $3, 1, now() + $4::interval)
		ON CONFLICT (bucket_id, domain) DO UPDATE
		SET holder = EXCLUDED.holder,
		    epoch = bucket_leases.epoch + 1,
		    expires_at = EXCLUDED.expires_at
		WHERE bucket_leases.expires_at < now()
		   OR bucket_leases.holder = EXCLUDED.holder
		RETURNING epoch`,
		bucketID, m.domain, m.holderID, fmt.Sprintf("%d seconds", int(ttl.Seconds()))).Scan(&epoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("lease acquire bucket %d: held by another replica", bucketID)
		}
		return 0, fmt.Errorf("lease acquire bucket %d: %w", bucketID, err)
	}
	return epoch, nil
}

func (m *Manager) Renew(ctx context.Context, bucketID int, ttl time.Duration) error {
	tag, err := m.conn.Exec(ctx, `
		UPDATE bucket_leases
		SET expires_at = now() + $1::interval
		WHERE bucket_id = $2 AND domain = $3 AND holder = $4`,
		fmt.Sprintf("%d seconds", int(ttl.Seconds())), bucketID, m.domain, m.holderID)
	if err != nil {
		return fmt.Errorf("lease renew bucket %d: %w", bucketID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotHolder
	}
	return nil
}

func (m *Manager) Release(ctx context.Context, bucketID int) error {
	tag, err := m.conn.Exec(ctx, `
		DELETE FROM bucket_leases
		WHERE bucket_id = $1 AND domain = $2 AND holder = $3`,
		bucketID, m.domain, m.holderID)
	if err != nil {
		return fmt.Errorf("lease release bucket %d: %w", bucketID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotHolder
	}
	return nil
}

// Grant is the coordinator's path: force-reassign a bucket to a new holder,
// bumping the epoch. The UPDATE takes an exclusive lock on the row, which
// conflicts with any FOR SHARE lock held by an in-flight writer (I4 defense).
func (m *Manager) Grant(ctx context.Context, bucketID int, newHolder string, ttl time.Duration) (int64, error) {
	var epoch int64
	err := m.conn.QueryRow(ctx, `
		UPDATE bucket_leases
		SET holder = $1, epoch = epoch + 1, expires_at = now() + $2::interval
		WHERE bucket_id = $3 AND domain = $4
		RETURNING epoch`,
		newHolder, fmt.Sprintf("%d seconds", int(ttl.Seconds())), bucketID, m.domain).Scan(&epoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("grant bucket %d: no existing lease", bucketID)
		}
		return 0, fmt.Errorf("grant bucket %d: %w", bucketID, err)
	}
	return epoch, nil
}

func (m *Manager) Get(ctx context.Context, bucketID int) (*Info, error) {
	info := &Info{BucketID: bucketID, Domain: m.domain}
	err := m.conn.QueryRow(ctx, `
		SELECT holder, epoch, expires_at FROM bucket_leases
		WHERE bucket_id = $1 AND domain = $2`,
		bucketID, m.domain).Scan(&info.Holder, &info.Epoch, &info.Expires)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lease get bucket %d: %w", bucketID, err)
	}
	return info, nil
}

// BothManager acquires/renews/releases both spec and status leases atomically.
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
	ttlStr := fmt.Sprintf("%d seconds", int(ttl.Seconds()))
	rows, err := b.conn.Query(ctx, `
		INSERT INTO bucket_leases (bucket_id, domain, holder, epoch, expires_at)
		VALUES ($1, 'spec', $2, 1, now() + $3::interval),
		       ($1, 'status', $2, 1, now() + $3::interval)
		ON CONFLICT (bucket_id, domain) DO UPDATE
		SET holder = EXCLUDED.holder,
		    epoch = bucket_leases.epoch + 1,
		    expires_at = EXCLUDED.expires_at
		WHERE bucket_leases.expires_at < now()
		   OR bucket_leases.holder = EXCLUDED.holder
		RETURNING domain, epoch`,
		bucketID, b.holderID, ttlStr)
	if err != nil {
		return BothEpochs{}, fmt.Errorf("acquire both bucket %d: %w", bucketID, err)
	}
	defer rows.Close()

	result := BothEpochs{}
	count := 0
	for rows.Next() {
		var domain string
		var epoch int64
		if err := rows.Scan(&domain, &epoch); err != nil {
			return BothEpochs{}, fmt.Errorf("acquire both scan: %w", err)
		}
		switch domain {
		case DomainSpec:
			result.Spec = epoch
		case DomainStatus:
			result.Status = epoch
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return BothEpochs{}, fmt.Errorf("acquire both rows: %w", err)
	}
	if count != 2 {
		return BothEpochs{}, fmt.Errorf("acquire both bucket %d: expected 2 rows, got %d (held by another replica)", bucketID, count)
	}
	return result, nil
}

func (b *BothManager) RenewBoth(ctx context.Context, bucketID int, ttl time.Duration) error {
	ttlStr := fmt.Sprintf("%d seconds", int(ttl.Seconds()))
	tag, err := b.conn.Exec(ctx, `
		UPDATE bucket_leases
		SET expires_at = now() + $1::interval
		WHERE bucket_id = $2 AND domain IN ('spec', 'status') AND holder = $3`,
		ttlStr, bucketID, b.holderID)
	if err != nil {
		return fmt.Errorf("renew both bucket %d: %w", bucketID, err)
	}
	if tag.RowsAffected() != 2 {
		return fmt.Errorf("renew both bucket %d: %w (expected 2 rows, got %d)", bucketID, ErrNotHolder, tag.RowsAffected())
	}
	return nil
}

func (b *BothManager) ReleaseBoth(ctx context.Context, bucketID int) error {
	tag, err := b.conn.Exec(ctx, `
		DELETE FROM bucket_leases
		WHERE bucket_id = $1 AND domain IN ('spec', 'status') AND holder = $2`,
		bucketID, b.holderID)
	if err != nil {
		return fmt.Errorf("release both bucket %d: %w", bucketID, err)
	}
	if tag.RowsAffected() != 2 {
		return fmt.Errorf("release both bucket %d: %w (expected 2 rows, got %d)", bucketID, ErrNotHolder, tag.RowsAffected())
	}
	return nil
}
