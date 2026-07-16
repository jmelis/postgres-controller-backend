package doorbell

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/model"
)

// Debouncer coalesces pg_notify calls per GVK. When Ring is called, it
// marks the GVK as dirty. A single background goroutine wakes every
// window interval, sends one pg_notify per dirty GVK, then clears the
// set. Under continuous writes this produces at most one notification
// per window per GVK, using a single dedicated connection.
type Debouncer struct {
	conn   *pgx.Conn
	window time.Duration

	mu    sync.Mutex
	dirty map[string]bool

	stop chan struct{}
	done chan struct{}
}

func NewDebouncer(conn *pgx.Conn, window time.Duration) *Debouncer {
	d := &Debouncer{
		conn:   conn,
		window: window,
		dirty:  make(map[string]bool),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go d.loop()
	return d
}

func (d *Debouncer) Ring(gvk string) {
	d.mu.Lock()
	d.dirty[gvk] = true
	d.mu.Unlock()
}

func (d *Debouncer) loop() {
	defer close(d.done)
	ticker := time.NewTicker(d.window)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.flush()
		case <-d.stop:
			d.flush()
			return
		}
	}
}

func (d *Debouncer) flush() {
	d.mu.Lock()
	if len(d.dirty) == 0 {
		d.mu.Unlock()
		return
	}
	gvks := make([]string, 0, len(d.dirty))
	for gvk := range d.dirty {
		gvks = append(gvks, gvk)
	}
	d.dirty = make(map[string]bool)
	d.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, gvk := range gvks {
		channel := model.DoorbellChannel(gvk)
		if _, err := d.conn.Exec(ctx, `SELECT pg_notify($1, '')`, channel); err != nil {
			log.Printf("doorbell send failed (non-fatal): %v", err)
		}
	}
}

// Close stops the background loop and flushes any pending notifications.
func (d *Debouncer) Close() {
	close(d.stop)
	<-d.done
}
