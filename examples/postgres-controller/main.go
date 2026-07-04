package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/internal/lease"
	"github.com/jmelis/postgres-controller-backend/internal/schema"
	"github.com/jmelis/postgres-controller-backend/pkg/crbridge"
)

const (
	apiVersion = "greeting.example.com/v1alpha1"

	gvkGreeting       = "greeting.example.com/v1alpha1/Greeting"
	gvkGreetingCard   = "greeting.example.com/v1alpha1/GreetingCard"
	gvkGreetingPolicy = "greeting.example.com/v1alpha1/GreetingPolicy"

	bucketID = 0
	holderID = "postgres-greeting-controller"
	leaseTTL = 60 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://greeting:greeting@localhost:5432/greetings?sslmode=disable"
	}
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Wait for postgres to be ready.
	var conn *pgx.Conn
	for i := 0; i < 30; i++ {
		var err error
		conn, err = pgx.Connect(ctx, dsn)
		if err == nil {
			break
		}
		logger.Info("waiting for postgres", "attempt", i+1, "err", err)
		time.Sleep(2 * time.Second)
	}
	if conn == nil {
		logger.Error("failed to connect to postgres after 30 attempts")
		os.Exit(1)
	}

	// Migrate schema.
	logger.Info("migrating schema")
	if err := schema.Migrate(ctx, conn); err != nil {
		logger.Error("schema migration failed", "err", err)
		os.Exit(1)
	}
	conn.Close(ctx)

	connFactory := func() (*pgx.Conn, error) {
		return pgx.Connect(ctx, dsn)
	}

	// Acquire leases for all three GVKs.
	logger.Info("acquiring leases")
	leaseConn, err := connFactory()
	if err != nil {
		logger.Error("connect for lease", "err", err)
		os.Exit(1)
	}

	mgr := lease.NewBothManager(leaseConn, holderID)
	gvks := []string{gvkGreeting, gvkGreetingCard, gvkGreetingPolicy}
	epochs := make(map[string]lease.BothEpochs)
	for _, gvk := range gvks {
		// Each GVK uses the same bucket but leases are per-bucket, so a single
		// AcquireBoth per bucket suffices. We call it once for the bucket.
		if _, ok := epochs[gvk]; !ok {
			ep, err := mgr.AcquireBoth(ctx, bucketID, leaseTTL)
			if err != nil {
				logger.Error("acquire lease", "gvk", gvk, "err", err)
				os.Exit(1)
			}
			for _, g := range gvks {
				epochs[g] = ep
			}
		}
	}
	logger.Info("leases acquired", "specEpoch", epochs[gvkGreeting].Spec, "statusEpoch", epochs[gvkGreeting].Status)

	// Create clients and lister-watchers for each GVK.
	assigner := func(_, _ string) int { return bucketID }
	buckets := []int{bucketID}

	clients := map[string]*crbridge.Client{
		gvkGreeting:       crbridge.NewClient(connFactory, gvkGreeting, assigner, holderID, epochs[gvkGreeting].Spec),
		gvkGreetingCard:   crbridge.NewClient(connFactory, gvkGreetingCard, assigner, holderID, epochs[gvkGreetingCard].Spec),
		gvkGreetingPolicy: crbridge.NewClient(connFactory, gvkGreetingPolicy, assigner, holderID, epochs[gvkGreetingPolicy].Spec),
	}

	lws := map[string]*crbridge.ListerWatcher{
		gvkGreeting:       crbridge.NewListerWatcher(connFactory, gvkGreeting, buckets),
		gvkGreetingCard:   crbridge.NewListerWatcher(connFactory, gvkGreetingCard, buckets),
		gvkGreetingPolicy: crbridge.NewListerWatcher(connFactory, gvkGreetingPolicy, buckets),
	}

	// Build CRD validator.
	logger.Info("loading CRD schemas")
	validator, err := NewValidator()
	if err != nil {
		logger.Error("load CRD schemas", "err", err)
		os.Exit(1)
	}

	// Start controller.
	ctrl := NewController(
		clients[gvkGreeting], clients[gvkGreetingCard], clients[gvkGreetingPolicy],
		lws[gvkGreeting], lws[gvkGreetingPolicy], lws[gvkGreetingCard],
		validator, logger,
	)
	go func() {
		if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("controller exited", "err", err)
		}
	}()

	// Start HTTP API.
	apiServer := NewAPIServer(clients, lws, validator, logger)
	httpServer := &http.Server{Addr: listenAddr, Handler: apiServer.Handler()}
	go func() {
		logger.Info("HTTP API listening", "addr", listenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
		}
	}()

	// Lease renewal ticker.
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := mgr.RenewBoth(ctx, bucketID, leaseTTL); err != nil {
					logger.Error("lease renewal failed", "err", err)
				}
			}
		}
	}()

	// Wait for shutdown.
	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)
	// Don't call ReleaseBoth here — during rolling updates the new pod may
	// have already re-acquired with the same holderID, and our delete would
	// wipe its lease rows.  Let the TTL expire naturally instead.
	leaseConn.Close(shutdownCtx)

	logger.Info("shutdown complete")
}

