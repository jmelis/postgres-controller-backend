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

	// Acquire leases.
	logger.Info("acquiring leases")
	leaseConn, err := connFactory()
	if err != nil {
		logger.Error("connect for lease", "err", err)
		os.Exit(1)
	}

	leaseMgr := lease.NewBothManager(leaseConn, holderID)
	epochs, err := leaseMgr.AcquireBoth(ctx, bucketID, leaseTTL)
	if err != nil {
		logger.Error("acquire lease", "err", err)
		os.Exit(1)
	}
	logger.Info("leases acquired", "specEpoch", epochs.Spec, "statusEpoch", epochs.Status)

	// Common config.
	assigner := func(_, _ string) int { return bucketID }
	buckets := []int{bucketID}
	leaseEpochs := crbridge.LeaseEpochs{Spec: epochs.Spec, Status: epochs.Status}

	// Build typed clients — used by both the controller and the HTTP API.
	greetingClient := crbridge.NewTypedClient[GreetingSpec, GreetingStatus](
		crbridge.NewClient(connFactory, gvkGreeting, assigner, holderID, epochs.Spec),
		crbridge.NewListerWatcher(connFactory, gvkGreeting, buckets),
	)
	cardClient := crbridge.NewTypedClient[GreetingCardSpec, GreetingCardStatus](
		crbridge.NewClient(connFactory, gvkGreetingCard, assigner, holderID, epochs.Spec),
		crbridge.NewListerWatcher(connFactory, gvkGreetingCard, buckets),
	)
	policyClient := crbridge.NewTypedClient[GreetingPolicySpec, GreetingPolicyStatus](
		crbridge.NewClient(connFactory, gvkGreetingPolicy, assigner, holderID, epochs.Spec),
		crbridge.NewListerWatcher(connFactory, gvkGreetingPolicy, buckets),
	)

	// Register the controller with the Manager.
	reconciler := &GreetingReconciler{
		Greetings: greetingClient,
		Cards:     cardClient,
		Policies:  policyClient,
	}

	mgr := crbridge.NewManager(crbridge.ManagerConfig{
		ConnFactory:    connFactory,
		HolderID:       holderID,
		BucketAssigner: assigner,
		BucketIDs:      buckets,
		LeaseEpochs: map[string]crbridge.LeaseEpochs{
			gvkGreeting:       leaseEpochs,
			gvkGreetingCard:   leaseEpochs,
			gvkGreetingPolicy: leaseEpochs,
		},
		Logger: logger,
	})

	crbridge.NewControllerFor[GreetingSpec, GreetingStatus](mgr, gvkGreeting, reconciler).
		Watches(gvkGreetingPolicy, reconciler.policyToGreetings).
		Complete()

	go func() {
		if err := mgr.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("manager exited", "err", err)
		}
	}()

	// Build CRD validator and start HTTP API.
	// The HTTP API uses untyped clients because it serves raw JSON.
	logger.Info("loading CRD schemas")
	validator, err := NewValidator()
	if err != nil {
		logger.Error("load CRD schemas", "err", err)
		os.Exit(1)
	}

	untypedClients := map[string]*crbridge.Client{
		gvkGreeting:       greetingClient.Untyped(),
		gvkGreetingCard:   cardClient.Untyped(),
		gvkGreetingPolicy: policyClient.Untyped(),
	}
	untypedLWs := map[string]*crbridge.ListerWatcher{
		gvkGreeting:       greetingClient.ListerWatcher(),
		gvkGreetingCard:   cardClient.ListerWatcher(),
		gvkGreetingPolicy: policyClient.ListerWatcher(),
	}

	apiServer := NewAPIServer(untypedClients, untypedLWs, validator, logger)
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
				if err := leaseMgr.RenewBoth(ctx, bucketID, leaseTTL); err != nil {
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
