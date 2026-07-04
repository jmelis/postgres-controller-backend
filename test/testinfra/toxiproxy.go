package testinfra

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/client"
	"github.com/jackc/pgx/v5"
	"github.com/jmelisba/postgres-controller-backend/internal/schema"
)

type ProxiedDB struct {
	DirectConnStr  string
	ProxiedConnStr string
	Proxy          *toxiproxy.Proxy
	ToxiClient     *toxiproxy.Client
	network        string
	pgContainer    string
	toxiContainer  string
}

// StartPostgresWithProxy starts a Postgres container and a Toxiproxy container
// on a shared podman network. Returns direct and proxied connection strings.
func StartPostgresWithProxy() *ProxiedDB {
	apiPort := freePortNoT()
	proxyPort := freePortNoT()
	networkName := fmt.Sprintf("pgctl-net-%d", apiPort)
	pgContainer := fmt.Sprintf("pgctl-pg-%d", apiPort)
	toxiContainer := fmt.Sprintf("pgctl-toxi-%d", apiPort)

	// Create podman network
	run("podman", "network", "create", networkName)

	// Start Postgres on the network
	run("podman", "run", "-d", "--rm",
		"--name", pgContainer,
		"--network", networkName,
		"-e", "POSTGRES_DB=pgctl_test",
		"-e", "POSTGRES_USER=test",
		"-e", "POSTGRES_PASSWORD=test",
		"docker.io/library/postgres:16-alpine")

	// Start Toxiproxy on the same network, expose API and proxy ports
	run("podman", "run", "-d", "--rm",
		"--name", toxiContainer,
		"--network", networkName,
		"-p", fmt.Sprintf("%d:8474", apiPort),
		"-p", fmt.Sprintf("%d:15432", proxyPort),
		"ghcr.io/shopify/toxiproxy:latest")

	// Wait for postgres to be ready (connect via the network using pg container name)
	// First, get the direct port mapping for postgres
	pgDirectPort := freePortNoT()
	// Actually, we need postgres accessible from host too for direct connections.
	// Let's restart postgres with a host port mapping.
	run("podman", "stop", pgContainer)

	run("podman", "run", "-d", "--rm",
		"--name", pgContainer,
		"--network", networkName,
		"-p", fmt.Sprintf("%d:5432", pgDirectPort),
		"-e", "POSTGRES_DB=pgctl_test",
		"-e", "POSTGRES_USER=test",
		"-e", "POSTGRES_PASSWORD=test",
		"docker.io/library/postgres:16-alpine")

	directConnStr := fmt.Sprintf("postgres://test:test@localhost:%d/pgctl_test?sslmode=disable", pgDirectPort)
	waitForPostgresNoT(directConnStr)

	// Wait for toxiproxy API
	waitForTCP(fmt.Sprintf("localhost:%d", apiPort), 30*time.Second)

	// Create the proxy: toxiproxy listens on 0.0.0.0:15432 -> pgContainer:5432
	toxiClient := toxiproxy.NewClient(fmt.Sprintf("http://localhost:%d", apiPort))
	proxy, err := toxiClient.CreateProxy("postgres",
		"0.0.0.0:15432",
		fmt.Sprintf("%s:5432", pgContainer))
	if err != nil {
		panic(fmt.Sprintf("create toxiproxy proxy: %v", err))
	}

	proxiedConnStr := fmt.Sprintf("postgres://test:test@localhost:%d/pgctl_test?sslmode=disable", proxyPort)

	// Wait for proxied connection to work
	waitForPostgresNoT(proxiedConnStr)

	// Run migrations via direct connection
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, directConnStr)
	if err != nil {
		panic(fmt.Sprintf("connect for migration: %v", err))
	}
	if err := schema.Migrate(ctx, conn); err != nil {
		conn.Close(ctx)
		panic(fmt.Sprintf("migrate: %v", err))
	}
	conn.Close(ctx)

	return &ProxiedDB{
		DirectConnStr:  directConnStr,
		ProxiedConnStr: proxiedConnStr,
		Proxy:          proxy,
		ToxiClient:     toxiClient,
		network:        networkName,
		pgContainer:    pgContainer,
		toxiContainer:  toxiContainer,
	}
}

func (p *ProxiedDB) Stop() {
	exec.Command("podman", "stop", p.toxiContainer).Run()
	exec.Command("podman", "stop", p.pgContainer).Run()
	exec.Command("podman", "network", "rm", p.network).Run()
}

func (p *ProxiedDB) DirectConn(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, p.DirectConnStr)
}

func (p *ProxiedDB) ProxiedConn(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, p.ProxiedConnStr)
}

func run(name string, args ...string) {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("%s %v: %v\n%s", name, args, err, out))
	}
}

func waitForTCP(addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	panic(fmt.Sprintf("tcp %s not ready after %v", addr, timeout))
}
