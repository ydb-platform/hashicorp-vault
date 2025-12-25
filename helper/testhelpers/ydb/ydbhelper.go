package ydb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/vault/sdk/helper/docker"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

type Config struct {
	DSN       string
	Table     string
	SAKeyFile string
	shp       *docker.ServiceHostPort
}

func (c *Config) Address() string {
	if c.shp == nil {
		u, err := url.Parse(c.DSN)
		if err != nil || u.Host == "" {
			return c.DSN
		}
		return u.Host
	}
	return c.shp.Address()
}

func (c *Config) URL() *url.URL {
	if c.shp == nil {
		u, _ := url.Parse(c.DSN)
		return u
	}
	return c.shp.URL()
}

func PrepareTestContainer(t *testing.T) (func(), *Config) {
	t.Helper()

	// Determine target table name. Environment variable takes precedence; if
	// not provided, generate a unique table name for the test run.
	tableName := os.Getenv("VAULT_YDB_TABLE")
	if tableName == "" {
		tableName = fmt.Sprintf("vault_kv_test_%d", time.Now().UnixNano())
	}

	if dsn := os.Getenv("VAULT_YDB_DSN"); dsn != "" {
		cfg := &Config{
			DSN:       dsn,
			Table:     tableName,
			SAKeyFile: os.Getenv("VAULT_YDB_SA_KEYFILE"),
		}

		// Ensure the table exists on the remote DSN and return a cleanup that
		// drops it after the test finishes.
		ctxc, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		db, err := ydb.Open(ctxc, cfg.DSN)
		if err != nil {
			t.Fatalf("ydb: open failed for DSN %s: %v", cfg.DSN, err)
		}
		defer db.Close(context.Background())

		createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key Utf8, value Bytes, PRIMARY KEY (key))", cfg.Table)
		var lastErr error
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			lastErr = db.Query().Exec(ctxc, createStmt)
			if lastErr == nil {
				lastErr = nil
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if lastErr != nil {
			t.Fatalf("ydb: failed to create table %s: %v", cfg.Table, lastErr)
		}

		// cleanup will attempt to drop the table; ignore errors but log them.
		cleanup := func() {
			ctxd, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			db2, err := ydb.Open(ctxd, cfg.DSN)
			if err == nil {
				defer db2.Close(context.Background())
				dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", cfg.Table)
				if derr := db2.Query().Exec(ctxd, dropStmt); derr != nil {
					t.Logf("ydb: failed to drop table %s during cleanup: %v", cfg.Table, derr)
				}
			} else {
				t.Logf("ydb: failed to open DB for cleanup: %v", err)
			}
		}

		return cleanup, cfg
	}

	repo := os.Getenv("YDB_DOCKER_REPO")
	if repo == "" {
		t.Skip("no VAULT_YDB_DSN and no YDB_DOCKER_REPO provided; skipping ydb integration test")
	}

	tag := os.Getenv("YDB_DOCKER_TAG")
	if tag == "" {
		tag = "latest"
	}

	// For docker-run path, reuse existing tableName variable if set. If not,
	// respect VAULT_YDB_TABLE or generate a test-specific table name.
	if envTable := os.Getenv("VAULT_YDB_TABLE"); envTable != "" {
		tableName = envTable
	} else if tableName == "" {
		// If not set via env or earlier DSN path, generate unique name to avoid collisions in CI.
		tableName = fmt.Sprintf("vault_kv_test_%d", time.Now().UnixNano())
	}

	t.Logf("ydb helper: creating docker runner for image %s:%s", repo, tag)
	runner, err := docker.NewServiceRunner(docker.RunOptions{
		ImageRepo:     repo,
		ImageTag:      tag,
		Env:           []string{"GRPC_PORT=2136", "GRPC_TLS_PORT=2135"},
		ContainerName: "ydb",
		Ports: []string{"2136/tcp", "2135/tcp"},
	})
	if err != nil {
		t.Fatalf("Could not create YDB docker runner: %v", err)
	}

	t.Logf("ydb helper: docker runner created for image %s:%s", repo, tag)

	startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer startCancel()
	t.Logf("ydb helper: starting container (startup timeout 2m)")

	svc, err := runner.StartService(startCtx, func(ctx context.Context, host string, port int) (docker.ServiceConfig, error) {
		t.Logf("ydb helper: StartService callback: host=%s port=%d", host, port)
		dsn := fmt.Sprintf("grpc://127.0.0.1:%d/local", port)
		t.Logf("ydb helper: testing DSN %s", dsn)
		ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		db, err := ydb.Open(ctx2, dsn)
		if err != nil {
			t.Logf("ydb helper: connectivity check failed for %s: %v", dsn, err)
			return nil, err
		}
		_ = db.Close(context.Background())

		cfg := &Config{
			DSN:   dsn,
			Table: tableName,
		}
		// wrap host:port so caller can inspect URL/Address if needed
		cfg.shp = docker.NewServiceHostPort("127.0.0.1", port)
		return cfg, nil
	})
	if err != nil {
		t.Fatalf("Could not start local YDB: %v", err)
	}

	// After container start, attempt to create the table once it's reachable.
	// svc.Config implements docker.ServiceConfig so assert to our type
	cfg, ok := svc.Config.(*Config)
	if !ok {
		svc.Cleanup()
		t.Fatalf("ydb: unexpected service config type")
	}
	t.Logf("ydb helper: container service started; DSN=%s table=%s", cfg.DSN, cfg.Table)

	// Ensure the table exists on the newly-started YDB instance.
	{
		ctxc, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		db, err := ydb.Open(ctxc, cfg.DSN)
		if err != nil {
			svc.Cleanup()
			t.Fatalf("ydb: open after start failed: %v", err)
		}
		defer db.Close(context.Background())

		cfg.Table = tableName

		createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key Utf8, value Bytes, PRIMARY KEY (key))", cfg.Table)
		t.Logf("ydb helper: ensuring table exists: %s", cfg.Table)
		var lastErr error
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			lastErr = db.Query().Exec(ctxc, createStmt)
			if lastErr == nil {
				t.Logf("ydb helper: table %s is ready", cfg.Table)
				break
			}
			t.Logf("ydb helper: waiting for table creation, last error: %v", lastErr)
			time.Sleep(500 * time.Millisecond)
		}
		if lastErr != nil {
			svc.Cleanup()
			t.Fatalf("ydb: failed to create table %s: %v", cfg.Table, lastErr)
		}
	}

	// Return cleanup that first tries to drop the table, then stops the service.
	cleanup := func() {
		t.Logf("ydb helper: cleanup started for table %s, DSN=%s", cfg.Table, cfg.DSN)
		ctxd, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		db2, err := ydb.Open(ctxd, cfg.DSN)
		if err == nil {
			defer db2.Close(context.Background())
			dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", cfg.Table)
			if derr := db2.Query().Exec(ctxd, dropStmt); derr != nil {
				t.Logf("ydb: failed to drop table %s during cleanup: %v", cfg.Table, derr)
			} else {
				t.Logf("ydb helper: dropped table %s", cfg.Table)
			}
		} else {
			t.Logf("ydb: failed to open DB for cleanup: %v", err)
		}
		t.Logf("ydb helper: stopping container service now")
		svc.Cleanup()
		t.Logf("ydb helper: container stopped and cleanup finished")
	}

	return cleanup, cfg
}
