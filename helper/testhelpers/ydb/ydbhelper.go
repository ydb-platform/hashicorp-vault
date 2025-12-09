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

	if dsn := os.Getenv("VAULT_YDB_DSN"); dsn != "" {
		cfg := &Config{
			DSN:       dsn,
			Table:     os.Getenv("VAULT_YDB_TABLE"),
			SAKeyFile: os.Getenv("VAULT_YDB_YC_SA_ACCOUNT_KEY_FILE_PATH"),
		}
		if cfg.Table == "" {
			cfg.Table = "vault_kv"
		}
		return func() {}, cfg
	}

	repo := os.Getenv("YDB_DOCKER_REPO")
	if repo == "" {
		t.Skip("no VAULT_YDB_DSN and no YDB_DOCKER_REPO provided; skipping ydb integration test")
	}

	tag := os.Getenv("YDB_DOCKER_TAG")
	if tag == "" {
		tag = "latest"
	}

	tableName := os.Getenv("VAULT_YDB_TABLE")
	if tableName == "" {
		tableName = "vault_kv"
	}

	runner, err := docker.NewServiceRunner(docker.RunOptions{
		ImageRepo:     repo,
		ImageTag:      tag,
		Env:           []string{"GRPC_PORT=2136", "GRPC_TLS_PORT=2135"},
		ContainerName: "ydb",
		Ports:         []string{"2135/tcp", "2136/tcp"},
	})
	if err != nil {
		t.Fatalf("Could not create YDB docker runner: %v", err)
	}

	svc, err := runner.StartService(context.Background(), func(ctx context.Context, host string, port int) (docker.ServiceConfig, error) {
		dsn := fmt.Sprintf("grpc://%s:%d", host, port)
		ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		db, err := ydb.Open(ctx2, dsn)
		if err != nil {
			return nil, err
		}
		_ = db.Close(context.Background())

		cfg := &Config{
			DSN:   dsn,
			Table: tableName,
		}
		// wrap host:port so caller can inspect URL/Address if needed
		cfg.shp = docker.NewServiceHostPort(host, port)
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

	{
		ctxc, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		db, err := ydb.Open(ctxc, cfg.DSN)
		if err != nil {
			svc.Cleanup()
			t.Fatalf("ydb: open after start failed: %v", err)
		}
		defer db.Close(context.Background())

		createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key Utf8, value Bytes, PRIMARY KEY (key))", cfg.Table)
		var lastErr error
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			err = db.Query().Exec(ctxc, createStmt)
			if err == nil {
				lastErr = nil
				break
			}
			lastErr = err
			time.Sleep(500 * time.Millisecond)
		}
		if lastErr != nil {
			svc.Cleanup()
			t.Fatalf("ydb: failed to create table %s: %v", cfg.Table, lastErr)
		}
	}

	return svc.Cleanup, cfg
}
