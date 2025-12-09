package ydb

import (
	"fmt"
	"testing"

	log "github.com/hashicorp/go-hclog"
	helper "github.com/hashicorp/vault/helper/testhelpers/ydb"
	"github.com/hashicorp/vault/sdk/helper/logging"
	"github.com/hashicorp/vault/sdk/physical"
)

func TestYDBBackend(t *testing.T) {
	logger := logging.NewVaultLogger(log.Debug)

	cleanup, cfg := helper.PrepareTestContainer(t)
	defer cleanup()

	logger.Info(fmt.Sprintf("YDB DSN: %v", cfg.DSN))
	logger.Info(fmt.Sprintf("YDB VAULT TABLE: %v", cfg.Table))

	backend, err := NewYDBBackend(map[string]string{
		"dsn":                      cfg.DSN,
		"table":                    cfg.Table,
		"internal_ca":              "no",
		"service_account_key_file": cfg.SAKeyFile,
	}, logger)
	if err != nil {
		t.Fatalf("Failed to create new backend: %v", err)
	}

	logger.Info("Running basic backend tests")
	physical.ExerciseBackend(t, backend)

	logger.Info("Running list prefix backend tests")
	physical.ExerciseBackend_ListPrefix(t, backend)
	physical.ExerciseTransactionalBackend(t, backend)
}
