package ydb

import (
	"fmt"
	"os"
	"testing"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/helper/logging"
	"github.com/hashicorp/vault/sdk/physical"
)

func TestYDBBackend(t *testing.T) {
	logger := logging.NewVaultLogger(log.Debug)

	dsn := os.Getenv("VAULT_YDB_DSN")
	table := os.Getenv("VAULT_YDB_TABLE")
	sa_account_key_file := os.Getenv("VAULT_YDB_YC_SA_ACCOUNT_KEY_FILE_PATH")

	logger.Info(fmt.Sprintf("YDB DSN: %v", dsn))
	logger.Info(fmt.Sprintf("YDB VAULT TABLE: %v", table))

	backend, err := NewYDBBackend(map[string]string{
		"dsn":                      dsn,
		"table":                    table,
		"internal_ca":              "yes",
		"service_account_key_file": sa_account_key_file,
	}, logger)
	if err != nil {
		t.Fatalf("Failed to create new backend: %v", err)
	}

	logger.Info("Running basic backend tests")
	physical.ExerciseBackend(t, backend)

	logger.Info("Running list prefix backend tests")
	physical.ExerciseBackend_ListPrefix(t, backend)
}
