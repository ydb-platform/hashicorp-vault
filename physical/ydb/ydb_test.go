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
	table := "vault_storage_kv"

	logger.Info(fmt.Sprintf("YDB DSN: %v", dsn))
	logger.Info(fmt.Sprintf("YDB VAULT TABLE: %v", table))

	backend, err := NewYDBBackend(map[string]string{
		"dsn":   dsn,
		"table": table,
	}, logger)
	if err != nil {
		t.Fatalf("Failed to create new backend: %v", err)
	}

	// defer func() {
	// 	y := backend.(*YDBBackend)
	// 	err := y.db.Query().Exec(context.TODO(), fmt.Sprintf("TRUNCATE TABLE %v ", table))
	// 	if err != nil {
	// 		t.Fatalf("Failed to truncate table: %v", err)
	// 	}
	// }()

	logger.Info("Running basic backend tests")
	physical.ExerciseBackend(t, backend)

	logger.Info("Running list prefix backend tests")
	physical.ExerciseBackend_ListPrefix(t, backend)
}
