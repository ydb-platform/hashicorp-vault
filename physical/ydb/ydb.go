package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/physical"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const VAULT_TABLE = "vault_kv"

type YDBBackend struct {
	db     *ydb.Driver
	table  string
	logger log.Logger
}

var _ physical.Backend = (*YDBBackend)(nil)

// var _ physical.Transactional = (*YDBBackend)(nil)

func NewYDBBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	dsn, ok := conf["dsn"]
	if !ok {
		if envDSN := os.Getenv("VAULT_YDB_DSN"); envDSN != "" {
			dsn = envDSN
		} else {
			const errStr = "YDB: dsn it not set"
			logger.Error(errStr)
			return &YDBBackend{}, fmt.Errorf(errStr)
		}
	}
	table, ok := conf["table"]
	if !ok {
		if envTable := os.Getenv("VAULT_YDB_TABLE"); envTable != "" {
			table = envTable
		} else {
			table = VAULT_TABLE
		}
	}

	db, err := ydb.Open(context.TODO(), dsn)
	if err != nil {
		errStr := "YDB: failed to open database connection"
		logger.Error(errStr)
		return &YDBBackend{}, fmt.Errorf(errStr+": %w", err)
	}

	// TODO: Check for vault storage table exists

	return &YDBBackend{
		db:     db,
		table:  table,
		logger: logger,
	}, nil
}

func (y *YDBBackend) Put(ctx context.Context, entry *physical.Entry) error {
	err := y.db.Query().Exec(ctx,
		`UPSERT INTO vault_storage_kv (key, value) VALUES ($key, $value)`,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(entry.Key).
				Param("$value").Bytes(entry.Value).Build()),
	)
	if err != nil {
		errStr := "YDB: failed to put entry: " + entry.String()
		y.logger.Error(errStr+" %w", err)
		return fmt.Errorf(errStr+" %w", err)
	}

	return nil
}

func (y *YDBBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	q, err := y.db.Query().QueryRow(ctx,
		`SELECT key, value FROM vault_storage_kv WHERE key = $key`,
		query.WithParameters(
			ydb.ParamsBuilder().
				// Param("$vault_table").Text(VAULT_TABLE).
				Param("$key").Text(key).Build()),
	)
	if err != nil && !errors.Is(err, io.EOF) {
		errStr := "YDB: failed to get key " + key
		y.logger.Error(fmt.Sprintf(errStr+" %e", err))
		return nil, fmt.Errorf(errStr+" %w", err)
	}

	entry := physical.Entry{}
	tmp := struct {
		Key   string `sql:"key"`
		Value []byte `sql:"value"`
	}{}

	if errors.Is(err, io.EOF) {
		return nil, nil
	}

	if err = q.ScanStruct(&tmp); err != nil {
		errStr := "YDB: failed to get key " + key
		y.logger.Error(errStr)
		return nil, fmt.Errorf(errStr+" %w", err)
	}

	entry.Key = tmp.Key
	entry.Value = tmp.Value

	return &entry, nil
}

func (y *YDBBackend) Delete(ctx context.Context, key string) error {
	err := y.db.Query().Exec(ctx,
		`DELETE FROM vault_storage_kv WHERE key = $key`,
		query.WithParameters(
			ydb.ParamsBuilder().
				// Param("$vault_table").Text(VAULT_TABLE).
				Param("$key").Text(key).Build()),
	)
	if err != nil {
		errStr := "YDB: failed to drop entry with key " + key
		y.logger.Error(errStr+" %w", err)
		return fmt.Errorf(errStr+" %w", err)
	}

	return nil
}

func (y *YDBBackend) List(ctx context.Context, prefix string) ([]string, error) {
	errStr := "YDB: failed to list keys by prefix " + prefix
	prefixKey := prefix + "%"
	if prefix == "" {
		prefixKey = ""
	}
	q, err := y.db.Query().Query(ctx,
		`SELECT key FROM vault_storage_kv WHERE key LIKE $prefix`,
		query.WithParameters(
			ydb.ParamsBuilder().
				// Param("$vault_table").Text(y.table).
				Param("$prefix").Text(prefixKey).Build()),
	)
	if err != nil {
		y.logger.Error(fmt.Sprintf(errStr+" %w", err))
		return nil, fmt.Errorf(errStr+" %w", err)
	}
	defer q.Close(ctx)

	// Precalculate number of rows?
	lst := make([]string, 0)

	for rs, err := range q.ResultSets(ctx) {
		if err != nil {
			y.logger.Error(errStr)
			return nil, fmt.Errorf(errStr+" %w", err)
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				y.logger.Error(errStr)
				return nil, fmt.Errorf(errStr+" %w", err)
			}

			lst = append(lst, row.Values()[0].Yql())
		}
	}

	return lst, nil
}

func (y *YDBBackend) Transaction(ctx context.Context, tx []*physical.TxnEntry) error {
	return nil
}
