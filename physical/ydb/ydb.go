package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/physical"
	env "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const VAULT_TABLE = "vault_kv"

type YDBBackend struct {
	db     *ydb.Driver
	table  string
	logger log.Logger
}

var (
	_ physical.Backend             = (*YDBBackend)(nil)
	_ physical.Transactional       = (*YDBBackend)(nil)
	_ physical.TransactionalLimits = (*YDBBackend)(nil)
)

func NewYDBBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	// Environment variables take precedence over supplied configuration values.
	// DSN: prefer VAULT_YDB_DSN if set, otherwise use conf["dsn"].
	var dsn string
	if envDSN := os.Getenv("VAULT_YDB_DSN"); envDSN != "" {
		dsn = strings.TrimSpace(envDSN)
	} else {
		dsn = strings.TrimSpace(conf["dsn"])
		if dsn == "" {
			const errStr = "YDB: dsn is not set"
			return &YDBBackend{}, fmt.Errorf(errStr)
		}
	}

	// Table: prefer VAULT_YDB_TABLE if set, otherwise use conf["table"], falling back to default.
	var table string
	if envTable := os.Getenv("VAULT_YDB_TABLE"); envTable != "" {
		table = strings.TrimSpace(envTable)
	} else {
		table = strings.TrimSpace(conf["table"])
		if table == "" {
			table = VAULT_TABLE
		}
	}

	opts := getYDBOptionsFromConfMap(conf)

	// Override from ENV
	opts = append(opts, env.WithEnvironCredentials())

	ctx := context.TODO()
	db, err := ydb.Open(ctx, dsn, opts...)
	if err != nil {
		errStr := "YDB: failed to open database connection"
		logger.Error(errStr, "error", err)
		return &YDBBackend{}, fmt.Errorf(errStr+": %w", err)
	}

	// Ensure the table exists or create schema as required.
	if err = ensureTableExists(ctx, db, table, logger); err != nil {
		errStr := "YDB: failed to ensure table exists"
		logger.Error(errStr, "table", table, "error", err)
		return &YDBBackend{}, fmt.Errorf(errStr+": %w", err)
	}

	return &YDBBackend{
		db:     db,
		table:  table,
		logger: logger,
	}, nil
}

func ensureTableExists(ctx context.Context, db *ydb.Driver, tableName string, logger log.Logger) error {
	var fullTableName string
	if strings.HasPrefix(tableName, "/") {
		fullTableName = tableName
	} else {
		fullTableName = db.Name() + "/" + tableName
	}

	_, err := db.Scheme().DescribePath(ctx, fullTableName)
	tableExists := err == nil

	if tableExists {
		logger.Info("YDB: table already exists", "table", tableName)
		return nil
	}

	logger.Info("YDB: creating table", "table", tableName)

	queryStmt := fmt.Sprintf(`
		CREATE TABLE %s (
			key Text NOT NULL,
			value Bytes,
			updated_at Timestamp,
			PRIMARY KEY (key)
		)`, tableName)

	err = db.Query().Exec(ctx, queryStmt)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	logger.Info("YDB: table created successfully", "table", tableName)
	return nil
}

func (y *YDBBackend) Put(ctx context.Context, entry *physical.Entry) error {
	stmt := fmt.Sprintf("UPSERT INTO %s (key, value) VALUES ($key, $value)", y.table)
	err := y.db.Query().Exec(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(entry.Key).
				Param("$value").Bytes(entry.Value).Build()),
	)
	if err != nil {
		errStr := "YDB: failed to put entry: " + entry.String()
		y.logger.Error(errStr, "error", err)
		return fmt.Errorf(errStr+" %w", err)
	}
	return nil
}

func (y *YDBBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	stmt := fmt.Sprintf("SELECT key AS Key, value AS Value FROM %s WHERE key = $key", y.table)
	q, err := y.db.Query().QueryRow(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(key).Build()),
	)
	if err != nil && !errors.Is(err, io.EOF) {
		errStr := "YDB: failed to get key " + key
		y.logger.Error(errStr, "error", err)
		return nil, fmt.Errorf(errStr+" %w", err)
	}

	if errors.Is(err, io.EOF) {
		return nil, nil
	}

	entry := physical.Entry{}
	if err = q.ScanStruct(&entry, query.WithScanStructAllowMissingColumnsFromSelect()); err != nil {
		errStr := "YDB: failed to get key " + key
		y.logger.Error(errStr, "error", err)
		return nil, fmt.Errorf(errStr+" %w", err)
	}

	return &entry, nil
}

func (y *YDBBackend) Delete(ctx context.Context, key string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE key = $key", y.table)
	err := y.db.Query().Exec(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$key").Text(key).Build()),
	)
	if err != nil {
		errStr := "YDB: failed to drop entry with key " + key
		y.logger.Error(errStr, "error", err)
		return fmt.Errorf(errStr+" %w", err)
	}
	return nil
}

func sanitizeYqlValue(val string) string {
	val = strings.TrimSpace(val)
	if val == "" {
		return val
	}

	if strings.HasSuffix(val, "u") || strings.HasSuffix(val, "U") {
		val = strings.TrimSuffix(val, "u")
		val = strings.TrimSuffix(val, "U")
		val = strings.TrimSpace(val)
	}

	if i := strings.IndexAny(val, `"'`); i != -1 {
		j := strings.LastIndexAny(val, `"'`)
		if j > i {
			return val[i+1 : j]
		}

		val = strings.ReplaceAll(val, `"`, "")
		val = strings.ReplaceAll(val, `'`, "")
	}

	val = strings.Trim(val, `"' `)
	return val
}

func (y *YDBBackend) List(ctx context.Context, prefix string) ([]string, error) {
	errStr := "YDB: failed to list keys by prefix " + prefix
	likePrefix := prefix + "%"
	if prefix == "" {
		likePrefix = "%"
	}

	stmt := fmt.Sprintf("SELECT key FROM %s WHERE key LIKE $prefix ORDER BY key", y.table)
	q, err := y.db.Query().Query(ctx,
		stmt,
		query.WithParameters(
			ydb.ParamsBuilder().
				Param("$prefix").Text(likePrefix).Build()),
	)
	if err != nil {
		y.logger.Error(errStr, "error", err)
		return nil, fmt.Errorf(errStr+" %w", err)
	}
	defer q.Close(ctx)

	seen := make(map[string]struct{})
	for rs, rerr := range q.ResultSets(ctx) {
		if rerr != nil {
			y.logger.Error(errStr, "error", rerr)
			return nil, fmt.Errorf(errStr+" %w", rerr)
		}
		for row, rerr := range rs.Rows(ctx) {
			if rerr != nil {
				y.logger.Error(errStr, "error", rerr)
				return nil, fmt.Errorf(errStr+" %w", rerr)
			}

			val := row.Values()[0].Yql()
			val = sanitizeYqlValue(val)

			rel := val
			if prefix != "" {
				rel = strings.TrimPrefix(val, prefix)
			}

			if rel == "" {
				continue
			}

			if idx := strings.Index(rel, "/"); idx != -1 {
				dir := rel[:idx+1]
				seen[dir] = struct{}{}
			} else {
				seen[rel] = struct{}{}
			}
		}
	}

	lst := make([]string, 0, len(seen))
	for k := range seen {
		lst = append(lst, k)
	}
	return lst, nil
}

func (y *YDBBackend) GetInternal(ctx context.Context, key string) (*physical.Entry, error) {
	return y.Get(ctx, key)
}

func (y *YDBBackend) PutInternal(ctx context.Context, entry *physical.Entry) error {
	return y.Put(ctx, entry)
}

func (y *YDBBackend) DeleteInternal(ctx context.Context, key string) error {
	return y.Delete(ctx, key)
}

type ydbTxWrapper struct {
	tx    query.TxActor
	table string
}

func (w *ydbTxWrapper) GetInternal(ctx context.Context, key string) (*physical.Entry, error) {
	stmt := fmt.Sprintf("SELECT key, value FROM %s WHERE key = $key", w.table)
	params := ydb.ParamsBuilder().Param("$key").Text(key).Build()

	res, err := w.tx.Query(ctx, stmt, query.WithParameters(params))
	if err != nil {
		return nil, err
	}
	defer res.Close(ctx)

	for rs, rerr := range res.ResultSets(ctx) {
		if rerr != nil {
			return nil, rerr
		}
		for row, rerr := range rs.Rows(ctx) {
			if rerr != nil {
				return nil, rerr
			}
			var k string
			var v []byte
			if err := row.Scan(&k, &v); err != nil {
				return nil, err
			}
			return &physical.Entry{Key: k, Value: v}, nil
		}
	}
	return nil, nil
}

func (w *ydbTxWrapper) PutInternal(ctx context.Context, entry *physical.Entry) error {
	stmt := fmt.Sprintf("UPSERT INTO %s (key, value) VALUES ($key, $value)", w.table)
	params := ydb.ParamsBuilder().
		Param("$key").Text(entry.Key).
		Param("$value").Bytes(entry.Value).Build()
	err := w.tx.Exec(ctx, stmt, query.WithParameters(params))
	return err
}

func (w *ydbTxWrapper) DeleteInternal(ctx context.Context, key string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE key = $key", w.table)
	params := ydb.ParamsBuilder().Param("$key").Text(key).Build()
	err := w.tx.Exec(ctx, stmt, query.WithParameters(params))
	return err
}

func (y *YDBBackend) Transaction(ctx context.Context, txns []*physical.TxnEntry) error {
	if len(txns) == 0 {
		return nil
	}
	return y.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		w := &ydbTxWrapper{tx: tx, table: y.table}
		return physical.GenericTransactionHandler(ctx, w, txns)
	})
}

// Return default transaction limits
func (y *YDBBackend) TransactionLimits() (int, int) {
	return 63, 128 * 1024
}

func getYDBOptionsFromConfMap(conf map[string]string) []ydb.Option {
	var opts []ydb.Option

	// Token: environment variables take precedence over configuration map.
	if envv := os.Getenv("VAULT_YDB_TOKEN"); envv != "" {
		opts = append(opts, ydb.WithAccessTokenCredentials(strings.TrimSpace(envv)))
	} else if v, ok := conf["token"]; ok && strings.TrimSpace(v) != "" {
		opts = append(opts, ydb.WithAccessTokenCredentials(strings.TrimSpace(v)))
	}

	// internal_ca: environment variable VAULT_YDB_INTERNAL_CA takes precedence
	internalCAVal := ""
	if envv := os.Getenv("VAULT_YDB_INTERNAL_CA"); envv != "" {
		internalCAVal = envv
	} else if v, ok := conf["internal_ca"]; ok {
		internalCAVal = v
	}
	internalCA := false
	if internalCAVal != "" && (strings.EqualFold(internalCAVal, "true") || internalCAVal == "1" || strings.EqualFold(internalCAVal, "yes")) {
		internalCA = true
	}

	// service_account_key_file: environment variable VAULT_YDB_SA_KEYFILE takes precedence
	saKeyFile := ""
	if envv := os.Getenv("VAULT_YDB_SA_KEYFILE"); envv != "" {
		saKeyFile = envv
	} else if v, ok := conf["service_account_key_file"]; ok && strings.TrimSpace(v) != "" {
		saKeyFile = strings.TrimSpace(v)
	}

	// If YC-specific options are set, use the ydb-go-yc helper to configure them.
	// This preserves previous behavior while letting explicit env vars override
	// config map values.
	if internalCA {
		opts = append(opts, yc.WithInternalCA())
	}
	if saKeyFile != "" {
		opts = append(opts, yc.WithServiceAccountKeyFileCredentials(saKeyFile))
	}

	return opts
}
