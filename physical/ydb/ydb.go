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

var _ physical.Backend = (*YDBBackend)(nil)

func NewYDBBackend(conf map[string]string, logger log.Logger) (physical.Backend, error) {
	dsn := strings.TrimSpace(conf["dsn"])
	if dsn == "" {
		if envDSN := os.Getenv("VAULT_YDB_DSN"); envDSN != "" {
			dsn = envDSN
		} else {
			const errStr = "YDB: dsn is not set"
			return &YDBBackend{}, fmt.Errorf(errStr)
		}
	}

	table := strings.TrimSpace(conf["table"])
	if table == "" {
		if envTable := os.Getenv("VAULT_YDB_TABLE"); envTable != "" {
			table = envTable
		} else {
			table = VAULT_TABLE
		}
	}

	var opts []ydb.Option

	internalCAVal := ""
	if v, ok := conf["internal_ca"]; ok {
		internalCAVal = v
	} else if envv := os.Getenv("VAULT_YDB_INTERNAL_CA"); envv != "" {
		internalCAVal = envv
	}
	internalCA := false
	if internalCAVal != "" && (strings.EqualFold(internalCAVal, "true") || internalCAVal == "1" || strings.EqualFold(internalCAVal, "yes")) {
		internalCA = true
	}

	saKeyFile := ""
	if v, ok := conf["service_account_key_file"]; ok && strings.TrimSpace(v) != "" {
		saKeyFile = strings.TrimSpace(v)
	} else if envv := os.Getenv("VAULT_YDB_SA_KEYFILE"); envv != "" {
		saKeyFile = envv
	}

	if internalCA || saKeyFile != "" {
		logger.Info("YDB: configuring Yandex.Cloud helper options for secure connection", "internal_ca", internalCA, "sa_keyfile_set", saKeyFile != "")
		if internalCA {
			opts = append(opts, yc.WithInternalCA())
		}
		if saKeyFile != "" {
			if _, err := os.Stat(saKeyFile); err != nil {
				logger.Warn("YDB: service account key file not accessible; attempting to proceed and let yc package surface the error", "path", saKeyFile, "err", err)
			}
			opts = append(opts, yc.WithServiceAccountKeyFileCredentials(saKeyFile))
		}
	} else {
		if strings.HasPrefix(strings.ToLower(dsn), "grpcs://") {
			logger.Info("YDB: detected grpcs DSN; attempting secure connection with provided DSN. If you need YC-specific auth (internal CA or service account), set \"internal_ca\" or \"service_account_key_file\" in config or VAULT_YDB_INTERNAL_CA/VAULT_YDB_SA_KEYFILE env vars.")
		}
	}

	// Open YDB driver with options (may be empty)
	db, err := ydb.Open(context.TODO(), dsn, opts...)
	if err != nil {
		errStr := "YDB: failed to open database connection"
		logger.Error(errStr, "error", err)
		return &YDBBackend{}, fmt.Errorf(errStr+": %w", err)
	}

	// TODO: Ensure the table exists or create schema as required.

	return &YDBBackend{
		db:     db,
		table:  table,
		logger: logger,
	}, nil
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
	stmt := fmt.Sprintf("SELECT key, value FROM %s WHERE key = $key", y.table)
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

	tmp := struct {
		Key   string `sql:"key"`
		Value []byte `sql:"value"`
	}{}

	if err = q.ScanStruct(&tmp); err != nil {
		errStr := "YDB: failed to get key " + key
		y.logger.Error(errStr, "error", err)
		return nil, fmt.Errorf(errStr+" %w", err)
	}

	entry := physical.Entry{
		Key:   tmp.Key,
		Value: tmp.Value,
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

func (y *YDBBackend) Transaction(ctx context.Context, tx []*physical.TxnEntry) error {
	return nil
}
