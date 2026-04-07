package ydb

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/vault/sdk/physical"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	coordoptions "github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
)

const ydbHALockSessionTimeout = 15 * time.Second

var _ physical.Lock = (*ydbHALock)(nil)

type ydbHALock struct {
	backend   *YDBBackend
	key       string
	value     string
	semaphore string

	mu      sync.Mutex
	held    bool
	session coordination.Session
	lease   coordination.Lease
}

func (y *YDBBackend) HAEnabled() bool {
	return y.haEnabled
}

func (y *YDBBackend) LockWith(key, value string) (physical.Lock, error) {
	return &ydbHALock{
		backend:   y,
		key:       key,
		value:     value,
		semaphore: ydbHASemaphoreName(key),
	}, nil
}

func (l *ydbHALock) Lock(stopCh <-chan struct{}) (<-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.held {
		return nil, fmt.Errorf("lock already held")
	}

	ctx, cancel := context.WithTimeout(context.Background(), ydbHALockSessionTimeout)
	defer cancel()

	if err := l.backend.ensureCoordinationNodeExists(ctx); err != nil {
		return nil, err
	}

	session, err := l.backend.db.Coordination().Session(
		ctx,
		l.backend.coordinationNode,
		coordoptions.WithDescription("vault lock "+l.key),
		coordoptions.WithSessionTimeout(ydbHALockSessionTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create coordination session: %w", err)
	}

	acquireCtx := context.Background()
	acquireCancel := func() {}
	acquireDone := make(chan struct{})
	if stopCh != nil {
		var cancel context.CancelFunc
		acquireCtx, cancel = context.WithCancel(context.Background())
		acquireCancel = cancel
		go func() {
			select {
			case <-stopCh:
				cancel()
			case <-acquireDone:
			}
		}()
	}

	lease, err := session.AcquireSemaphore(
		acquireCtx,
		l.semaphore,
		coordination.Exclusive,
		coordoptions.WithEphemeral(true),
		coordoptions.WithAcquireData([]byte(l.value)),
	)
	close(acquireDone)
	acquireCancel()
	if err != nil {
		_ = session.Close(context.Background())
		if stopCh != nil && errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to acquire coordination semaphore: %w", err)
	}

	l.held = true
	l.session = session
	l.lease = lease

	return lease.Context().Done(), nil
}

func (l *ydbHALock) Unlock() error {
	l.mu.Lock()
	if !l.held {
		l.mu.Unlock()
		return nil
	}

	lease := l.lease
	session := l.session
	l.held = false
	l.lease = nil
	l.session = nil
	l.mu.Unlock()

	var unlockErr error
	if lease != nil {
		unlockErr = lease.Release()
	}
	if session != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), ydbHALockSessionTimeout)
		defer cancel()
		if err := session.Close(closeCtx); unlockErr == nil {
			unlockErr = err
		}
	}

	return unlockErr
}

func (l *ydbHALock) Value() (bool, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ydbHALockSessionTimeout)
	defer cancel()

	if err := l.backend.ensureCoordinationNodeExists(ctx); err != nil {
		return false, "", err
	}

	session, err := l.backend.db.Coordination().Session(
		ctx,
		l.backend.coordinationNode,
		coordoptions.WithSessionTimeout(ydbHALockSessionTimeout),
	)
	if err != nil {
		return false, "", fmt.Errorf("failed to create coordination session: %w", err)
	}
	defer session.Close(context.Background())

	desc, err := session.DescribeSemaphore(ctx, l.semaphore, coordoptions.WithDescribeOwners(true))
	if err != nil {
		if ydb.IsOperationErrorSchemeError(err) {
			return false, "", nil
		}
		return false, "", fmt.Errorf("failed to describe coordination semaphore: %w", err)
	}
	if len(desc.Owners) == 0 {
		return false, "", nil
	}

	return true, string(desc.Owners[0].Data), nil
}

func (y *YDBBackend) ensureCoordinationNodeExists(ctx context.Context) error {
	err := y.db.Coordination().CreateNode(ctx, y.coordinationNode, coordination.NodeConfig{
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeStrict,
		AttachConsistencyMode:    coordination.ConsistencyModeStrict,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err == nil || ydb.IsOperationErrorAlreadyExistsError(err) {
		return nil
	}
	return fmt.Errorf("failed to ensure coordination node exists: %w", err)
}

func ydbHASemaphoreName(key string) string {
	return "vault-lock-" + base64.RawURLEncoding.EncodeToString([]byte(key))
}
