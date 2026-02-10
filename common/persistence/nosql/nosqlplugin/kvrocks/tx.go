package kvrocks

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const defaultWatchMaxAttempts = 10

func (db *DB) watchWithRetry(ctx context.Context, keys []string, fn func(tx *redis.Tx) error) error {
	for attempt := 1; attempt <= defaultWatchMaxAttempts; attempt++ {
		err := db.rdb.Watch(ctx, fn, keys...)
		if errors.Is(err, redis.TxFailedErr) {
			// Watched key changed; retry.
			continue
		}
		return err
	}
	return fmt.Errorf("kvrocks: transaction failed after %d attempts: %w", defaultWatchMaxAttempts, redis.TxFailedErr)
}
