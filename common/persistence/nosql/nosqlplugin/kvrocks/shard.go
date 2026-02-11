package kvrocks

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

func (db *DB) InsertShard(ctx context.Context, row *nosqlplugin.ShardRow) error {
	if row == nil || row.InternalShardInfo == nil {
		return fmt.Errorf("kvrocks: InsertShard: nil row/shard")
	}
	shardID := row.ShardID
	kRange := db.kShardRange(shardID)
	kRow := db.kShardRow(shardID)

	keysToWatch := []string{kRange}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		if n, err := tx.Exists(ctx, kRange).Result(); err != nil {
			return err
		} else if n > 0 {
			// Already exists.
			curStr, err := tx.Get(ctx, kRange).Result()
			if err != nil && err != redis.Nil {
				return err
			}
			cur := int64(0)
			if curStr != "" {
				if cur, err = parseInt64(curStr); err != nil {
					return fmt.Errorf("kvrocks: InsertShard: parse range: %w", err)
				}
			}
			return &nosqlplugin.ShardOperationConditionFailure{
				RangeID: cur,
				Details: "shard already exists",
			}
		}

		b, err := marshalJSON(row)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kRange, fmt.Sprintf("%d", row.RangeID), 0)
			pipe.Set(ctx, kRow, b, 0)
			return nil
		})
		return err
	})
}

func (db *DB) SelectShard(ctx context.Context, shardID int, currentClusterName string) (rangeID int64, shard *nosqlplugin.ShardRow, err error) {
	kRow := db.kShardRow(shardID)
	rowBytes, err := db.rdb.Get(ctx, kRow).Bytes()
	if err != nil {
		return 0, nil, err
	}
	out := &nosqlplugin.ShardRow{}
	if err := unmarshalJSON(rowBytes, out); err != nil {
		return 0, nil, err
	}
	// Best-effort defaulting for older rows.
	if out.InternalShardInfo != nil {
		if out.ClusterTransferAckLevel == nil {
			out.ClusterTransferAckLevel = map[string]int64{
				currentClusterName: out.TransferAckLevel,
			}
		}
		if out.ClusterTimerAckLevel == nil {
			out.ClusterTimerAckLevel = map[string]time.Time{
				currentClusterName: out.TimerAckLevel,
			}
		}
		if out.ClusterReplicationLevel == nil {
			out.ClusterReplicationLevel = make(map[string]int64)
		}
		if out.ReplicationDLQAckLevel == nil {
			out.ReplicationDLQAckLevel = make(map[string]int64)
		}
	}

	rangeStr, err := db.rdb.Get(ctx, db.kShardRange(shardID)).Result()
	if err != nil {
		// If row exists but range doesn't, treat as corruption.
		return 0, nil, err
	}
	rangeID, err = parseInt64(rangeStr)
	if err != nil {
		return 0, nil, fmt.Errorf("kvrocks: SelectShard: parse range: %w", err)
	}
	return rangeID, out, nil
}

func (db *DB) UpdateRangeID(ctx context.Context, shardID int, rangeID int64, previousRangeID int64) error {
	kRange := db.kShardRange(shardID)

	keysToWatch := []string{kRange}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		curStr, err := tx.Get(ctx, kRange).Result()
		if err != nil {
			return err
		}
		cur, err := parseInt64(curStr)
		if err != nil {
			return fmt.Errorf("kvrocks: UpdateRangeID: parse range: %w", err)
		}
		if cur != previousRangeID {
			return &nosqlplugin.ShardOperationConditionFailure{
				RangeID: cur,
				Details: fmt.Sprintf("range_id mismatch: expected=%d actual=%d", previousRangeID, cur),
			}
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kRange, fmt.Sprintf("%d", rangeID), 0)
			return nil
		})
		return err
	})
}

func (db *DB) UpdateShard(ctx context.Context, row *nosqlplugin.ShardRow, previousRangeID int64) error {
	if row == nil || row.InternalShardInfo == nil {
		return fmt.Errorf("kvrocks: UpdateShard: nil row/shard")
	}
	shardID := row.ShardID
	kRange := db.kShardRange(shardID)
	kRow := db.kShardRow(shardID)

	keysToWatch := []string{kRange}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		curStr, err := tx.Get(ctx, kRange).Result()
		if err != nil {
			return err
		}
		cur, err := parseInt64(curStr)
		if err != nil {
			return fmt.Errorf("kvrocks: UpdateShard: parse range: %w", err)
		}
		if cur != previousRangeID {
			return &nosqlplugin.ShardOperationConditionFailure{
				RangeID: cur,
				Details: fmt.Sprintf("range_id mismatch: expected=%d actual=%d", previousRangeID, cur),
			}
		}

		b, err := marshalJSON(row)
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kRange, fmt.Sprintf("%d", row.RangeID), 0)
			pipe.Set(ctx, kRow, b, 0)
			return nil
		})
		return err
	})
}
