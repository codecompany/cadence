package kvrocks

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

func (db *DB) InsertConfig(ctx context.Context, row *persistence.InternalConfigStoreEntry) error {
	if row == nil {
		return fmt.Errorf("kvrocks: InsertConfig: nil row")
	}
	rowType := int(row.RowType)
	version := row.Version

	kVersions := db.kCfgVersions(rowType)
	kEntry := db.kCfgEntry(rowType, version)

	keysToWatch := []string{kEntry}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		// Condition: same version must not already exist.
		if n, err := tx.Exists(ctx, kEntry).Result(); err != nil {
			return err
		} else if n > 0 {
			return nosqlplugin.NewConditionFailure("config_store")
		}

		b, err := marshalJSON(row)
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kEntry, b, 0)
			// Keep an ordered set of versions for quick "latest" selection.
			pipe.ZAdd(ctx, kVersions, redis.Z{Score: 0, Member: encodeInt64Lex(version)})
			return nil
		})
		return err
	})
}

func (db *DB) SelectLatestConfig(ctx context.Context, rowType int) (*persistence.InternalConfigStoreEntry, error) {
	kVersions := db.kCfgVersions(rowType)
	// Lex order, so last element is the largest version.
	members, err := db.rdb.ZRevRange(ctx, kVersions, 0, 0).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, redis.Nil
	}
	ver, err := parseInt64(members[0])
	if err != nil {
		return nil, fmt.Errorf("kvrocks: SelectLatestConfig: parse version: %w", err)
	}

	b, err := db.rdb.Get(ctx, db.kCfgEntry(rowType, ver)).Bytes()
	if err != nil {
		return nil, err
	}
	out := &persistence.InternalConfigStoreEntry{}
	if err := unmarshalJSON(b, out); err != nil {
		return nil, err
	}
	return out, nil
}
