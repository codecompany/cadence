package kvrocks

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"

	"github.com/redis/go-redis/v9"
)

func (db *DB) InsertDomain(ctx context.Context, row *nosqlplugin.DomainRow) error {
	if row == nil || row.Info == nil {
		return fmt.Errorf("kvrocks: InsertDomain: nil row/info")
	}
	name := row.Info.Name
	id := row.Info.ID

	kByName := db.kDomainByName(name)
	kByID := db.kDomainByID(id)
	kRow := db.kDomainRow(name)
	kMeta := db.kDomainMeta()
	kNames := db.kDomainNamesIndex()

	keysToWatch := []string{kByName, kByID, kMeta}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		// Check for collisions.
		if n, err := tx.Exists(ctx, kByName).Result(); err != nil {
			return err
		} else if n > 0 {
			return &types.DomainAlreadyExistsError{Message: fmt.Sprintf("Domain %v already exists", name)}
		}
		if n, err := tx.Exists(ctx, kByID).Result(); err != nil {
			return err
		} else if n > 0 {
			return &types.DomainAlreadyExistsError{Message: fmt.Sprintf("Domain %v already exists", name)}
		}

		metaStr, err := tx.Get(ctx, kMeta).Result()
		metaVersion := int64(0)
		if err != nil {
			if err != redis.Nil {
				return err
			}
		} else {
			metaVersion, err = parseInt64(metaStr)
			if err != nil {
				return fmt.Errorf("kvrocks: InsertDomain: parse domain metadata version: %w", err)
			}
		}

		// Match Cassandra semantics: domain row stores the current metadata version, and metadata increments after write.
		row.NotificationVersion = metaVersion
		rowBytes, err := marshalJSON(row)
		if err != nil {
			return err
		}
		nextMeta := metaVersion + 1

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kByName, id, 0)
			pipe.Set(ctx, kByID, name, 0)
			pipe.Set(ctx, kRow, rowBytes, 0)
			pipe.ZAdd(ctx, kNames, redis.Z{Score: 0, Member: name})
			pipe.Set(ctx, kMeta, fmt.Sprintf("%d", nextMeta), 0)
			return nil
		})
		return err
	})
}

func (db *DB) UpdateDomain(ctx context.Context, row *nosqlplugin.DomainRow) error {
	if row == nil || row.Info == nil {
		return fmt.Errorf("kvrocks: UpdateDomain: nil row/info")
	}
	name := row.Info.Name
	id := row.Info.ID

	kByName := db.kDomainByName(name)
	kRow := db.kDomainRow(name)
	kMeta := db.kDomainMeta()

	keysToWatch := []string{kByName, kMeta}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		storedID, err := tx.Get(ctx, kByName).Result()
		if err != nil {
			return err // includes redis.Nil => not found
		}
		if storedID != id {
			// Name belongs to a different domain ID. Treat as conditional failure.
			return nosqlplugin.NewConditionFailure("domain")
		}

		metaStr, err := tx.Get(ctx, kMeta).Result()
		metaVersion := int64(0)
		if err != nil {
			if err != redis.Nil {
				return err
			}
		} else {
			metaVersion, err = parseInt64(metaStr)
			if err != nil {
				return fmt.Errorf("kvrocks: UpdateDomain: parse domain metadata version: %w", err)
			}
		}

		// Enforce metadata CAS.
		if metaVersion != row.NotificationVersion {
			return nosqlplugin.NewConditionFailure("domain")
		}

		rowBytes, err := marshalJSON(row)
		if err != nil {
			return err
		}
		nextMeta := metaVersion + 1

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kRow, rowBytes, 0)
			pipe.Set(ctx, kMeta, fmt.Sprintf("%d", nextMeta), 0)
			return nil
		})
		return err
	})
}

func (db *DB) SelectDomain(ctx context.Context, domainID *string, domainName *string) (*nosqlplugin.DomainRow, error) {
	if domainID != nil && domainName != nil {
		return nil, fmt.Errorf("kvrocks: SelectDomain: both ID and name provided")
	}
	if domainID == nil && domainName == nil {
		return nil, fmt.Errorf("kvrocks: SelectDomain: both ID and name are empty")
	}

	var name string
	if domainName != nil {
		name = *domainName
	} else {
		n, err := db.rdb.Get(ctx, db.kDomainByID(*domainID)).Result()
		if err != nil {
			return nil, err
		}
		name = n
	}

	b, err := db.rdb.Get(ctx, db.kDomainRow(name)).Bytes()
	if err != nil {
		return nil, err
	}
	out := &nosqlplugin.DomainRow{}
	if err := unmarshalJSON(b, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) SelectAllDomains(ctx context.Context, pageSize int, pageToken []byte) ([]*nosqlplugin.DomainRow, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}

	kNames := db.kDomainNamesIndex()
	min := "-"
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	names, err := db.rdb.ZRangeByLex(ctx, kNames, &redis.ZRangeBy{
		Min:    min,
		Max:    "+",
		Offset: 0,
		Count:  int64(pageSize) + 1,
	}).Result()
	if err != nil {
		return nil, nil, err
	}

	var next []byte
	if len(names) > pageSize {
		next = []byte(names[pageSize-1])
		names = names[:pageSize]
	}

	if len(names) == 0 {
		return nil, next, nil
	}

	keys := make([]string, 0, len(names))
	for _, n := range names {
		keys = append(keys, db.kDomainRow(n))
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}

	rows := make([]*nosqlplugin.DomainRow, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			// Index entry without backing row; ignore.
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectAllDomains: unexpected mget type for %q: %T", keys[i], v)
		}
		r := &nosqlplugin.DomainRow{}
		if err := unmarshalJSON([]byte(s), r); err != nil {
			return nil, nil, err
		}
		rows = append(rows, r)
	}

	return rows, next, nil
}

func (db *DB) DeleteDomain(ctx context.Context, domainID *string, domainName *string) error {
	if domainID == nil && domainName == nil {
		return fmt.Errorf("kvrocks: DeleteDomain: must provide either domainID or domainName")
	}

	var (
		id   string
		name string
	)
	if domainName != nil {
		name = *domainName
		storedID, err := db.rdb.Get(ctx, db.kDomainByName(name)).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		id = storedID
	} else {
		id = *domainID
		storedName, err := db.rdb.Get(ctx, db.kDomainByID(id)).Result()
		if err != nil {
			if err == redis.Nil {
				return nil
			}
			return err
		}
		name = storedName
	}

	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kDomainByName(name))
	pipe.Del(ctx, db.kDomainByID(id))
	pipe.Del(ctx, db.kDomainRow(name))
	pipe.ZRem(ctx, db.kDomainNamesIndex(), name)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) SelectDomainMetadata(ctx context.Context) (int64, error) {
	s, err := db.rdb.Get(ctx, db.kDomainMeta()).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	v, err := parseInt64(s)
	if err != nil {
		return 0, fmt.Errorf("kvrocks: SelectDomainMetadata: parse: %w", err)
	}
	return v, nil
}
