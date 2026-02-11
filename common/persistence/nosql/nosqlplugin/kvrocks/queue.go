package kvrocks

import (
	"context"
	"fmt"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

func (db *DB) InsertIntoQueue(ctx context.Context, row *nosqlplugin.QueueMessageRow) error {
	if row == nil {
		return fmt.Errorf("kvrocks: InsertIntoQueue: nil row")
	}
	kIdx := db.kQueueIndex(row.QueueType)
	kMsg := db.kQueueMessage(row.QueueType, row.ID)

	keysToWatch := []string{kMsg}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		if n, err := tx.Exists(ctx, kMsg).Result(); err != nil {
			return err
		} else if n > 0 {
			return nosqlplugin.NewConditionFailure("queue")
		}
		// Store payload bytes directly to avoid JSON base64 overhead.
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kMsg, row.Payload, 0)
			pipe.ZAdd(ctx, kIdx, redis.Z{Score: 0, Member: encodeInt64Lex(row.ID)})
			return nil
		})
		return err
	})
}

func (db *DB) SelectLastEnqueuedMessageID(ctx context.Context, queueType persistence.QueueType) (int64, error) {
	kIdx := db.kQueueIndex(queueType)
	members, err := db.rdb.ZRevRange(ctx, kIdx, 0, 0).Result()
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return 0, redis.Nil
	}
	id, err := parseInt64(members[0])
	if err != nil {
		return 0, fmt.Errorf("kvrocks: SelectLastEnqueuedMessageID: parse: %w", err)
	}
	return id, nil
}

func (db *DB) SelectMessagesFrom(ctx context.Context, queueType persistence.QueueType, exclusiveBeginMessageID int64, maxRows int) ([]*nosqlplugin.QueueMessageRow, error) {
	if maxRows <= 0 {
		return nil, nil
	}

	kIdx := db.kQueueIndex(queueType)

	min := "-"
	if exclusiveBeginMessageID >= 0 {
		min = "(" + encodeInt64Lex(exclusiveBeginMessageID)
	}

	ids, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    "+",
		Offset: 0,
		Count:  int64(maxRows),
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(ids))
	parsed := make([]int64, 0, len(ids))
	for _, idStr := range ids {
		id, err := parseInt64(idStr)
		if err != nil {
			return nil, fmt.Errorf("kvrocks: SelectMessagesFrom: parse id: %w", err)
		}
		parsed = append(parsed, id)
		keys = append(keys, db.kQueueMessage(queueType, id))
	}

	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	out := make([]*nosqlplugin.QueueMessageRow, 0, len(vals))
	var staleMembers []string
	for i, v := range vals {
		if v == nil {
			staleMembers = append(staleMembers, ids[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("kvrocks: SelectMessagesFrom: unexpected mget type for %q: %T", keys[i], v)
		}
		out = append(out, &nosqlplugin.QueueMessageRow{
			QueueType: queueType,
			ID:        parsed[i],
			Payload:   []byte(s),
		})
	}
	// Best-effort cleanup of index entries whose payload has expired/been removed.
	if len(staleMembers) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, staleMembers).Err()
	}
	return out, nil
}

func (db *DB) SelectMessagesBetween(ctx context.Context, request nosqlplugin.SelectMessagesBetweenRequest) (*nosqlplugin.SelectMessagesBetweenResponse, error) {
	if request.PageSize <= 0 {
		return &nosqlplugin.SelectMessagesBetweenResponse{}, nil
	}

	kIdx := db.kQueueIndex(request.QueueType)
	min := "-"
	if len(request.NextPageToken) > 0 {
		min = "(" + string(request.NextPageToken)
	} else if request.ExclusiveBeginMessageID >= 0 {
		min = "(" + encodeInt64Lex(request.ExclusiveBeginMessageID)
	}
	max := "[" + encodeInt64Lex(request.InclusiveEndMessageID)

	ids, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  int64(request.PageSize) + 1,
	}).Result()
	if err != nil {
		return nil, err
	}

	var next []byte
	if len(ids) > request.PageSize {
		next = []byte(ids[request.PageSize-1])
		ids = ids[:request.PageSize]
	}

	keys := make([]string, 0, len(ids))
	parsed := make([]int64, 0, len(ids))
	for _, idStr := range ids {
		id, err := parseInt64(idStr)
		if err != nil {
			return nil, fmt.Errorf("kvrocks: SelectMessagesBetween: parse id: %w", err)
		}
		parsed = append(parsed, id)
		keys = append(keys, db.kQueueMessage(request.QueueType, id))
	}

	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	rows := make([]nosqlplugin.QueueMessageRow, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("kvrocks: SelectMessagesBetween: unexpected mget type for %q: %T", keys[i], v)
		}
		rows = append(rows, nosqlplugin.QueueMessageRow{
			QueueType: request.QueueType,
			ID:        parsed[i],
			Payload:   []byte(s),
		})
	}

	return &nosqlplugin.SelectMessagesBetweenResponse{
		Rows:          rows,
		NextPageToken: next,
	}, nil
}

func (db *DB) DeleteMessagesBefore(ctx context.Context, queueType persistence.QueueType, exclusiveBeginMessageID int64) error {
	// Delete all message IDs < exclusiveBeginMessageID.
	if exclusiveBeginMessageID <= 0 {
		return nil
	}
	return db.deleteQueueRange(ctx, queueType, "-", "("+encodeInt64Lex(exclusiveBeginMessageID))
}

func (db *DB) DeleteMessagesInRange(ctx context.Context, queueType persistence.QueueType, exclusiveBeginMessageID int64, inclusiveEndMessageID int64) error {
	min := "-"
	if exclusiveBeginMessageID >= 0 {
		min = "(" + encodeInt64Lex(exclusiveBeginMessageID)
	}
	max := "[" + encodeInt64Lex(inclusiveEndMessageID)
	return db.deleteQueueRange(ctx, queueType, min, max)
}

func (db *DB) DeleteMessage(ctx context.Context, queueType persistence.QueueType, messageID int64) error {
	kIdx := db.kQueueIndex(queueType)
	member := encodeInt64Lex(messageID)
	kMsg := db.kQueueMessage(queueType, messageID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, kMsg)
	pipe.ZRem(ctx, kIdx, member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) deleteQueueRange(ctx context.Context, queueType persistence.QueueType, min, max string) error {
	kIdx := db.kQueueIndex(queueType)
	ids, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  0, // 0 means "all"
	}).Result()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}

	pipe := db.rdb.Pipeline()
	for _, idStr := range ids {
		id, err := parseInt64(idStr)
		if err != nil {
			return fmt.Errorf("kvrocks: deleteQueueRange: parse id: %w", err)
		}
		pipe.Del(ctx, db.kQueueMessage(queueType, id))
	}
	pipe.ZRem(ctx, kIdx, ids)
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) InsertQueueMetadata(ctx context.Context, row nosqlplugin.QueueMetadataRow) error {
	kMeta := db.kQueueMetadata(row.QueueType)
	// Match Cassandra behavior: OK if already exists.
	if row.ClusterAckLevels == nil {
		row.ClusterAckLevels = map[string]int64{}
	}
	b, err := marshalJSON(row)
	if err != nil {
		return err
	}
	_, err = db.rdb.SetNX(ctx, kMeta, b, 0).Result()
	return err
}

func (db *DB) UpdateQueueMetadataCas(ctx context.Context, row nosqlplugin.QueueMetadataRow) error {
	kMeta := db.kQueueMetadata(row.QueueType)
	keysToWatch := []string{kMeta}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		b, err := tx.Get(ctx, kMeta).Bytes()
		if err != nil {
			return err
		}
		var cur nosqlplugin.QueueMetadataRow
		if err := unmarshalJSON(b, &cur); err != nil {
			return err
		}
		if cur.Version != row.Version-1 {
			return nosqlplugin.NewConditionFailure("queue")
		}
		if row.ClusterAckLevels == nil {
			row.ClusterAckLevels = map[string]int64{}
		}
		newBytes, err := marshalJSON(row)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kMeta, newBytes, 0)
			return nil
		})
		return err
	})
}

func (db *DB) SelectQueueMetadata(ctx context.Context, queueType persistence.QueueType) (*nosqlplugin.QueueMetadataRow, error) {
	b, err := db.rdb.Get(ctx, db.kQueueMetadata(queueType)).Bytes()
	if err != nil {
		return nil, err
	}
	var out nosqlplugin.QueueMetadataRow
	if err := unmarshalJSON(b, &out); err != nil {
		return nil, err
	}
	if out.ClusterAckLevels == nil {
		out.ClusterAckLevels = map[string]int64{}
	}
	return &out, nil
}

func (db *DB) GetQueueSize(ctx context.Context, queueType persistence.QueueType) (int64, error) {
	return db.rdb.ZCard(ctx, db.kQueueIndex(queueType)).Result()
}
