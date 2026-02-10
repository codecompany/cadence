package kvrocks

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"

	"github.com/redis/go-redis/v9"
)

func (db *DB) SelectTaskList(ctx context.Context, filter *nosqlplugin.TaskListFilter) (*nosqlplugin.TaskListRow, error) {
	if filter == nil {
		return nil, fmt.Errorf("kvrocks: SelectTaskList: nil filter")
	}
	b, err := db.rdb.Get(ctx, db.kTaskListRow(filter.DomainID, filter.TaskListName, filter.TaskListType)).Bytes()
	if err != nil {
		return nil, err
	}
	out := &nosqlplugin.TaskListRow{}
	if err := unmarshalJSON(b, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) InsertTaskList(ctx context.Context, row *nosqlplugin.TaskListRow) error {
	if row == nil {
		return fmt.Errorf("kvrocks: InsertTaskList: nil row")
	}
	kTL := db.kTaskListRow(row.DomainID, row.TaskListName, row.TaskListType)

	keysToWatch := []string{kTL}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		if n, err := tx.Exists(ctx, kTL).Result(); err != nil {
			return err
		} else if n > 0 {
			// Return the current range ID, if possible.
			curBytes, err := tx.Get(ctx, kTL).Bytes()
			if err != nil {
				return err
			}
			cur := &nosqlplugin.TaskListRow{}
			if err := unmarshalJSON(curBytes, cur); err != nil {
				return err
			}
			return &nosqlplugin.TaskOperationConditionFailure{
				RangeID: cur.RangeID,
				Details: "tasklist already exists",
			}
		}

		b, err := marshalJSON(row)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kTL, b, 0)
			return nil
		})
		return err
	})
}

func (db *DB) UpdateTaskList(ctx context.Context, row *nosqlplugin.TaskListRow, previousRangeID int64) error {
	return db.updateTaskList(ctx, 0, row, previousRangeID)
}

func (db *DB) UpdateTaskListWithTTL(ctx context.Context, ttlSeconds int64, row *nosqlplugin.TaskListRow, previousRangeID int64) error {
	if ttlSeconds <= 0 {
		return db.updateTaskList(ctx, 0, row, previousRangeID)
	}
	return db.updateTaskList(ctx, time.Duration(ttlSeconds)*time.Second, row, previousRangeID)
}

func (db *DB) updateTaskList(ctx context.Context, ttl time.Duration, row *nosqlplugin.TaskListRow, previousRangeID int64) error {
	if row == nil {
		return fmt.Errorf("kvrocks: UpdateTaskList: nil row")
	}
	kTL := db.kTaskListRow(row.DomainID, row.TaskListName, row.TaskListType)

	keysToWatch := []string{kTL}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		curBytes, err := tx.Get(ctx, kTL).Bytes()
		if err != nil {
			return err
		}
		cur := &nosqlplugin.TaskListRow{}
		if err := unmarshalJSON(curBytes, cur); err != nil {
			return err
		}
		if cur.RangeID != previousRangeID {
			return &nosqlplugin.TaskOperationConditionFailure{
				RangeID: cur.RangeID,
				Details: fmt.Sprintf("range_id mismatch: expected=%d actual=%d", previousRangeID, cur.RangeID),
			}
		}

		b, err := marshalJSON(row)
		if err != nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, kTL, b, ttl)
			return nil
		})
		return err
	})
}

func (db *DB) ListTaskList(ctx context.Context, pageSize int, nextPageToken []byte) (*nosqlplugin.ListTaskListResult, error) {
	// Cassandra returns unsupported and the higher-level NoSQL store does not rely on this.
	return nil, &types.InternalServiceError{Message: "unsupported operation"}
}

func (db *DB) DeleteTaskList(ctx context.Context, filter *nosqlplugin.TaskListFilter, previousRangeID int64) error {
	if filter == nil {
		return fmt.Errorf("kvrocks: DeleteTaskList: nil filter")
	}
	kTL := db.kTaskListRow(filter.DomainID, filter.TaskListName, filter.TaskListType)
	kIdx := db.kTaskListTasksIndex(filter.DomainID, filter.TaskListName, filter.TaskListType)

	keysToWatch := []string{kTL}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		curBytes, err := tx.Get(ctx, kTL).Bytes()
		if err != nil {
			// If not found, delete is idempotent.
			if err == redis.Nil {
				return nil
			}
			return err
		}
		cur := &nosqlplugin.TaskListRow{}
		if err := unmarshalJSON(curBytes, cur); err != nil {
			return err
		}
		if cur.RangeID != previousRangeID {
			return &nosqlplugin.TaskOperationConditionFailure{
				RangeID: cur.RangeID,
				Details: fmt.Sprintf("range_id mismatch: expected=%d actual=%d", previousRangeID, cur.RangeID),
			}
		}

		// Best-effort cleanup of tasks.
		members, err := tx.ZRange(ctx, kIdx, 0, -1).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, m := range members {
				taskID, err := parseInt64(m)
				if err != nil {
					return err
				}
				pipe.Del(ctx, db.kTaskRow(filter.DomainID, filter.TaskListName, filter.TaskListType, taskID))
			}
			pipe.Del(ctx, kTL)
			pipe.Del(ctx, kIdx)
			return nil
		})
		return err
	})
}

func (db *DB) InsertTasks(ctx context.Context, tasksToInsert []*nosqlplugin.TaskRowForInsert, tasklistCondition *nosqlplugin.TaskListRow) error {
	if tasklistCondition == nil {
		return fmt.Errorf("kvrocks: InsertTasks: nil tasklistCondition")
	}
	kTL := db.kTaskListRow(tasklistCondition.DomainID, tasklistCondition.TaskListName, tasklistCondition.TaskListType)
	kIdx := db.kTaskListTasksIndex(tasklistCondition.DomainID, tasklistCondition.TaskListName, tasklistCondition.TaskListType)

	keysToWatch := []string{kTL}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		curBytes, err := tx.Get(ctx, kTL).Bytes()
		if err != nil {
			return err
		}
		cur := &nosqlplugin.TaskListRow{}
		if err := unmarshalJSON(curBytes, cur); err != nil {
			return err
		}
		if cur.RangeID != tasklistCondition.RangeID {
			return &nosqlplugin.TaskOperationConditionFailure{
				RangeID: cur.RangeID,
				Details: fmt.Sprintf("range_id mismatch: expected=%d actual=%d", tasklistCondition.RangeID, cur.RangeID),
			}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, t := range tasksToInsert {
				if t == nil {
					continue
				}
				key := db.kTaskRow(t.DomainID, t.TaskListName, t.TaskListType, t.TaskID)
				b, err := marshalJSON(&t.TaskRow)
				if err != nil {
					return err
				}
				exp := time.Duration(0)
				if t.TTLSeconds > 0 {
					exp = time.Duration(t.TTLSeconds) * time.Second
				}
				pipe.Set(ctx, key, b, exp)
				pipe.ZAdd(ctx, kIdx, redis.Z{Score: 0, Member: encodeInt64Lex(t.TaskID)})
			}
			return nil
		})
		return err
	})
}

func (db *DB) SelectTasks(ctx context.Context, filter *nosqlplugin.TasksFilter) ([]*nosqlplugin.TaskRow, error) {
	if filter == nil {
		return nil, fmt.Errorf("kvrocks: SelectTasks: nil filter")
	}
	if filter.BatchSize <= 0 {
		return nil, nil
	}
	kIdx := db.kTaskListTasksIndex(filter.DomainID, filter.TaskListName, filter.TaskListType)

	min := "(" + encodeInt64Lex(filter.MinTaskID)
	max := "[" + encodeInt64Lex(filter.MaxTaskID)

	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  int64(filter.BatchSize),
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(members))
	for _, m := range members {
		taskID, err := parseInt64(m)
		if err != nil {
			return nil, err
		}
		keys = append(keys, db.kTaskRow(filter.DomainID, filter.TaskListName, filter.TaskListType, taskID))
	}

	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	out := make([]*nosqlplugin.TaskRow, 0, len(vals))
	var staleMembers []string
	for i, v := range vals {
		if v == nil {
			staleMembers = append(staleMembers, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("kvrocks: SelectTasks: unexpected mget type for %q: %T", keys[i], v)
		}
		tr := &nosqlplugin.TaskRow{}
		if err := unmarshalJSON([]byte(s), tr); err != nil {
			return nil, err
		}
		out = append(out, tr)
	}

	if len(staleMembers) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, staleMembers).Err()
	}

	return out, nil
}

func (db *DB) RangeDeleteTasks(ctx context.Context, filter *nosqlplugin.TasksFilter) (rowsDeleted int, err error) {
	if filter == nil {
		return 0, fmt.Errorf("kvrocks: RangeDeleteTasks: nil filter")
	}
	kIdx := db.kTaskListTasksIndex(filter.DomainID, filter.TaskListName, filter.TaskListType)

	min := "(" + encodeInt64Lex(filter.MinTaskID)
	max := "[" + encodeInt64Lex(filter.MaxTaskID)

	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(members) == 0 {
		return persistence.UnknownNumRowsAffected, nil
	}

	pipe := db.rdb.Pipeline()
	for _, m := range members {
		taskID, err := parseInt64(m)
		if err != nil {
			return 0, err
		}
		pipe.Del(ctx, db.kTaskRow(filter.DomainID, filter.TaskListName, filter.TaskListType, taskID))
	}
	pipe.ZRem(ctx, kIdx, members)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return persistence.UnknownNumRowsAffected, nil
}

func (db *DB) GetTasksCount(ctx context.Context, filter *nosqlplugin.TasksFilter) (int64, error) {
	if filter == nil {
		return 0, fmt.Errorf("kvrocks: GetTasksCount: nil filter")
	}
	kIdx := db.kTaskListTasksIndex(filter.DomainID, filter.TaskListName, filter.TaskListType)
	min := "(" + encodeInt64Lex(filter.MinTaskID)
	return db.rdb.ZLexCount(ctx, kIdx, min, "+").Result()
}
