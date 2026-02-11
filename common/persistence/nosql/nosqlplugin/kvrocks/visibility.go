package kvrocks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

func visibilityMember(timeUnixNano int64, runID string) string {
	// time first for ordering, runID for uniqueness.
	return encodeInt64Lex(timeUnixNano) + ":" + runID
}

func parseVisibilityMember(m string) (timeUnixNano int64, runID string, err error) {
	parts := strings.SplitN(m, ":", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("kvrocks: visibility: invalid member %q", m)
	}
	ts, err := parseInt64(parts[0])
	if err != nil {
		return 0, "", err
	}
	return ts, parts[1], nil
}

func (db *DB) zRevRangeByLexPage(
	ctx context.Context,
	key string,
	minBound string,
	maxBound string,
	pageToken []byte,
	pageSize int,
) ([]string, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	max := maxBound
	if len(pageToken) > 0 {
		max = "(" + string(pageToken)
	}
	members, err := db.rdb.ZRevRangeByLex(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    minBound,
		Offset: 0,
		Count:  int64(pageSize) + 1,
	}).Result()
	if err != nil {
		return nil, nil, err
	}
	var next []byte
	if len(members) > pageSize {
		next = []byte(members[pageSize-1])
		members = members[:pageSize]
	}
	return members, next, nil
}

func (db *DB) InsertVisibility(ctx context.Context, ttlSeconds int64, row *nosqlplugin.VisibilityRowForInsert) error {
	if row == nil {
		return fmt.Errorf("kvrocks: InsertVisibility: nil row")
	}
	vis := row.VisibilityRow
	vis.DomainID = row.DomainID

	// RecordWorkflowExecutionStarted can be retried and, in rare cases, can arrive after the close record.
	// If the execution is already closed, keep the closed row and indices intact.
	kRow := db.kVisRow(row.DomainID, vis.RunID)
	if existingBytes, err := db.rdb.Get(ctx, kRow).Bytes(); err == nil {
		existing := &nosqlplugin.VisibilityRow{}
		if err := unmarshalJSON(existingBytes, existing); err != nil {
			return err
		}
		if existing.Status != nil {
			return nil
		}
	} else if err != redis.Nil {
		return err
	}

	b, err := marshalJSON(&vis)
	if err != nil {
		return err
	}

	exp := time.Duration(0)
	if ttlSeconds > 0 {
		exp = time.Duration(ttlSeconds) * time.Second
	}

	member := visibilityMember(vis.StartTime.UnixNano(), vis.RunID)
	pipe := db.rdb.Pipeline()
	pipe.Set(ctx, kRow, b, exp)
	pipe.ZAdd(ctx, db.kVisOpen(row.DomainID), redis.Z{Score: 0, Member: member})
	pipe.ZAdd(ctx, db.kVisOpenByType(row.DomainID, vis.TypeName), redis.Z{Score: 0, Member: member})
	pipe.ZAdd(ctx, db.kVisOpenByWorkflowID(row.DomainID, vis.WorkflowID), redis.Z{Score: 0, Member: member})
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) UpdateVisibility(ctx context.Context, ttlSeconds int64, row *nosqlplugin.VisibilityRowForUpdate) error {
	if row == nil {
		return fmt.Errorf("kvrocks: UpdateVisibility: nil row")
	}
	if row.UpdateCloseToOpen {
		// Not used by Cadence today. Implementing this correctly requires knowing the previous close status/time.
		return fmt.Errorf("kvrocks: UpdateVisibility: close-to-open not supported")
	}

	vis := row.VisibilityRow
	vis.DomainID = row.DomainID

	b, err := marshalJSON(&vis)
	if err != nil {
		return err
	}

	exp := time.Duration(0)
	if ttlSeconds > 0 {
		exp = time.Duration(ttlSeconds) * time.Second
	}

	startMember := visibilityMember(vis.StartTime.UnixNano(), vis.RunID)
	closeMember := visibilityMember(vis.CloseTime.UnixNano(), vis.RunID)

	pipe := db.rdb.Pipeline()

	// Update the row first (idempotent).
	pipe.Set(ctx, db.kVisRow(row.DomainID, vis.RunID), b, exp)

	if row.UpdateOpenToClose {
		// Remove from open indices.
		pipe.ZRem(ctx, db.kVisOpen(row.DomainID), startMember)
		pipe.ZRem(ctx, db.kVisOpenByType(row.DomainID, vis.TypeName), startMember)
		pipe.ZRem(ctx, db.kVisOpenByWorkflowID(row.DomainID, vis.WorkflowID), startMember)

		// Add to closed indices (by start time).
		pipe.ZAdd(ctx, db.kVisClosedByStart(row.DomainID), redis.Z{Score: 0, Member: startMember})
		pipe.ZAdd(ctx, db.kVisClosedByStartType(row.DomainID, vis.TypeName), redis.Z{Score: 0, Member: startMember})
		pipe.ZAdd(ctx, db.kVisClosedByStartWorkflowID(row.DomainID, vis.WorkflowID), redis.Z{Score: 0, Member: startMember})
		if vis.Status != nil {
			pipe.ZAdd(ctx, db.kVisClosedByStartStatus(row.DomainID, int32(*vis.Status)), redis.Z{Score: 0, Member: startMember})
		}

		// Add to closed indices (by close time) to support optional listClosedOrderingByCloseTime.
		pipe.ZAdd(ctx, db.kVisClosedByClose(row.DomainID), redis.Z{Score: 0, Member: closeMember})
		pipe.ZAdd(ctx, db.kVisClosedByCloseType(row.DomainID, vis.TypeName), redis.Z{Score: 0, Member: closeMember})
		pipe.ZAdd(ctx, db.kVisClosedByCloseWorkflowID(row.DomainID, vis.WorkflowID), redis.Z{Score: 0, Member: closeMember})
		if vis.Status != nil {
			pipe.ZAdd(ctx, db.kVisClosedByCloseStatus(row.DomainID, int32(*vis.Status)), redis.Z{Score: 0, Member: closeMember})
		}
	}

	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectVisibility(ctx context.Context, filter *nosqlplugin.VisibilityFilter) (*nosqlplugin.SelectVisibilityResponse, error) {
	if filter == nil {
		return nil, fmt.Errorf("kvrocks: SelectVisibility: nil filter")
	}
	if filter.ListRequest.PageSize <= 0 {
		return &nosqlplugin.SelectVisibilityResponse{}, nil
	}

	domainID := filter.ListRequest.DomainUUID
	var (
		idxKey    string
		earliest  = filter.ListRequest.EarliestTime
		latest    = filter.ListRequest.LatestTime
		pageToken = filter.ListRequest.NextPageToken
		pageSize  = filter.ListRequest.PageSize
		minBound  string
		maxBound  string
	)

	switch filter.FilterType {
	case nosqlplugin.AllOpen:
		idxKey = db.kVisOpen(domainID)
	case nosqlplugin.AllClosed:
		if filter.SortType == nosqlplugin.SortByClosedTime {
			idxKey = db.kVisClosedByClose(domainID)
		} else {
			idxKey = db.kVisClosedByStart(domainID)
		}
	case nosqlplugin.OpenByWorkflowType:
		idxKey = db.kVisOpenByType(domainID, filter.WorkflowType)
	case nosqlplugin.ClosedByWorkflowType:
		if filter.SortType == nosqlplugin.SortByClosedTime {
			idxKey = db.kVisClosedByCloseType(domainID, filter.WorkflowType)
		} else {
			idxKey = db.kVisClosedByStartType(domainID, filter.WorkflowType)
		}
	case nosqlplugin.OpenByWorkflowID:
		idxKey = db.kVisOpenByWorkflowID(domainID, filter.WorkflowID)
	case nosqlplugin.ClosedByWorkflowID:
		if filter.SortType == nosqlplugin.SortByClosedTime {
			idxKey = db.kVisClosedByCloseWorkflowID(domainID, filter.WorkflowID)
		} else {
			idxKey = db.kVisClosedByStartWorkflowID(domainID, filter.WorkflowID)
		}
	case nosqlplugin.ClosedByClosedStatus:
		if filter.SortType == nosqlplugin.SortByClosedTime {
			idxKey = db.kVisClosedByCloseStatus(domainID, filter.CloseStatus)
		} else {
			idxKey = db.kVisClosedByStartStatus(domainID, filter.CloseStatus)
		}
	default:
		return nil, fmt.Errorf("kvrocks: SelectVisibility: unsupported filter type %v", filter.FilterType)
	}

	// Time bounds are inclusive for visibility queries (match Cassandra/SQL behavior).
	minBound = "[" + visibilityMember(earliest.UnixNano(), "")
	maxBound = "[" + encodeInt64Lex(latest.UnixNano()) + ":~"

	members, next, err := db.zRevRangeByLexPage(ctx, idxKey, minBound, maxBound, pageToken, pageSize)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return &nosqlplugin.SelectVisibilityResponse{Executions: nil, NextPageToken: next}, nil
	}

	rowKeys := make([]string, 0, len(members))
	runIDs := make([]string, 0, len(members))
	for _, m := range members {
		_, runID, err := parseVisibilityMember(m)
		if err != nil {
			return nil, err
		}
		runIDs = append(runIDs, runID)
		rowKeys = append(rowKeys, db.kVisRow(domainID, runID))
	}

	vals, err := db.rdb.MGet(ctx, rowKeys...).Result()
	if err != nil {
		return nil, err
	}

	out := make([]*nosqlplugin.VisibilityRow, 0, len(vals))
	var stale []string
	for i, v := range vals {
		if v == nil {
			stale = append(stale, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("kvrocks: SelectVisibility: unexpected mget type for %q: %T", rowKeys[i], v)
		}
		r := &nosqlplugin.VisibilityRow{}
		if err := unmarshalJSON([]byte(s), r); err != nil {
			return nil, err
		}
		// Backfill in case older rows didn't store DomainID.
		if r.DomainID == "" {
			r.DomainID = domainID
		}
		// Backfill in case older rows didn't store RunID.
		if r.RunID == "" {
			r.RunID = runIDs[i]
		}
		out = append(out, r)
	}
	if len(stale) > 0 {
		_ = db.rdb.ZRem(ctx, idxKey, stale).Err()
	}

	return &nosqlplugin.SelectVisibilityResponse{
		Executions:    out,
		NextPageToken: next,
	}, nil
}

func (db *DB) DeleteVisibility(ctx context.Context, domainID, workflowID, runID string) error {
	if domainID == "" || runID == "" {
		return fmt.Errorf("kvrocks: DeleteVisibility: missing domainID/runID")
	}
	b, err := db.rdb.Get(ctx, db.kVisRow(domainID, runID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	vis := &nosqlplugin.VisibilityRow{}
	if err := unmarshalJSON(b, vis); err != nil {
		return err
	}
	_ = workflowID // workflowID may be empty in caller; trust stored row for index cleanup.

	startMember := visibilityMember(vis.StartTime.UnixNano(), runID)
	closeMember := visibilityMember(vis.CloseTime.UnixNano(), runID)

	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kVisRow(domainID, runID))

	// Best-effort cleanup from open indices.
	pipe.ZRem(ctx, db.kVisOpen(domainID), startMember)
	pipe.ZRem(ctx, db.kVisOpenByType(domainID, vis.TypeName), startMember)
	pipe.ZRem(ctx, db.kVisOpenByWorkflowID(domainID, vis.WorkflowID), startMember)

	// Best-effort cleanup from closed indices.
	pipe.ZRem(ctx, db.kVisClosedByStart(domainID), startMember)
	pipe.ZRem(ctx, db.kVisClosedByStartType(domainID, vis.TypeName), startMember)
	pipe.ZRem(ctx, db.kVisClosedByStartWorkflowID(domainID, vis.WorkflowID), startMember)

	pipe.ZRem(ctx, db.kVisClosedByClose(domainID), closeMember)
	pipe.ZRem(ctx, db.kVisClosedByCloseType(domainID, vis.TypeName), closeMember)
	pipe.ZRem(ctx, db.kVisClosedByCloseWorkflowID(domainID, vis.WorkflowID), closeMember)

	if vis.Status != nil {
		pipe.ZRem(ctx, db.kVisClosedByStartStatus(domainID, int32(*vis.Status)), startMember)
		pipe.ZRem(ctx, db.kVisClosedByCloseStatus(domainID, int32(*vis.Status)), closeMember)
	}

	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectOneClosedWorkflow(ctx context.Context, domainID, workflowID, runID string) (*nosqlplugin.VisibilityRow, error) {
	if domainID == "" || runID == "" {
		return nil, fmt.Errorf("kvrocks: SelectOneClosedWorkflow: missing domainID/runID")
	}
	b, err := db.rdb.Get(ctx, db.kVisRow(domainID, runID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	vis := &nosqlplugin.VisibilityRow{}
	if err := unmarshalJSON(b, vis); err != nil {
		return nil, err
	}
	// Ensure it's the requested execution and that it is closed.
	if workflowID != "" && vis.WorkflowID != workflowID {
		return nil, nil
	}
	if vis.Status == nil {
		return nil, nil
	}
	if vis.DomainID == "" {
		vis.DomainID = domainID
	}
	if vis.RunID == "" {
		vis.RunID = runID
	}
	return vis, nil
}
