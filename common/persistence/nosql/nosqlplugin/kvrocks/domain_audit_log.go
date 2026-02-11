package kvrocks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

func domainAuditMember(createdUnixNano int64, eventID string) string {
	return encodeInt64Lex(createdUnixNano) + ":" + eventID
}

func parseDomainAuditMember(m string) (createdUnixNano int64, eventID string, err error) {
	parts := strings.SplitN(m, ":", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("kvrocks: domain audit: invalid member %q", m)
	}
	ts, err := parseInt64(parts[0])
	if err != nil {
		return 0, "", err
	}
	return ts, parts[1], nil
}

func (db *DB) InsertDomainAuditLog(ctx context.Context, row *nosqlplugin.DomainAuditLogRow) error {
	if row == nil {
		return fmt.Errorf("kvrocks: InsertDomainAuditLog: nil row")
	}
	b, err := marshalJSON(row)
	if err != nil {
		return err
	}

	exp := time.Duration(0)
	if row.TTLSeconds > 0 {
		exp = time.Duration(row.TTLSeconds) * time.Second
	}

	kRow := db.kDomainAuditRow(row.DomainID, row.OperationType, row.EventID)
	kIdx := db.kDomainAuditIndex(row.DomainID, row.OperationType)
	member := domainAuditMember(row.CreatedTime.UnixNano(), row.EventID)

	pipe := db.rdb.Pipeline()
	pipe.Set(ctx, kRow, b, exp)
	pipe.ZAdd(ctx, kIdx, redis.Z{Score: 0, Member: member})
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectDomainAuditLogs(ctx context.Context, filter *nosqlplugin.DomainAuditLogFilter) ([]*nosqlplugin.DomainAuditLogRow, []byte, error) {
	if filter == nil {
		return nil, nil, fmt.Errorf("kvrocks: SelectDomainAuditLogs: nil filter")
	}
	if filter.DomainID == "" {
		return nil, nil, fmt.Errorf("kvrocks: SelectDomainAuditLogs: empty domainID")
	}

	start := time.Unix(0, 0)
	if filter.MinCreatedTime != nil {
		start = *filter.MinCreatedTime
	}
	end := time.Now().UTC()
	if filter.MaxCreatedTime != nil {
		end = *filter.MaxCreatedTime
	}

	kIdx := db.kDomainAuditIndex(filter.DomainID, filter.OperationType)

	minBound := "[" + encodeInt64Lex(start.UnixNano()) + ":"
	maxBound := "+"
	// MaxCreatedTime is exclusive.
	if filter.MaxCreatedTime != nil {
		maxBound = "(" + encodeInt64Lex(end.UnixNano()) + ":"
	}

	members, next, err := db.zRevRangeByLexPage(ctx, kIdx, minBound, maxBound, filter.NextPageToken, filter.PageSize)
	if err != nil {
		return nil, nil, err
	}
	if len(members) == 0 {
		return nil, next, nil
	}

	rowKeys := make([]string, 0, len(members))
	eventIDs := make([]string, 0, len(members))
	for _, m := range members {
		_, eid, err := parseDomainAuditMember(m)
		if err != nil {
			return nil, nil, err
		}
		eventIDs = append(eventIDs, eid)
		rowKeys = append(rowKeys, db.kDomainAuditRow(filter.DomainID, filter.OperationType, eid))
	}

	vals, err := db.rdb.MGet(ctx, rowKeys...).Result()
	if err != nil {
		return nil, nil, err
	}

	out := make([]*nosqlplugin.DomainAuditLogRow, 0, len(vals))
	var stale []string
	for i, v := range vals {
		if v == nil {
			stale = append(stale, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectDomainAuditLogs: unexpected mget type for %q: %T", rowKeys[i], v)
		}
		r := &nosqlplugin.DomainAuditLogRow{}
		if err := unmarshalJSON([]byte(s), r); err != nil {
			return nil, nil, err
		}
		// Backfill in case older rows didn't store these fields.
		if r.DomainID == "" {
			r.DomainID = filter.DomainID
		}
		if r.EventID == "" {
			r.EventID = eventIDs[i]
		}
		if r.OperationType == persistence.DomainAuditOperationTypeInvalid {
			r.OperationType = filter.OperationType
		}
		out = append(out, r)
	}
	if len(stale) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, stale).Err()
	}

	return out, next, nil
}
