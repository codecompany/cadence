package kvrocks

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"
	"github.com/uber/cadence/common/types"

	"github.com/redis/go-redis/v9"
)

func histAllTreesMember(shardID int, treeID, branchID string) string {
	// shardID is included so SelectAllHistoryTrees can enumerate across shards when history is not sharded.
	return encodeInt64Lex(int64(shardID)) + ":" + treeID + ":" + branchID
}

func parseHistAllTreesMember(m string) (shardID int, treeID, branchID string, err error) {
	parts := strings.SplitN(m, ":", 3)
	if len(parts) != 3 {
		return 0, "", "", fmt.Errorf("kvrocks: history: invalid tree index member %q", m)
	}
	sid, err := parseInt64(parts[0])
	if err != nil {
		return 0, "", "", fmt.Errorf("kvrocks: history: parse shardID: %w", err)
	}
	return int(sid), parts[1], parts[2], nil
}

func histNodeMember(nodeID, txnID int64) string {
	// Order by nodeID ASC, txnID DESC.
	invTxn := int64(math.MaxInt64) - txnID
	return encodeInt64Lex(nodeID) + ":" + encodeInt64Lex(invTxn)
}

func parseHistNodeMember(m string) (nodeID, txnID int64, err error) {
	parts := strings.SplitN(m, ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("kvrocks: history: invalid node index member %q", m)
	}
	nodeID, err = parseInt64(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("kvrocks: history: parse nodeID: %w", err)
	}
	inv, err := parseInt64(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("kvrocks: history: parse invTxn: %w", err)
	}
	txnID = int64(math.MaxInt64) - inv
	return nodeID, txnID, nil
}

func normalizeBranchAncestors(ancestors []*types.HistoryBranchRange) []*types.HistoryBranchRange {
	if len(ancestors) == 0 {
		return nil
	}
	out := make([]*types.HistoryBranchRange, 0, len(ancestors))
	for _, a := range ancestors {
		if a == nil {
			continue
		}
		out = append(out, &types.HistoryBranchRange{
			BranchID:  a.BranchID,
			EndNodeID: a.EndNodeID,
		})
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EndNodeID < out[j].EndNodeID })
	out[0].BeginNodeID = 1
	for i := 1; i < len(out); i++ {
		out[i].BeginNodeID = out[i-1].EndNodeID
	}
	return out
}

func (db *DB) InsertIntoHistoryTreeAndNode(ctx context.Context, treeRow *nosqlplugin.HistoryTreeRow, nodeRow *nosqlplugin.HistoryNodeRow) error {
	if treeRow == nil && nodeRow == nil {
		return fmt.Errorf("kvrocks: InsertIntoHistoryTreeAndNode: require at least a tree row or a node row")
	}

	var shardID int
	if nodeRow != nil {
		shardID = nodeRow.ShardID
	} else {
		shardID = treeRow.ShardID
	}

	_, err := db.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		if treeRow != nil {
			// Store ancestors without BeginNodeID; we recompute it on read for consistency with Cassandra.
			treeCopy := *treeRow
			treeCopy.Ancestors = normalizeBranchAncestors(treeRow.Ancestors)
			b, err := marshalJSON(&treeCopy)
			if err != nil {
				return err
			}
			pipe.Set(ctx, db.kHistTreeRow(treeRow.ShardID, treeRow.TreeID, treeRow.BranchID), b, 0)
			pipe.SAdd(ctx, db.kHistTreeBranches(treeRow.ShardID, treeRow.TreeID), treeRow.BranchID)
			pipe.ZAdd(ctx, db.kHistAllTreesIndex(), redis.Z{Score: 0, Member: histAllTreesMember(treeRow.ShardID, treeRow.TreeID, treeRow.BranchID)})
		}
		if nodeRow != nil {
			if nodeRow.TxnID == nil {
				return fmt.Errorf("kvrocks: InsertIntoHistoryTreeAndNode: nil txnID")
			}
			val := encodeFramedStringAndBytes(nodeRow.DataEncoding, nodeRow.Data)
			pipe.Set(ctx, db.kHistNodeRow(shardID, nodeRow.TreeID, nodeRow.BranchID, nodeRow.NodeID, *nodeRow.TxnID), val, 0)
			pipe.ZAdd(ctx, db.kHistNodeIndex(shardID, nodeRow.TreeID, nodeRow.BranchID), redis.Z{Score: 0, Member: histNodeMember(nodeRow.NodeID, *nodeRow.TxnID)})
		}
		return nil
	})
	return err
}

func (db *DB) SelectFromHistoryNode(ctx context.Context, filter *nosqlplugin.HistoryNodeFilter) ([]*nosqlplugin.HistoryNodeRow, []byte, error) {
	if filter == nil {
		return nil, nil, fmt.Errorf("kvrocks: SelectFromHistoryNode: nil filter")
	}
	if filter.PageSize <= 0 {
		return nil, nil, nil
	}

	kIdx := db.kHistNodeIndex(filter.ShardID, filter.TreeID, filter.BranchID)

	min := "[" + encodeInt64Lex(filter.MinNodeID) + ":"
	if len(filter.NextPageToken) > 0 {
		min = "(" + string(filter.NextPageToken)
	}
	max := "(" + encodeInt64Lex(filter.MaxNodeID) + ":"

	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: 0,
		Count:  int64(filter.PageSize) + 1,
	}).Result()
	if err != nil {
		return nil, nil, err
	}

	var next []byte
	if len(members) > filter.PageSize {
		next = []byte(members[filter.PageSize-1])
		members = members[:filter.PageSize]
	}

	if len(members) == 0 {
		return nil, next, nil
	}

	keys := make([]string, 0, len(members))
	meta := make([]struct {
		nodeID int64
		txnID  int64
	}, 0, len(members))
	for _, m := range members {
		nodeID, txnID, err := parseHistNodeMember(m)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, db.kHistNodeRow(filter.ShardID, filter.TreeID, filter.BranchID, nodeID, txnID))
		meta = append(meta, struct {
			nodeID int64
			txnID  int64
		}{nodeID: nodeID, txnID: txnID})
	}

	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}

	rows := make([]*nosqlplugin.HistoryNodeRow, 0, len(vals))
	var staleMembers []string
	for i, v := range vals {
		if v == nil {
			staleMembers = append(staleMembers, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectFromHistoryNode: unexpected mget type for %q: %T", keys[i], v)
		}
		enc, data, err := decodeFramedStringAndBytes([]byte(s))
		if err != nil {
			return nil, nil, err
		}
		txn := meta[i].txnID
		rows = append(rows, &nosqlplugin.HistoryNodeRow{
			ShardID:         filter.ShardID,
			TreeID:          filter.TreeID,
			BranchID:        filter.BranchID,
			NodeID:          meta[i].nodeID,
			TxnID:           &txn,
			Data:            data,
			DataEncoding:    enc,
			CreateTimestamp: time.Time{}, // not used by callers
		})
	}
	if len(staleMembers) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, staleMembers).Err()
	}

	return rows, next, nil
}

func (db *DB) DeleteFromHistoryTreeAndNode(ctx context.Context, treeFilter *nosqlplugin.HistoryTreeFilter, nodeFilters []*nosqlplugin.HistoryNodeFilter) error {
	if treeFilter == nil || treeFilter.BranchID == nil {
		return fmt.Errorf("kvrocks: DeleteFromHistoryTreeAndNode: nil tree filter/branchID")
	}

	// Delete nodes first (can be large).
	for _, nf := range nodeFilters {
		if nf == nil {
			continue
		}
		kIdx := db.kHistNodeIndex(nf.ShardID, nf.TreeID, nf.BranchID)
		min := "[" + encodeInt64Lex(nf.MinNodeID) + ":"

		for {
			members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
				Min:    min,
				Max:    "+",
				Offset: 0,
				Count:  1000,
			}).Result()
			if err != nil {
				return err
			}
			if len(members) == 0 {
				break
			}

			pipe := db.rdb.Pipeline()
			for _, m := range members {
				nodeID, txnID, err := parseHistNodeMember(m)
				if err != nil {
					return err
				}
				pipe.Del(ctx, db.kHistNodeRow(nf.ShardID, nf.TreeID, nf.BranchID, nodeID, txnID))
			}
			pipe.ZRem(ctx, kIdx, members)
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
	}

	branchID := *treeFilter.BranchID
	kTree := db.kHistTreeRow(treeFilter.ShardID, treeFilter.TreeID, branchID)
	kBranches := db.kHistTreeBranches(treeFilter.ShardID, treeFilter.TreeID)
	kAll := db.kHistAllTreesIndex()
	member := histAllTreesMember(treeFilter.ShardID, treeFilter.TreeID, branchID)

	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, kTree)
	pipe.SRem(ctx, kBranches, branchID)
	pipe.ZRem(ctx, kAll, member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) SelectAllHistoryTrees(ctx context.Context, nextPageToken []byte, pageSize int) ([]*nosqlplugin.HistoryTreeRow, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kAll := db.kHistAllTreesIndex()
	min := "-"
	if len(nextPageToken) > 0 {
		min = "(" + string(nextPageToken)
	}
	members, err := db.rdb.ZRangeByLex(ctx, kAll, &redis.ZRangeBy{
		Min:    min,
		Max:    "+",
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

	// Fetch rows.
	keys := make([]string, 0, len(members))
	parsed := make([]struct {
		shardID  int
		treeID   string
		branchID string
	}, 0, len(members))
	for _, m := range members {
		sid, tid, bid, err := parseHistAllTreesMember(m)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, db.kHistTreeRow(sid, tid, bid))
		parsed = append(parsed, struct {
			shardID  int
			treeID   string
			branchID string
		}{shardID: sid, treeID: tid, branchID: bid})
	}

	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}

	out := make([]*nosqlplugin.HistoryTreeRow, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectAllHistoryTrees: unexpected mget type for %q: %T", keys[i], v)
		}
		row := &nosqlplugin.HistoryTreeRow{}
		if err := unmarshalJSON([]byte(s), row); err != nil {
			return nil, nil, err
		}
		// Ensure required fields are populated even if older rows are missing them.
		row.ShardID = parsed[i].shardID
		row.TreeID = parsed[i].treeID
		row.BranchID = parsed[i].branchID
		out = append(out, row)
	}

	return out, next, nil
}

func (db *DB) SelectFromHistoryTree(ctx context.Context, filter *nosqlplugin.HistoryTreeFilter) ([]*nosqlplugin.HistoryTreeRow, error) {
	if filter == nil {
		return nil, fmt.Errorf("kvrocks: SelectFromHistoryTree: nil filter")
	}
	kBranches := db.kHistTreeBranches(filter.ShardID, filter.TreeID)
	branchIDs, err := db.rdb.SMembers(ctx, kBranches).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if len(branchIDs) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(branchIDs))
	for _, bid := range branchIDs {
		keys = append(keys, db.kHistTreeRow(filter.ShardID, filter.TreeID, bid))
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	rows := make([]*nosqlplugin.HistoryTreeRow, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("kvrocks: SelectFromHistoryTree: unexpected mget type for %q: %T", keys[i], v)
		}
		row := &nosqlplugin.HistoryTreeRow{}
		if err := unmarshalJSON([]byte(s), row); err != nil {
			return nil, err
		}
		row.ShardID = filter.ShardID
		row.TreeID = filter.TreeID
		// Recompute BeginNodeID for ancestors.
		row.Ancestors = normalizeBranchAncestors(row.Ancestors)
		rows = append(rows, row)
	}

	return rows, nil
}
