package kvrocks

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/nosql/nosqlplugin"

	"github.com/redis/go-redis/v9"
)

// Match Cassandra's "current_workflow" synthetic runID to keep behavior consistent in scanners.
const kvrocksPermanentRunID = "30000000-0000-f000-f000-000000000001"

type workflowExecutionRecord struct {
	State            *persistence.InternalWorkflowMutableState `json:"state"`
	LastWriteVersion int64                                     `json:"last_write_version"`
}

func currentIdxMember(domainID, workflowID string) string {
	return domainID + ":" + workflowID
}

func parseCurrentIdxMember(m string) (domainID, workflowID string, err error) {
	parts := strings.SplitN(m, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("kvrocks: workflow: invalid current index member %q", m)
	}
	return parts[0], parts[1], nil
}

func execIdxMember(domainID, workflowID, runID string) string {
	return domainID + ":" + workflowID + ":" + runID
}

func parseExecIdxMember(m string) (domainID, workflowID, runID string, err error) {
	parts := strings.SplitN(m, ":", 3)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("kvrocks: workflow: invalid exec index member %q", m)
	}
	return parts[0], parts[1], parts[2], nil
}

func timerTaskMember(visibilityUnixNano, taskID int64) string {
	return encodeInt64Lex(visibilityUnixNano) + ":" + encodeInt64Lex(taskID)
}

func parseTimerTaskMember(m string) (visibilityUnixNano, taskID int64, err error) {
	parts := strings.SplitN(m, ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("kvrocks: workflow: invalid timer task member %q", m)
	}
	visibilityUnixNano, err = parseInt64(parts[0])
	if err != nil {
		return 0, 0, err
	}
	taskID, err = parseInt64(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return visibilityUnixNano, taskID, nil
}

func signalRequestedMapFromSlice(ids []string) map[string]struct{} {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

func (db *DB) buildMutableState(req *nosqlplugin.WorkflowExecutionRequest) *persistence.InternalWorkflowMutableState {
	info := req.InternalWorkflowExecutionInfo

	state := &persistence.InternalWorkflowMutableState{
		ExecutionInfo:       &info,
		VersionHistories:    req.VersionHistories,
		ReplicationState:    nil,
		ActivityInfos:       req.ActivityInfos,
		TimerInfos:          req.TimerInfos,
		ChildExecutionInfos: req.ChildWorkflowInfos,
		RequestCancelInfos:  req.RequestCancelInfos,
		SignalInfos:         req.SignalInfos,
		SignalRequestedIDs:  signalRequestedMapFromSlice(req.SignalRequestedIDs),
		BufferedEvents:      nil,
	}
	if req.Checksums != nil {
		state.Checksum = *req.Checksums
	}
	sanitizeMutableState(state)
	return state
}

func (db *DB) applyExecutionUpdate(rec *workflowExecutionRecord, req *nosqlplugin.WorkflowExecutionRequest) error {
	if rec.State == nil {
		rec.State = &persistence.InternalWorkflowMutableState{}
	}

	info := req.InternalWorkflowExecutionInfo
	rec.State.ExecutionInfo = &info
	rec.State.VersionHistories = req.VersionHistories
	if req.Checksums != nil {
		rec.State.Checksum = *req.Checksums
	}
	rec.LastWriteVersion = req.LastWriteVersion

	switch req.MapsWriteMode {
	case nosqlplugin.WorkflowExecutionMapsWriteModeCreate, nosqlplugin.WorkflowExecutionMapsWriteModeReset:
		rec.State.ActivityInfos = req.ActivityInfos
		rec.State.TimerInfos = req.TimerInfos
		rec.State.ChildExecutionInfos = req.ChildWorkflowInfos
		rec.State.RequestCancelInfos = req.RequestCancelInfos
		rec.State.SignalInfos = req.SignalInfos
		rec.State.SignalRequestedIDs = signalRequestedMapFromSlice(req.SignalRequestedIDs)
	case nosqlplugin.WorkflowExecutionMapsWriteModeUpdate:
		if rec.State.ActivityInfos == nil {
			rec.State.ActivityInfos = make(map[int64]*persistence.InternalActivityInfo)
		}
		for k, v := range req.ActivityInfos {
			rec.State.ActivityInfos[k] = v
		}
		for _, k := range req.ActivityInfoKeysToDelete {
			delete(rec.State.ActivityInfos, k)
		}

		if rec.State.TimerInfos == nil {
			rec.State.TimerInfos = make(map[string]*persistence.TimerInfo)
		}
		for k, v := range req.TimerInfos {
			rec.State.TimerInfos[k] = v
		}
		for _, k := range req.TimerInfoKeysToDelete {
			delete(rec.State.TimerInfos, k)
		}

		if rec.State.ChildExecutionInfos == nil {
			rec.State.ChildExecutionInfos = make(map[int64]*persistence.InternalChildExecutionInfo)
		}
		for k, v := range req.ChildWorkflowInfos {
			rec.State.ChildExecutionInfos[k] = v
		}
		for _, k := range req.ChildWorkflowInfoKeysToDelete {
			delete(rec.State.ChildExecutionInfos, k)
		}

		if rec.State.RequestCancelInfos == nil {
			rec.State.RequestCancelInfos = make(map[int64]*persistence.RequestCancelInfo)
		}
		for k, v := range req.RequestCancelInfos {
			rec.State.RequestCancelInfos[k] = v
		}
		for _, k := range req.RequestCancelInfoKeysToDelete {
			delete(rec.State.RequestCancelInfos, k)
		}

		if rec.State.SignalInfos == nil {
			rec.State.SignalInfos = make(map[int64]*persistence.SignalInfo)
		}
		for k, v := range req.SignalInfos {
			rec.State.SignalInfos[k] = v
		}
		for _, k := range req.SignalInfoKeysToDelete {
			delete(rec.State.SignalInfos, k)
		}

		if rec.State.SignalRequestedIDs == nil {
			rec.State.SignalRequestedIDs = make(map[string]struct{})
		}
		for _, id := range req.SignalRequestedIDs {
			if id == "" {
				continue
			}
			rec.State.SignalRequestedIDs[id] = struct{}{}
		}
		for _, id := range req.SignalRequestedIDsKeysToDelete {
			delete(rec.State.SignalRequestedIDs, id)
		}
	default:
		return fmt.Errorf("kvrocks: unknown maps write mode %v", req.MapsWriteMode)
	}

	switch req.EventBufferWriteMode {
	case nosqlplugin.EventBufferWriteModeNone:
		// noop
	case nosqlplugin.EventBufferWriteModeClear:
		rec.State.BufferedEvents = nil
	case nosqlplugin.EventBufferWriteModeAppend:
		if req.NewBufferedEventBatch == nil {
			return fmt.Errorf("kvrocks: append buffered event: nil batch")
		}
		rec.State.BufferedEvents = append(rec.State.BufferedEvents, req.NewBufferedEventBatch)
	default:
		return fmt.Errorf("kvrocks: unknown event buffer write mode %v", req.EventBufferWriteMode)
	}

	sanitizeMutableState(rec.State)
	return nil
}

func (db *DB) SelectCurrentWorkflow(ctx context.Context, shardID int, domainID, workflowID string) (*nosqlplugin.CurrentWorkflowRow, error) {
	b, err := db.rdb.Get(ctx, db.kWfCurrent(shardID, domainID, workflowID)).Bytes()
	if err != nil {
		return nil, err
	}
	out := &nosqlplugin.CurrentWorkflowRow{}
	if err := unmarshalJSON(b, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *DB) SelectAllCurrentWorkflows(ctx context.Context, shardID int, pageToken []byte, pageSize int) ([]*persistence.CurrentWorkflowExecution, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kIdx := db.kWfCurrentIndex(shardID)

	min := "-"
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
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
	if len(members) == 0 {
		return nil, next, nil
	}

	keys := make([]string, 0, len(members))
	parsed := make([]struct {
		domainID   string
		workflowID string
	}, 0, len(members))
	for _, m := range members {
		did, wid, err := parseCurrentIdxMember(m)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, db.kWfCurrent(shardID, did, wid))
		parsed = append(parsed, struct {
			domainID   string
			workflowID string
		}{domainID: did, workflowID: wid})
	}

	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}

	out := make([]*persistence.CurrentWorkflowExecution, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectAllCurrentWorkflows: unexpected mget type for %q: %T", keys[i], v)
		}
		row := &nosqlplugin.CurrentWorkflowRow{}
		if err := unmarshalJSON([]byte(s), row); err != nil {
			return nil, nil, err
		}
		out = append(out, &persistence.CurrentWorkflowExecution{
			DomainID:     parsed[i].domainID,
			WorkflowID:   parsed[i].workflowID,
			RunID:        kvrocksPermanentRunID,
			State:        row.State,
			CurrentRunID: row.RunID,
		})
	}

	return out, next, nil
}

func (db *DB) DeleteCurrentWorkflow(ctx context.Context, shardID int, domainID, workflowID, currentRunIDCondition string) error {
	kCur := db.kWfCurrent(shardID, domainID, workflowID)
	b, err := db.rdb.Get(ctx, kCur).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	row := &nosqlplugin.CurrentWorkflowRow{}
	if err := unmarshalJSON(b, row); err != nil {
		return err
	}
	if row.RunID != currentRunIDCondition {
		return nil
	}
	member := currentIdxMember(domainID, workflowID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, kCur)
	pipe.ZRem(ctx, db.kWfCurrentIndex(shardID), member)
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectWorkflowExecution(ctx context.Context, shardID int, domainID, workflowID, runID string) (*nosqlplugin.WorkflowExecution, error) {
	b, err := db.rdb.Get(ctx, db.kWfExecution(shardID, domainID, workflowID, runID)).Bytes()
	if err != nil {
		return nil, err
	}
	rec := &workflowExecutionRecord{}
	if err := unmarshalJSON(b, rec); err != nil {
		return nil, err
	}
	if rec.State == nil {
		return nil, fmt.Errorf("kvrocks: SelectWorkflowExecution: missing state")
	}
	sanitizeMutableState(rec.State)
	return rec.State, nil
}

func (db *DB) SelectAllWorkflowExecutions(ctx context.Context, shardID int, pageToken []byte, pageSize int) ([]*persistence.InternalListConcreteExecutionsEntity, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kIdx := db.kWfExecutionIndex(shardID)
	min := "-"
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
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
	if len(members) == 0 {
		return nil, next, nil
	}

	keys := make([]string, 0, len(members))
	for _, m := range members {
		did, wid, rid, err := parseExecIdxMember(m)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, db.kWfExecution(shardID, did, wid, rid))
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}
	out := make([]*persistence.InternalListConcreteExecutionsEntity, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectAllWorkflowExecutions: unexpected mget type for %q: %T", keys[i], v)
		}
		rec := &workflowExecutionRecord{}
		if err := unmarshalJSON([]byte(s), rec); err != nil {
			return nil, nil, err
		}
		if rec.State == nil || rec.State.ExecutionInfo == nil {
			continue
		}
		sanitizeMutableState(rec.State)
		out = append(out, &persistence.InternalListConcreteExecutionsEntity{
			ExecutionInfo:    rec.State.ExecutionInfo,
			VersionHistories: rec.State.VersionHistories,
		})
	}
	return out, next, nil
}

func (db *DB) IsWorkflowExecutionExists(ctx context.Context, shardID int, domainID, workflowID, runID string) (bool, error) {
	n, err := db.rdb.Exists(ctx, db.kWfExecution(shardID, domainID, workflowID, runID)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (db *DB) DeleteWorkflowExecution(ctx context.Context, shardID int, domainID, workflowID, runID string) error {
	member := execIdxMember(domainID, workflowID, runID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kWfExecution(shardID, domainID, workflowID, runID))
	pipe.Del(ctx, db.kWfExecutionNextEventID(shardID, domainID, workflowID, runID))
	pipe.Del(ctx, db.kWfActiveClusterSelectionPolicy(shardID, domainID, workflowID, runID))
	pipe.ZRem(ctx, db.kWfExecutionIndex(shardID), member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) writeHistoryTasks(ctx context.Context, pipe redis.Pipeliner, shardID int, tasksByCategory map[persistence.HistoryTaskCategory][]*nosqlplugin.HistoryMigrationTask) error {
	for c, tasks := range tasksByCategory {
		switch c.ID() {
		case persistence.HistoryTaskCategoryIDTransfer:
			for _, t := range tasks {
				if t == nil || t.Transfer == nil {
					continue
				}
				taskID := t.Transfer.TaskID
				copyTask := *t
				copyTask.TaskID = taskID
				b, err := marshalJSON(&copyTask)
				if err != nil {
					return err
				}
				pipe.Set(ctx, db.kTransferTask(shardID, taskID), b, 0)
				pipe.ZAdd(ctx, db.kTransferTaskIndex(shardID), redis.Z{Score: 0, Member: encodeInt64Lex(taskID)})
			}
		case persistence.HistoryTaskCategoryIDTimer:
			for _, t := range tasks {
				if t == nil || t.Timer == nil {
					continue
				}
				taskID := t.Timer.TaskID
				visUnixNano := t.Timer.VisibilityTimestamp.UnixNano()
				copyTask := *t
				copyTask.TaskID = taskID
				copyTask.ScheduledTime = t.Timer.VisibilityTimestamp
				b, err := marshalJSON(&copyTask)
				if err != nil {
					return err
				}
				pipe.Set(ctx, db.kTimerTask(shardID, visUnixNano, taskID), b, 0)
				pipe.ZAdd(ctx, db.kTimerTaskIndex(shardID), redis.Z{Score: 0, Member: timerTaskMember(visUnixNano, taskID)})
			}
		case persistence.HistoryTaskCategoryIDReplication:
			for _, t := range tasks {
				if t == nil || t.Replication == nil {
					continue
				}
				taskID := t.Replication.TaskID
				copyTask := *t
				copyTask.TaskID = taskID
				b, err := marshalJSON(&copyTask)
				if err != nil {
					return err
				}
				pipe.Set(ctx, db.kReplicationTask(shardID, taskID), b, 0)
				pipe.ZAdd(ctx, db.kReplicationTaskIndex(shardID), redis.Z{Score: 0, Member: encodeInt64Lex(taskID)})
			}
		}
	}
	return nil
}

func (db *DB) InsertWorkflowExecutionWithTasks(
	ctx context.Context,
	requests *nosqlplugin.WorkflowRequestsWriteRequest,
	currentWorkflowRequest *nosqlplugin.CurrentWorkflowWriteRequest,
	execution *nosqlplugin.WorkflowExecutionRequest,
	tasksByCategory map[persistence.HistoryTaskCategory][]*nosqlplugin.HistoryMigrationTask,
	activeClusterSelectionPolicyRow *nosqlplugin.ActiveClusterSelectionPolicyRow,
	shardCondition *nosqlplugin.ShardCondition,
) error {
	if execution == nil || shardCondition == nil {
		return fmt.Errorf("kvrocks: InsertWorkflowExecutionWithTasks: nil execution/shardCondition")
	}
	shardID := shardCondition.ShardID
	domainID := execution.DomainID
	workflowID := execution.WorkflowID
	runID := execution.RunID

	kShardRange := db.kShardRange(shardID)
	kCur := db.kWfCurrent(shardID, domainID, workflowID)
	kExec := db.kWfExecution(shardID, domainID, workflowID, runID)
	kExecNext := db.kWfExecutionNextEventID(shardID, domainID, workflowID, runID)

	keysToWatch := []string{kShardRange, kExec}
	if currentWorkflowRequest != nil && currentWorkflowRequest.WriteMode != nosqlplugin.CurrentWorkflowWriteModeNoop {
		keysToWatch = append(keysToWatch, kCur)
	}
	if requests != nil && requests.WriteMode == nosqlplugin.WorkflowRequestWriteModeInsert {
		for _, r := range requests.Rows {
			if r == nil {
				continue
			}
			keysToWatch = append(keysToWatch, db.kWfRequest(shardID, r.DomainID, r.WorkflowID, r.RequestType, r.RequestID))
		}
	}

	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		// Shard ownership check.
		actualRangeStr, err := tx.Get(ctx, kShardRange).Result()
		if err != nil {
			return err
		}
		actualRange, err := strconv.ParseInt(actualRangeStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kvrocks: InsertWorkflowExecutionWithTasks: parse shard range: %w", err)
		}
		if actualRange != shardCondition.RangeID {
			v := actualRange
			return &nosqlplugin.WorkflowOperationConditionFailure{ShardRangeIDNotMatch: &v}
		}

		// Workflow request dedupe.
		if requests != nil && requests.WriteMode == nosqlplugin.WorkflowRequestWriteModeInsert {
			for _, r := range requests.Rows {
				if r == nil {
					continue
				}
				kReq := db.kWfRequest(shardID, r.DomainID, r.WorkflowID, r.RequestType, r.RequestID)
				if n, err := tx.Exists(ctx, kReq).Result(); err != nil {
					return err
				} else if n > 0 {
					b, err := tx.Get(ctx, kReq).Bytes()
					if err != nil {
						return err
					}
					existing := &nosqlplugin.WorkflowRequestRow{}
					if err := unmarshalJSON(b, existing); err != nil {
						return err
					}
					return &nosqlplugin.WorkflowOperationConditionFailure{
						DuplicateRequest: &nosqlplugin.DuplicateRequest{
							RequestType: r.RequestType,
							RunID:       existing.RunID,
						},
					}
				}
			}
		}

		// Current workflow conditional write.
		if currentWorkflowRequest == nil {
			currentWorkflowRequest = &nosqlplugin.CurrentWorkflowWriteRequest{WriteMode: nosqlplugin.CurrentWorkflowWriteModeNoop}
		}

		switch currentWorkflowRequest.WriteMode {
		case nosqlplugin.CurrentWorkflowWriteModeNoop:
			// noop
		case nosqlplugin.CurrentWorkflowWriteModeInsert:
			if n, err := tx.Exists(ctx, kCur).Result(); err != nil {
				return err
			} else if n > 0 {
				b, err := tx.Get(ctx, kCur).Bytes()
				if err != nil {
					return err
				}
				existing := &nosqlplugin.CurrentWorkflowRow{}
				if err := unmarshalJSON(b, existing); err != nil {
					return err
				}
				msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v", workflowID, existing.RunID)
				return &nosqlplugin.WorkflowOperationConditionFailure{
					WorkflowExecutionAlreadyExists: &nosqlplugin.WorkflowExecutionAlreadyExists{
						OtherInfo:        msg,
						CreateRequestID:  existing.CreateRequestID,
						RunID:            existing.RunID,
						State:            existing.State,
						CloseStatus:      existing.CloseStatus,
						LastWriteVersion: existing.LastWriteVersion,
					},
				}
			}
		case nosqlplugin.CurrentWorkflowWriteModeUpdate:
			if currentWorkflowRequest.Condition == nil || currentWorkflowRequest.Condition.GetCurrentRunID() == "" {
				return fmt.Errorf("kvrocks: CurrentWorkflowWriteModeUpdate requires Condition.CurrentRunID")
			}
			b, err := tx.Get(ctx, kCur).Bytes()
			if err != nil {
				return err
			}
			existing := &nosqlplugin.CurrentWorkflowRow{}
			if err := unmarshalJSON(b, existing); err != nil {
				return err
			}
			if want := currentWorkflowRequest.Condition.GetCurrentRunID(); want != "" && existing.RunID != want {
				msg := fmt.Sprintf("Workflow execution creation condition failed by mismatch runID. WorkflowId: %v, Expected Current RunID: %v, Actual Current RunID: %v",
					workflowID, want, existing.RunID)
				return &nosqlplugin.WorkflowOperationConditionFailure{CurrentWorkflowConditionFailInfo: &msg}
			}
			// Match Cassandra semantics: only apply additional condition checks if both are provided.
			if currentWorkflowRequest.Condition.LastWriteVersion != nil && currentWorkflowRequest.Condition.State != nil {
				if existing.LastWriteVersion != *currentWorkflowRequest.Condition.LastWriteVersion {
					msg := fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, Expected Version: %v, Actual Version: %v",
						workflowID, *currentWorkflowRequest.Condition.LastWriteVersion, existing.LastWriteVersion)
					return &nosqlplugin.WorkflowOperationConditionFailure{CurrentWorkflowConditionFailInfo: &msg}
				}
				if existing.State != *currentWorkflowRequest.Condition.State {
					msg := fmt.Sprintf("Workflow execution creation condition failed. WorkflowId: %v, Expected State: %v, Actual State: %v",
						workflowID, *currentWorkflowRequest.Condition.State, existing.State)
					return &nosqlplugin.WorkflowOperationConditionFailure{CurrentWorkflowConditionFailInfo: &msg}
				}
			}
		default:
			return fmt.Errorf("kvrocks: unknown current workflow write mode %v", currentWorkflowRequest.WriteMode)
		}

		// Concrete execution must not exist.
		if n, err := tx.Exists(ctx, kExec).Result(); err != nil {
			return err
		} else if n > 0 {
			msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v", workflowID, runID)
			return &nosqlplugin.WorkflowOperationConditionFailure{
				WorkflowExecutionAlreadyExists: &nosqlplugin.WorkflowExecutionAlreadyExists{
					OtherInfo:        msg,
					CreateRequestID:  execution.CreateRequestID,
					RunID:            runID,
					State:            execution.State,
					CloseStatus:      execution.CloseStatus,
					LastWriteVersion: execution.LastWriteVersion,
				},
			}
		}

		state := db.buildMutableState(execution)
		rec := &workflowExecutionRecord{State: state, LastWriteVersion: execution.LastWriteVersion}
		recBytes, err := marshalJSON(rec)
		if err != nil {
			return err
		}

		var curBytes []byte
		if currentWorkflowRequest.WriteMode != nosqlplugin.CurrentWorkflowWriteModeNoop {
			curBytes, err = marshalJSON(&currentWorkflowRequest.Row)
			if err != nil {
				return err
			}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			// Workflow requests.
			if requests != nil {
				for _, r := range requests.Rows {
					if r == nil {
						continue
					}
					b, err := marshalJSON(r)
					if err != nil {
						return err
					}
					pipe.Set(ctx, db.kWfRequest(shardID, r.DomainID, r.WorkflowID, r.RequestType, r.RequestID), b, 0)
				}
			}

			// Current workflow.
			if currentWorkflowRequest.WriteMode == nosqlplugin.CurrentWorkflowWriteModeInsert ||
				currentWorkflowRequest.WriteMode == nosqlplugin.CurrentWorkflowWriteModeUpdate {
				pipe.Set(ctx, kCur, curBytes, 0)
				pipe.ZAdd(ctx, db.kWfCurrentIndex(shardID), redis.Z{Score: 0, Member: currentIdxMember(domainID, workflowID)})
			}

			// Execution.
			pipe.Set(ctx, kExec, recBytes, 0)
			pipe.Set(ctx, kExecNext, fmt.Sprintf("%d", execution.NextEventID), 0)
			pipe.ZAdd(ctx, db.kWfExecutionIndex(shardID), redis.Z{Score: 0, Member: execIdxMember(domainID, workflowID, runID)})

			// Active cluster selection policy.
			if activeClusterSelectionPolicyRow != nil && activeClusterSelectionPolicyRow.Policy != nil {
				b, err := marshalJSON(activeClusterSelectionPolicyRow.Policy)
				if err != nil {
					return err
				}
				pipe.Set(ctx, db.kWfActiveClusterSelectionPolicy(shardID, domainID, workflowID, runID), b, 0)
			}

			// Internal tasks.
			return db.writeHistoryTasks(ctx, pipe, shardID, tasksByCategory)
		})
		return err
	})
}

func (db *DB) UpdateWorkflowExecutionWithTasks(
	ctx context.Context,
	requests *nosqlplugin.WorkflowRequestsWriteRequest,
	currentWorkflowRequest *nosqlplugin.CurrentWorkflowWriteRequest,
	mutatedExecution *nosqlplugin.WorkflowExecutionRequest,
	insertedExecution *nosqlplugin.WorkflowExecutionRequest,
	activeClusterSelectionPolicyRow *nosqlplugin.ActiveClusterSelectionPolicyRow,
	resetExecution *nosqlplugin.WorkflowExecutionRequest,
	tasksByCategory map[persistence.HistoryTaskCategory][]*nosqlplugin.HistoryMigrationTask,
	shardCondition *nosqlplugin.ShardCondition,
) error {
	if shardCondition == nil {
		return fmt.Errorf("kvrocks: UpdateWorkflowExecutionWithTasks: nil shardCondition")
	}
	shardID := shardCondition.ShardID

	// Match Cassandra behavior: only one execution update is conditional on NextEventID.
	var primaryExec *nosqlplugin.WorkflowExecutionRequest
	if mutatedExecution != nil {
		primaryExec = mutatedExecution
	} else if resetExecution != nil {
		primaryExec = resetExecution
	} else {
		return fmt.Errorf("kvrocks: UpdateWorkflowExecutionWithTasks: missing mutated/reset execution")
	}
	domainID := primaryExec.DomainID
	workflowID := primaryExec.WorkflowID

	kShardRange := db.kShardRange(shardID)

	keysToWatch := []string{kShardRange}

	// Current workflow row might be updated.
	kCur := db.kWfCurrent(shardID, domainID, workflowID)
	if currentWorkflowRequest != nil && currentWorkflowRequest.WriteMode != nosqlplugin.CurrentWorkflowWriteModeNoop {
		keysToWatch = append(keysToWatch, kCur)
	}

	// Primary execution keys.
	kPrimaryExec := db.kWfExecution(shardID, domainID, workflowID, primaryExec.RunID)
	kPrimaryNext := db.kWfExecutionNextEventID(shardID, domainID, workflowID, primaryExec.RunID)
	keysToWatch = append(keysToWatch, kPrimaryExec, kPrimaryNext)

	// Inserted execution keys (if any) must not exist.
	if insertedExecution != nil {
		keysToWatch = append(keysToWatch,
			db.kWfExecution(shardID, insertedExecution.DomainID, insertedExecution.WorkflowID, insertedExecution.RunID),
		)
	}

	// Reset execution might also be updated (conflict resolve).
	if resetExecution != nil && resetExecution.RunID != primaryExec.RunID {
		keysToWatch = append(
			keysToWatch,
			db.kWfExecution(shardID, resetExecution.DomainID, resetExecution.WorkflowID, resetExecution.RunID),
			db.kWfExecutionNextEventID(shardID, resetExecution.DomainID, resetExecution.WorkflowID, resetExecution.RunID),
		)
	}

	// Workflow requests for insert mode.
	if requests != nil && requests.WriteMode == nosqlplugin.WorkflowRequestWriteModeInsert {
		for _, r := range requests.Rows {
			if r == nil {
				continue
			}
			keysToWatch = append(keysToWatch, db.kWfRequest(shardID, r.DomainID, r.WorkflowID, r.RequestType, r.RequestID))
		}
	}

	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		// Shard ownership check.
		actualRangeStr, err := tx.Get(ctx, kShardRange).Result()
		if err != nil {
			return err
		}
		actualRange, err := strconv.ParseInt(actualRangeStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kvrocks: UpdateWorkflowExecutionWithTasks: parse shard range: %w", err)
		}
		if actualRange != shardCondition.RangeID {
			v := actualRange
			return &nosqlplugin.WorkflowOperationConditionFailure{ShardRangeIDNotMatch: &v}
		}

		// Workflow request dedupe.
		if requests != nil && requests.WriteMode == nosqlplugin.WorkflowRequestWriteModeInsert {
			for _, r := range requests.Rows {
				if r == nil {
					continue
				}
				kReq := db.kWfRequest(shardID, r.DomainID, r.WorkflowID, r.RequestType, r.RequestID)
				if n, err := tx.Exists(ctx, kReq).Result(); err != nil {
					return err
				} else if n > 0 {
					b, err := tx.Get(ctx, kReq).Bytes()
					if err != nil {
						return err
					}
					existing := &nosqlplugin.WorkflowRequestRow{}
					if err := unmarshalJSON(b, existing); err != nil {
						return err
					}
					return &nosqlplugin.WorkflowOperationConditionFailure{
						DuplicateRequest: &nosqlplugin.DuplicateRequest{
							RequestType: r.RequestType,
							RunID:       existing.RunID,
						},
					}
				}
			}
		}

		// Current workflow conditional update.
		if currentWorkflowRequest == nil {
			currentWorkflowRequest = &nosqlplugin.CurrentWorkflowWriteRequest{WriteMode: nosqlplugin.CurrentWorkflowWriteModeNoop}
		}
		if currentWorkflowRequest.WriteMode == nosqlplugin.CurrentWorkflowWriteModeUpdate {
			if currentWorkflowRequest.Condition == nil || currentWorkflowRequest.Condition.GetCurrentRunID() == "" {
				return fmt.Errorf("kvrocks: CurrentWorkflowWriteModeUpdate requires Condition.CurrentRunID")
			}
			b, err := tx.Get(ctx, kCur).Bytes()
			if err != nil {
				return err
			}
			existing := &nosqlplugin.CurrentWorkflowRow{}
			if err := unmarshalJSON(b, existing); err != nil {
				return err
			}
			if want := currentWorkflowRequest.Condition.GetCurrentRunID(); want != "" && existing.RunID != want {
				msg := fmt.Sprintf("Failed to update mutable state. requestConditionalRunID: %v, Actual Value: %v",
					want, existing.RunID)
				return &nosqlplugin.WorkflowOperationConditionFailure{CurrentWorkflowConditionFailInfo: &msg}
			}
			// Match Cassandra semantics: only apply additional condition checks if both are provided.
			if currentWorkflowRequest.Condition.LastWriteVersion != nil && currentWorkflowRequest.Condition.State != nil {
				if existing.LastWriteVersion != *currentWorkflowRequest.Condition.LastWriteVersion {
					msg := fmt.Sprintf("Failed to update mutable state. requestConditionalVersion: %v, Actual Version: %v",
						*currentWorkflowRequest.Condition.LastWriteVersion, existing.LastWriteVersion)
					return &nosqlplugin.WorkflowOperationConditionFailure{CurrentWorkflowConditionFailInfo: &msg}
				}
				if existing.State != *currentWorkflowRequest.Condition.State {
					msg := fmt.Sprintf("Failed to update mutable state. requestConditionalState: %v, Actual State: %v",
						*currentWorkflowRequest.Condition.State, existing.State)
					return &nosqlplugin.WorkflowOperationConditionFailure{CurrentWorkflowConditionFailInfo: &msg}
				}
			}
		}

		// NextEventID CAS for primary execution.
		if primaryExec.PreviousNextEventIDCondition == nil {
			return fmt.Errorf("kvrocks: UpdateWorkflowExecutionWithTasks: missing PreviousNextEventIDCondition")
		}
		curNextStr, err := tx.Get(ctx, kPrimaryNext).Result()
		if err != nil {
			return err
		}
		curNext, err := strconv.ParseInt(curNextStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kvrocks: UpdateWorkflowExecutionWithTasks: parse nextEventID: %w", err)
		}
		if curNext != *primaryExec.PreviousNextEventIDCondition {
			msg := fmt.Sprintf("Failed to update mutable state. previousNextEventIDCondition: %v, actualNextEventID: %v, Request Current RunID: %v",
				*primaryExec.PreviousNextEventIDCondition, curNext, primaryExec.RunID)
			return &nosqlplugin.WorkflowOperationConditionFailure{UnknownConditionFailureDetails: &msg}
		}

		// Load primary execution record.
		primaryBytes, err := tx.Get(ctx, kPrimaryExec).Bytes()
		if err != nil {
			return err
		}
		primaryRec := &workflowExecutionRecord{}
		if err := unmarshalJSON(primaryBytes, primaryRec); err != nil {
			return err
		}
		if err := db.applyExecutionUpdate(primaryRec, primaryExec); err != nil {
			return err
		}
		primaryRecBytes, err := marshalJSON(primaryRec)
		if err != nil {
			return err
		}

		// Prepare inserted execution record, if any.
		var insertedRecBytes []byte
		if insertedExecution != nil {
			kIns := db.kWfExecution(shardID, insertedExecution.DomainID, insertedExecution.WorkflowID, insertedExecution.RunID)
			if n, err := tx.Exists(ctx, kIns).Result(); err != nil {
				return err
			} else if n > 0 {
				msg := fmt.Sprintf("Concrete execution already exists. WorkflowId: %v, RunId: %v", insertedExecution.WorkflowID, insertedExecution.RunID)
				return &nosqlplugin.WorkflowOperationConditionFailure{UnknownConditionFailureDetails: &msg}
			}
			state := db.buildMutableState(insertedExecution)
			rec := &workflowExecutionRecord{State: state, LastWriteVersion: insertedExecution.LastWriteVersion}
			insertedRecBytes, err = marshalJSON(rec)
			if err != nil {
				return err
			}
		}

		// Prepare reset execution record, if any (and not already treated as primary).
		var (
			resetRecBytes []byte
			kResetExec    string
			kResetNext    string
		)
		if resetExecution != nil && resetExecution.RunID != primaryExec.RunID {
			kResetExec = db.kWfExecution(shardID, resetExecution.DomainID, resetExecution.WorkflowID, resetExecution.RunID)
			kResetNext = db.kWfExecutionNextEventID(shardID, resetExecution.DomainID, resetExecution.WorkflowID, resetExecution.RunID)

			resetBytes, err := tx.Get(ctx, kResetExec).Bytes()
			if err != nil {
				return err
			}
			resetRec := &workflowExecutionRecord{}
			if err := unmarshalJSON(resetBytes, resetRec); err != nil {
				return err
			}
			if err := db.applyExecutionUpdate(resetRec, resetExecution); err != nil {
				return err
			}
			resetRecBytes, err = marshalJSON(resetRec)
			if err != nil {
				return err
			}
		}

		var curBytes []byte
		if currentWorkflowRequest.WriteMode != nosqlplugin.CurrentWorkflowWriteModeNoop {
			curBytes, err = marshalJSON(&currentWorkflowRequest.Row)
			if err != nil {
				return err
			}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			// Workflow requests.
			if requests != nil {
				for _, r := range requests.Rows {
					if r == nil {
						continue
					}
					b, err := marshalJSON(r)
					if err != nil {
						return err
					}
					pipe.Set(ctx, db.kWfRequest(shardID, r.DomainID, r.WorkflowID, r.RequestType, r.RequestID), b, 0)
				}
			}

			// Current workflow.
			if currentWorkflowRequest.WriteMode == nosqlplugin.CurrentWorkflowWriteModeUpdate {
				pipe.Set(ctx, kCur, curBytes, 0)
				pipe.ZAdd(ctx, db.kWfCurrentIndex(shardID), redis.Z{Score: 0, Member: currentIdxMember(domainID, workflowID)})
			}

			// Primary execution update.
			pipe.Set(ctx, kPrimaryExec, primaryRecBytes, 0)
			pipe.Set(ctx, kPrimaryNext, fmt.Sprintf("%d", primaryExec.NextEventID), 0)
			pipe.ZAdd(ctx, db.kWfExecutionIndex(shardID), redis.Z{Score: 0, Member: execIdxMember(domainID, workflowID, primaryExec.RunID)})

			// Inserted execution, if any.
			if insertedExecution != nil {
				kIns := db.kWfExecution(shardID, insertedExecution.DomainID, insertedExecution.WorkflowID, insertedExecution.RunID)
				kInsNext := db.kWfExecutionNextEventID(shardID, insertedExecution.DomainID, insertedExecution.WorkflowID, insertedExecution.RunID)
				pipe.Set(ctx, kIns, insertedRecBytes, 0)
				pipe.Set(ctx, kInsNext, fmt.Sprintf("%d", insertedExecution.NextEventID), 0)
				pipe.ZAdd(ctx, db.kWfExecutionIndex(shardID), redis.Z{Score: 0, Member: execIdxMember(insertedExecution.DomainID, insertedExecution.WorkflowID, insertedExecution.RunID)})
				if activeClusterSelectionPolicyRow != nil && activeClusterSelectionPolicyRow.Policy != nil {
					b, err := marshalJSON(activeClusterSelectionPolicyRow.Policy)
					if err != nil {
						return err
					}
					pipe.Set(ctx, db.kWfActiveClusterSelectionPolicy(shardID, insertedExecution.DomainID, insertedExecution.WorkflowID, insertedExecution.RunID), b, 0)
				}
			}

			// Reset execution (conflict resolve): update without NextEventID CAS when it's not the primary execution.
			if resetExecution != nil && resetExecution.RunID != primaryExec.RunID {
				pipe.Set(ctx, kResetExec, resetRecBytes, 0)
				pipe.Set(ctx, kResetNext, fmt.Sprintf("%d", resetExecution.NextEventID), 0)
				pipe.ZAdd(ctx, db.kWfExecutionIndex(shardID), redis.Z{Score: 0, Member: execIdxMember(resetExecution.DomainID, resetExecution.WorkflowID, resetExecution.RunID)})
			}

			// Internal tasks.
			return db.writeHistoryTasks(ctx, pipe, shardID, tasksByCategory)
		})
		return err
	})
}

func (db *DB) SelectTransferTasksOrderByTaskID(ctx context.Context, shardID, pageSize int, pageToken []byte, inclusiveMinTaskID, exclusiveMaxTaskID int64) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kIdx := db.kTransferTaskIndex(shardID)
	min := "[" + encodeInt64Lex(inclusiveMinTaskID)
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	max := "(" + encodeInt64Lex(exclusiveMaxTaskID)
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
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
	if len(members) == 0 {
		return nil, next, nil
	}
	keys := make([]string, 0, len(members))
	taskIDs := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := parseInt64(m)
		if err != nil {
			return nil, nil, err
		}
		taskIDs = append(taskIDs, id)
		keys = append(keys, db.kTransferTask(shardID, id))
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}
	out := make([]*nosqlplugin.HistoryMigrationTask, 0, len(vals))
	var stale []string
	for i, v := range vals {
		if v == nil {
			stale = append(stale, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectTransferTasksOrderByTaskID: unexpected mget type for %q: %T", keys[i], v)
		}
		t := &nosqlplugin.HistoryMigrationTask{}
		if err := unmarshalJSON([]byte(s), t); err != nil {
			return nil, nil, err
		}
		t.TaskID = taskIDs[i]
		out = append(out, t)
	}
	if len(stale) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, stale).Err()
	}
	return out, next, nil
}

func (db *DB) DeleteTransferTask(ctx context.Context, shardID int, taskID int64) error {
	member := encodeInt64Lex(taskID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kTransferTask(shardID, taskID))
	pipe.ZRem(ctx, db.kTransferTaskIndex(shardID), member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) RangeDeleteTransferTasks(ctx context.Context, shardID int, inclusiveBeginTaskID, exclusiveEndTaskID int64) error {
	kIdx := db.kTransferTaskIndex(shardID)
	min := "[" + encodeInt64Lex(inclusiveBeginTaskID)
	max := "(" + encodeInt64Lex(exclusiveEndTaskID)
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{Min: min, Max: max}).Result()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	pipe := db.rdb.Pipeline()
	for _, m := range members {
		id, err := parseInt64(m)
		if err != nil {
			return err
		}
		pipe.Del(ctx, db.kTransferTask(shardID, id))
	}
	pipe.ZRem(ctx, kIdx, members)
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectTimerTasksOrderByVisibilityTime(ctx context.Context, shardID, pageSize int, pageToken []byte, inclusiveMinTime, exclusiveMaxTime time.Time) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kIdx := db.kTimerTaskIndex(shardID)
	min := "[" + encodeInt64Lex(inclusiveMinTime.UnixNano()) + ":"
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	max := "(" + encodeInt64Lex(exclusiveMaxTime.UnixNano()) + ":"
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
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
	if len(members) == 0 {
		return nil, next, nil
	}
	keys := make([]string, 0, len(members))
	meta := make([]struct {
		visUnixNano int64
		taskID      int64
	}, 0, len(members))
	for _, m := range members {
		vis, id, err := parseTimerTaskMember(m)
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, db.kTimerTask(shardID, vis, id))
		meta = append(meta, struct {
			visUnixNano int64
			taskID      int64
		}{visUnixNano: vis, taskID: id})
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}
	out := make([]*nosqlplugin.HistoryMigrationTask, 0, len(vals))
	var stale []string
	for i, v := range vals {
		if v == nil {
			stale = append(stale, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectTimerTasksOrderByVisibilityTime: unexpected mget type for %q: %T", keys[i], v)
		}
		t := &nosqlplugin.HistoryMigrationTask{}
		if err := unmarshalJSON([]byte(s), t); err != nil {
			return nil, nil, err
		}
		t.TaskID = meta[i].taskID
		t.ScheduledTime = time.Unix(0, meta[i].visUnixNano)
		out = append(out, t)
	}
	if len(stale) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, stale).Err()
	}
	return out, next, nil
}

func (db *DB) DeleteTimerTask(ctx context.Context, shardID int, taskID int64, visibilityTimestamp time.Time) error {
	visUnixNano := visibilityTimestamp.UnixNano()
	member := timerTaskMember(visUnixNano, taskID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kTimerTask(shardID, visUnixNano, taskID))
	pipe.ZRem(ctx, db.kTimerTaskIndex(shardID), member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) RangeDeleteTimerTasks(ctx context.Context, shardID int, inclusiveMinTime, exclusiveMaxTime time.Time) error {
	kIdx := db.kTimerTaskIndex(shardID)
	min := "[" + encodeInt64Lex(inclusiveMinTime.UnixNano()) + ":"
	max := "(" + encodeInt64Lex(exclusiveMaxTime.UnixNano()) + ":"
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{Min: min, Max: max}).Result()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	pipe := db.rdb.Pipeline()
	for _, m := range members {
		vis, id, err := parseTimerTaskMember(m)
		if err != nil {
			return err
		}
		pipe.Del(ctx, db.kTimerTask(shardID, vis, id))
	}
	pipe.ZRem(ctx, kIdx, members)
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectReplicationTasksOrderByTaskID(ctx context.Context, shardID, pageSize int, pageToken []byte, inclusiveMinTaskID, exclusiveMaxTaskID int64) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kIdx := db.kReplicationTaskIndex(shardID)
	min := "[" + encodeInt64Lex(inclusiveMinTaskID)
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	max := "(" + encodeInt64Lex(exclusiveMaxTaskID)
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
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
	if len(members) == 0 {
		return nil, next, nil
	}
	keys := make([]string, 0, len(members))
	taskIDs := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := parseInt64(m)
		if err != nil {
			return nil, nil, err
		}
		taskIDs = append(taskIDs, id)
		keys = append(keys, db.kReplicationTask(shardID, id))
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}
	out := make([]*nosqlplugin.HistoryMigrationTask, 0, len(vals))
	var stale []string
	for i, v := range vals {
		if v == nil {
			stale = append(stale, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectReplicationTasksOrderByTaskID: unexpected mget type for %q: %T", keys[i], v)
		}
		t := &nosqlplugin.HistoryMigrationTask{}
		if err := unmarshalJSON([]byte(s), t); err != nil {
			return nil, nil, err
		}
		t.TaskID = taskIDs[i]
		out = append(out, t)
	}
	if len(stale) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, stale).Err()
	}
	return out, next, nil
}

func (db *DB) DeleteReplicationTask(ctx context.Context, shardID int, taskID int64) error {
	member := encodeInt64Lex(taskID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kReplicationTask(shardID, taskID))
	pipe.ZRem(ctx, db.kReplicationTaskIndex(shardID), member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) RangeDeleteReplicationTasks(ctx context.Context, shardID int, exclusiveEndTaskID int64) error {
	kIdx := db.kReplicationTaskIndex(shardID)
	max := "(" + encodeInt64Lex(exclusiveEndTaskID)
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{Min: "-", Max: max}).Result()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	pipe := db.rdb.Pipeline()
	for _, m := range members {
		id, err := parseInt64(m)
		if err != nil {
			return err
		}
		pipe.Del(ctx, db.kReplicationTask(shardID, id))
	}
	pipe.ZRem(ctx, kIdx, members)
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) InsertReplicationTask(ctx context.Context, tasks []*nosqlplugin.HistoryMigrationTask, condition nosqlplugin.ShardCondition) error {
	if len(tasks) == 0 {
		return nil
	}
	shardID := condition.ShardID
	kShardRange := db.kShardRange(shardID)
	keysToWatch := []string{kShardRange}
	return db.watchWithRetry(ctx, keysToWatch, func(tx *redis.Tx) error {
		actualRangeStr, err := tx.Get(ctx, kShardRange).Result()
		if err != nil {
			return err
		}
		actualRange, err := strconv.ParseInt(actualRangeStr, 10, 64)
		if err != nil {
			return fmt.Errorf("kvrocks: InsertReplicationTask: parse shard range: %w", err)
		}
		if actualRange != condition.RangeID {
			return &nosqlplugin.ShardOperationConditionFailure{RangeID: actualRange}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, t := range tasks {
				if t == nil || t.Replication == nil {
					continue
				}
				taskID := t.Replication.TaskID
				copyTask := *t
				copyTask.TaskID = taskID
				b, err := marshalJSON(&copyTask)
				if err != nil {
					return err
				}
				pipe.Set(ctx, db.kReplicationTask(shardID, taskID), b, 0)
				pipe.ZAdd(ctx, db.kReplicationTaskIndex(shardID), redis.Z{Score: 0, Member: encodeInt64Lex(taskID)})
			}
			return nil
		})
		return err
	})
}

func (db *DB) DeleteCrossClusterTask(ctx context.Context, shardID int, targetCluster string, taskID int64) error {
	// Cross-cluster tasks are deprecated. Noop for now.
	return nil
}

func (db *DB) InsertReplicationDLQTask(ctx context.Context, shardID int, sourceCluster string, task *nosqlplugin.HistoryMigrationTask) error {
	if task == nil || task.Replication == nil {
		return fmt.Errorf("kvrocks: InsertReplicationDLQTask: nil task/replication")
	}
	taskID := task.Replication.TaskID
	b, err := marshalJSON(task)
	if err != nil {
		return err
	}
	pipe := db.rdb.Pipeline()
	pipe.Set(ctx, db.kReplicationDLQTask(shardID, sourceCluster, taskID), b, 0)
	pipe.ZAdd(ctx, db.kReplicationDLQTaskIndex(shardID, sourceCluster), redis.Z{Score: 0, Member: encodeInt64Lex(taskID)})
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectReplicationDLQTasksOrderByTaskID(ctx context.Context, shardID int, sourceCluster string, pageSize int, pageToken []byte, inclusiveMinTaskID, exclusiveMaxTaskID int64) ([]*nosqlplugin.HistoryMigrationTask, []byte, error) {
	if pageSize <= 0 {
		return nil, nil, nil
	}
	kIdx := db.kReplicationDLQTaskIndex(shardID, sourceCluster)
	min := "[" + encodeInt64Lex(inclusiveMinTaskID)
	if len(pageToken) > 0 {
		min = "(" + string(pageToken)
	}
	max := "(" + encodeInt64Lex(exclusiveMaxTaskID)
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{
		Min:    min,
		Max:    max,
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
	if len(members) == 0 {
		return nil, next, nil
	}
	keys := make([]string, 0, len(members))
	taskIDs := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := parseInt64(m)
		if err != nil {
			return nil, nil, err
		}
		taskIDs = append(taskIDs, id)
		keys = append(keys, db.kReplicationDLQTask(shardID, sourceCluster, id))
	}
	vals, err := db.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, err
	}
	out := make([]*nosqlplugin.HistoryMigrationTask, 0, len(vals))
	var stale []string
	for i, v := range vals {
		if v == nil {
			stale = append(stale, members[i])
			continue
		}
		s, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("kvrocks: SelectReplicationDLQTasksOrderByTaskID: unexpected mget type for %q: %T", keys[i], v)
		}
		t := &nosqlplugin.HistoryMigrationTask{}
		if err := unmarshalJSON([]byte(s), t); err != nil {
			return nil, nil, err
		}
		t.TaskID = taskIDs[i]
		out = append(out, t)
	}
	if len(stale) > 0 {
		_ = db.rdb.ZRem(ctx, kIdx, stale).Err()
	}
	return out, next, nil
}

func (db *DB) SelectReplicationDLQTasksCount(ctx context.Context, shardID int, sourceCluster string) (int64, error) {
	return db.rdb.ZCard(ctx, db.kReplicationDLQTaskIndex(shardID, sourceCluster)).Result()
}

func (db *DB) DeleteReplicationDLQTask(ctx context.Context, shardID int, sourceCluster string, taskID int64) error {
	member := encodeInt64Lex(taskID)
	pipe := db.rdb.Pipeline()
	pipe.Del(ctx, db.kReplicationDLQTask(shardID, sourceCluster, taskID))
	pipe.ZRem(ctx, db.kReplicationDLQTaskIndex(shardID, sourceCluster), member)
	_, err := pipe.Exec(ctx)
	return err
}

func (db *DB) RangeDeleteReplicationDLQTasks(ctx context.Context, shardID int, sourceCluster string, inclusiveBeginTaskID, exclusiveEndTaskID int64) error {
	kIdx := db.kReplicationDLQTaskIndex(shardID, sourceCluster)
	min := "[" + encodeInt64Lex(inclusiveBeginTaskID)
	max := "(" + encodeInt64Lex(exclusiveEndTaskID)
	members, err := db.rdb.ZRangeByLex(ctx, kIdx, &redis.ZRangeBy{Min: min, Max: max}).Result()
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	pipe := db.rdb.Pipeline()
	for _, m := range members {
		id, err := parseInt64(m)
		if err != nil {
			return err
		}
		pipe.Del(ctx, db.kReplicationDLQTask(shardID, sourceCluster, id))
	}
	pipe.ZRem(ctx, kIdx, members)
	_, err = pipe.Exec(ctx)
	return err
}

func (db *DB) SelectActiveClusterSelectionPolicy(ctx context.Context, shardID int, domainID, wfID, rID string) (*nosqlplugin.ActiveClusterSelectionPolicyRow, error) {
	b, err := db.rdb.Get(ctx, db.kWfActiveClusterSelectionPolicy(shardID, domainID, wfID, rID)).Bytes()
	if err != nil {
		return nil, err
	}
	blob := &persistence.DataBlob{}
	if err := unmarshalJSON(b, blob); err != nil {
		return nil, err
	}
	blob = nilIfEmptyDataBlob(blob)
	return &nosqlplugin.ActiveClusterSelectionPolicyRow{
		ShardID:    shardID,
		DomainID:   domainID,
		WorkflowID: wfID,
		RunID:      rID,
		Policy:     blob,
	}, nil
}

func (db *DB) DeleteActiveClusterSelectionPolicy(ctx context.Context, shardID int, domainID, workflowID, runID string) error {
	return db.rdb.Del(ctx, db.kWfActiveClusterSelectionPolicy(shardID, domainID, workflowID, runID)).Err()
}
