package kvrocks

import (
	"fmt"
	"strings"

	"github.com/uber/cadence/common/persistence"
)

func (db *DB) key(parts ...string) string {
	if db.ns == "" {
		return strings.Join(parts, ":")
	}
	return db.ns + ":" + strings.Join(parts, ":")
}

func shardTag(shardID int) string {
	// Use Redis hash tags for cluster compatibility (all shard keys end up in the same slot).
	return fmt.Sprintf("{%d}", shardID)
}

func taskListTag(domainID, taskListName string, taskListType int) string {
	// Group all tasklist keys into the same Redis Cluster hash slot.
	return fmt.Sprintf("{tl|%s|%s|%d}", domainID, taskListName, taskListType)
}

// ---- domain keys (global) ----

func (db *DB) kDomainByName(name string) string { return db.key("domain", "name", name) }
func (db *DB) kDomainByID(id string) string     { return db.key("domain", "id", id) }
func (db *DB) kDomainRow(name string) string    { return db.key("domain", "row", name) }
func (db *DB) kDomainMeta() string              { return db.key("domain", "meta") }
func (db *DB) kDomainNamesIndex() string        { return db.key("domain", "names") }

// ---- shard keys (per-shard) ----

func (db *DB) kShardRange(shardID int) string { return db.key(shardTag(shardID), "shard", "range") }
func (db *DB) kShardRow(shardID int) string   { return db.key(shardTag(shardID), "shard", "row") }

// ---- history v2 keys ----

func (db *DB) kHistTreeRow(shardID int, treeID, branchID string) string {
	return db.key(shardTag(shardID), "hist", "tree", treeID, branchID)
}

func (db *DB) kHistTreeBranches(shardID int, treeID string) string {
	return db.key(shardTag(shardID), "hist", "tree", treeID, "branches")
}

// Global index for (shardID, treeID, branchID). Used by SelectAllHistoryTrees when history is not sharded.
func (db *DB) kHistAllTreesIndex() string { return db.key("hist", "trees") }

func (db *DB) kHistNodeIndex(shardID int, treeID, branchID string) string {
	return db.key(shardTag(shardID), "hist", "nodeidx", treeID, branchID)
}

func (db *DB) kHistNodeRow(shardID int, treeID, branchID string, nodeID int64, txnID int64) string {
	return db.key(shardTag(shardID), "hist", "node", treeID, branchID, encodeInt64Lex(nodeID), encodeInt64Lex(txnID))
}

// ---- config store keys ----

func (db *DB) kCfgVersions(rowType int) string {
	return db.key("cfg", fmt.Sprintf("%d", rowType), "versions")
}
func (db *DB) kCfgEntry(rowType int, version int64) string {
	return db.key("cfg", fmt.Sprintf("%d", rowType), "v", fmt.Sprintf("%d", version))
}

// ---- queue keys ----

func (db *DB) kQueueIndex(queueType persistence.QueueType) string {
	return db.key("queue", fmt.Sprintf("%d", queueType), "idx")
}

func (db *DB) kQueueMessage(queueType persistence.QueueType, messageID int64) string {
	return db.key("queue", fmt.Sprintf("%d", queueType), "msg", encodeInt64Lex(messageID))
}

func (db *DB) kQueueMetadata(queueType persistence.QueueType) string {
	return db.key("queue", fmt.Sprintf("%d", queueType), "meta")
}

// ---- matching task keys ----

func (db *DB) kTaskListRow(domainID, taskListName string, taskListType int) string {
	return db.key(taskListTag(domainID, taskListName, taskListType), "tasklist", "row")
}

func (db *DB) kTaskListTasksIndex(domainID, taskListName string, taskListType int) string {
	return db.key(taskListTag(domainID, taskListName, taskListType), "tasklist", "tasks")
}

func (db *DB) kTaskRow(domainID, taskListName string, taskListType int, taskID int64) string {
	return db.key(taskListTag(domainID, taskListName, taskListType), "task", encodeInt64Lex(taskID))
}

// ---- workflow execution keys (per-shard) ----

func (db *DB) kWfCurrent(shardID int, domainID, workflowID string) string {
	return db.key(shardTag(shardID), "wf", "current", domainID, workflowID)
}

func (db *DB) kWfCurrentIndex(shardID int) string {
	return db.key(shardTag(shardID), "wf", "current", "idx")
}

func (db *DB) kWfExecution(shardID int, domainID, workflowID, runID string) string {
	return db.key(shardTag(shardID), "wf", "exec", domainID, workflowID, runID)
}

func (db *DB) kWfExecutionNextEventID(shardID int, domainID, workflowID, runID string) string {
	return db.key(shardTag(shardID), "wf", "exec", "next", domainID, workflowID, runID)
}

func (db *DB) kWfExecutionIndex(shardID int) string {
	return db.key(shardTag(shardID), "wf", "exec", "idx")
}

func (db *DB) kWfRequest(shardID int, domainID, workflowID string, requestType persistence.WorkflowRequestType, requestID string) string {
	return db.key(shardTag(shardID), "wf", "req", domainID, workflowID, fmt.Sprintf("%d", requestType), requestID)
}

func (db *DB) kWfActiveClusterSelectionPolicy(shardID int, domainID, workflowID, runID string) string {
	return db.key(shardTag(shardID), "wf", "acsp", domainID, workflowID, runID)
}

// ---- internal history task keys (per-shard) ----

func (db *DB) kTransferTaskIndex(shardID int) string {
	return db.key(shardTag(shardID), "htask", "transfer", "idx")
}

func (db *DB) kTransferTask(shardID int, taskID int64) string {
	return db.key(shardTag(shardID), "htask", "transfer", encodeInt64Lex(taskID))
}

func (db *DB) kReplicationTaskIndex(shardID int) string {
	return db.key(shardTag(shardID), "htask", "replication", "idx")
}

func (db *DB) kReplicationTask(shardID int, taskID int64) string {
	return db.key(shardTag(shardID), "htask", "replication", encodeInt64Lex(taskID))
}

func (db *DB) kTimerTaskIndex(shardID int) string {
	return db.key(shardTag(shardID), "htask", "timer", "idx")
}

func (db *DB) kTimerTask(shardID int, visibilityUnixNano int64, taskID int64) string {
	return db.key(shardTag(shardID), "htask", "timer", encodeInt64Lex(visibilityUnixNano), encodeInt64Lex(taskID))
}

func (db *DB) kReplicationDLQTaskIndex(shardID int, sourceCluster string) string {
	return db.key(shardTag(shardID), "htask", "replication_dlq", sourceCluster, "idx")
}

func (db *DB) kReplicationDLQTask(shardID int, sourceCluster string, taskID int64) string {
	return db.key(shardTag(shardID), "htask", "replication_dlq", sourceCluster, encodeInt64Lex(taskID))
}

// ---- visibility keys (global, partitioned by domainID) ----

func (db *DB) kVisRow(domainID, runID string) string {
	return db.key("vis", domainID, "row", runID)
}

func (db *DB) kVisOpen(domainID string) string {
	return db.key("vis", domainID, "open")
}

func (db *DB) kVisOpenByType(domainID, workflowType string) string {
	return db.key("vis", domainID, "open", "type", workflowType)
}

func (db *DB) kVisOpenByWorkflowID(domainID, workflowID string) string {
	return db.key("vis", domainID, "open", "wid", workflowID)
}

func (db *DB) kVisClosedByStart(domainID string) string {
	return db.key("vis", domainID, "closed", "start")
}

func (db *DB) kVisClosedByClose(domainID string) string {
	return db.key("vis", domainID, "closed", "close")
}

func (db *DB) kVisClosedByStartType(domainID, workflowType string) string {
	return db.key("vis", domainID, "closed", "start", "type", workflowType)
}

func (db *DB) kVisClosedByCloseType(domainID, workflowType string) string {
	return db.key("vis", domainID, "closed", "close", "type", workflowType)
}

func (db *DB) kVisClosedByStartWorkflowID(domainID, workflowID string) string {
	return db.key("vis", domainID, "closed", "start", "wid", workflowID)
}

func (db *DB) kVisClosedByCloseWorkflowID(domainID, workflowID string) string {
	return db.key("vis", domainID, "closed", "close", "wid", workflowID)
}

func (db *DB) kVisClosedByStartStatus(domainID string, closeStatus int32) string {
	return db.key("vis", domainID, "closed", "start", "status", fmt.Sprintf("%d", closeStatus))
}

func (db *DB) kVisClosedByCloseStatus(domainID string, closeStatus int32) string {
	return db.key("vis", domainID, "closed", "close", "status", fmt.Sprintf("%d", closeStatus))
}

// ---- domain audit log keys (global, partitioned by domainID + operationType) ----

func (db *DB) kDomainAuditRow(domainID string, opType persistence.DomainAuditOperationType, eventID string) string {
	return db.key("domain_audit", domainID, fmt.Sprintf("%d", opType), "row", eventID)
}

func (db *DB) kDomainAuditIndex(domainID string, opType persistence.DomainAuditOperationType) string {
	return db.key("domain_audit", domainID, fmt.Sprintf("%d", opType), "idx")
}
