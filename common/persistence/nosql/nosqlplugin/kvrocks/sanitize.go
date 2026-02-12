package kvrocks

import "github.com/uber/cadence/common/persistence"

func nilIfEmptyDataBlob(b *persistence.DataBlob) *persistence.DataBlob {
	if b == nil || len(b.Data) == 0 {
		return nil
	}
	return b
}

// sanitizeMutableState converts "nil-safe" empty blobs (DataBlob{Data:nil/empty})
// back to nil pointers, matching what the higher-level deserializers expect.
//
// The NoSQL execution store util frequently uses ToNilSafeDataBlob() to avoid nil
// pointer access, but these empty blobs must not be persisted as-is since
// deserializers treat non-nil empty blobs as corrupt data.
func sanitizeMutableState(ms *persistence.InternalWorkflowMutableState) {
	if ms == nil {
		return
	}

	if ms.ExecutionInfo != nil {
		ms.ExecutionInfo.CompletionEvent = nilIfEmptyDataBlob(ms.ExecutionInfo.CompletionEvent)
		ms.ExecutionInfo.AutoResetPoints = nilIfEmptyDataBlob(ms.ExecutionInfo.AutoResetPoints)
		ms.ExecutionInfo.ActiveClusterSelectionPolicy = nilIfEmptyDataBlob(ms.ExecutionInfo.ActiveClusterSelectionPolicy)
	}

	ms.VersionHistories = nilIfEmptyDataBlob(ms.VersionHistories)
	ms.ChecksumData = nilIfEmptyDataBlob(ms.ChecksumData)

	if len(ms.BufferedEvents) > 0 {
		out := ms.BufferedEvents[:0]
		for _, b := range ms.BufferedEvents {
			if b == nil || len(b.Data) == 0 {
				continue
			}
			out = append(out, b)
		}
		if len(out) == 0 {
			ms.BufferedEvents = nil
		} else {
			ms.BufferedEvents = out
		}
	}

	for _, a := range ms.ActivityInfos {
		if a == nil {
			continue
		}
		a.ScheduledEvent = nilIfEmptyDataBlob(a.ScheduledEvent)
		a.StartedEvent = nilIfEmptyDataBlob(a.StartedEvent)
	}

	for _, c := range ms.ChildExecutionInfos {
		if c == nil {
			continue
		}
		c.InitiatedEvent = nilIfEmptyDataBlob(c.InitiatedEvent)
		c.StartedEvent = nilIfEmptyDataBlob(c.StartedEvent)
	}
}
