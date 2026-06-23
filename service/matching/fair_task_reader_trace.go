package matching

import (
	"sync/atomic"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// writePathTracer emits diagnostic logs for write-path mergeTasksLocked calls.
// It is gated by the FairReaderWritePathRecovery dynamic config and capped at
// maxWritePathTraceLogs emissions per enable cycle to avoid log flooding.
// Toggling the flag off and back on resets the counter.
type writePathTracer struct {
	logger  log.Logger
	enabled func() bool // reads the dynamic config
	count   atomic.Int32
	wasOn   bool
}

const maxWritePathTraceLogs = 50

// begin checks whether tracing is active and returns a writePathTrace to collect
// data into, or nil if tracing is off or the cap has been reached.
func (wt *writePathTracer) begin(state writePathTraceState) *writePathTrace {
	on := wt.enabled()
	if !on {
		wt.wasOn = false
		return nil
	}
	// Reset counter on off→on transition.
	if !wt.wasOn {
		wt.count.Store(0)
		wt.wasOn = true
	}
	if wt.count.Load() >= maxWritePathTraceLogs {
		return nil
	}
	return &writePathTrace{owner: wt, before: state}
}

// writePathTraceState captures the reader state at a point in time.
type writePathTraceState struct {
	atEnd       bool
	loadedTasks int
	readLevel   fairLevel
	ackLevel    fairLevel
	readPending bool
}

// writePathTrace collects diagnostic counters for a single mergeTasksLocked call.
// Created by writePathTracer.begin, emitted by emit.
type writePathTrace struct {
	owner  *writePathTracer
	before writePathTraceState

	inputTasks             int
	filteredBelowAck       int
	filteredAboveReadLevel int
	filteredDupe           int
	filteredEvictedAck     int
	mergedSetSize          int
	batchSize              int
	tasksEvicted           int
	acksEvicted            int
	expired                int
	tasksAdded             int
}

// emit logs the collected trace if the cap hasn't been reached.
func (t *writePathTrace) emit(after writePathTraceState) {
	if t == nil {
		return
	}
	n := t.owner.count.Add(1)
	if n > maxWritePathTraceLogs {
		return
	}
	t.owner.logger.Info("fair-reader write-path trace",
		tag.NewInt("trace-seq", int(n)),
		// before state
		tag.NewBoolTag("before-atEnd", t.before.atEnd),
		tag.NewInt("before-loadedTasks", t.before.loadedTasks),
		tag.NewInt64("before-readLevel-id", t.before.readLevel.id),
		tag.NewInt("before-readLevel-pass", int(t.before.readLevel.pass)),
		tag.NewInt64("before-ackLevel-id", t.before.ackLevel.id),
		tag.NewInt("before-ackLevel-pass", int(t.before.ackLevel.pass)),
		tag.NewBoolTag("before-readPending", t.before.readPending),
		// after state
		tag.NewBoolTag("after-atEnd", after.atEnd),
		tag.NewInt("after-loadedTasks", after.loadedTasks),
		tag.NewInt64("after-readLevel-id", after.readLevel.id),
		tag.NewInt("after-readLevel-pass", int(after.readLevel.pass)),
		tag.NewInt64("after-ackLevel-id", after.ackLevel.id),
		tag.NewInt("after-ackLevel-pass", int(after.ackLevel.pass)),
		tag.NewBoolTag("after-readPending", after.readPending),
		// counters
		tag.NewInt("input-tasks", t.inputTasks),
		tag.NewInt("filtered-below-ack", t.filteredBelowAck),
		tag.NewInt("filtered-above-read-level", t.filteredAboveReadLevel),
		tag.NewInt("filtered-dupe", t.filteredDupe),
		tag.NewInt("filtered-evicted-ack", t.filteredEvictedAck),
		tag.NewInt("merged-set-size", t.mergedSetSize),
		tag.NewInt("batch-size", t.batchSize),
		tag.NewInt("tasks-evicted", t.tasksEvicted),
		tag.NewInt("acks-evicted", t.acksEvicted),
		tag.NewInt("expired", t.expired),
		tag.NewInt("tasks-added", t.tasksAdded),
	)
}
