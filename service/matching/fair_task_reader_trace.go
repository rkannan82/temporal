package matching

import (
	"fmt"
	"strings"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const writeTraceRingSize = 20

// writeTaskTracer keeps a ring buffer of the most recent write-path mergeTasksLocked
// summaries. When stuck state is detected, the ring is dumped as a single log line.
// The ring is always populated (no dynamic config gating) so that it captures the
// history leading up to a stuck state without needing to be pre-enabled.
type writeTaskTracer struct {
	logger log.Logger
	ring   [writeTraceRingSize]writeTraceEntry
	pos    int  // next write position in ring
	count  int  // total entries written (min(count, writeTraceRingSize) entries are valid)
	dumped bool // true after we've dumped once per stuck episode
}

// writeTraceEntry is one write-path mergeTasksLocked summary.
type writeTraceEntry struct {
	beforeAtEnd            bool
	beforeLoadedTasks      int
	afterAtEnd             bool
	afterLoadedTasks       int
	filteredAboveReadLevel int
	batchSize              int
	tasksEvicted           int
}

func (e writeTraceEntry) String() string {
	return fmt.Sprintf("{atEnd:%v→%v loaded:%d→%d filtered:%d batch:%d evicted:%d}",
		e.beforeAtEnd, e.afterAtEnd,
		e.beforeLoadedTasks, e.afterLoadedTasks,
		e.filteredAboveReadLevel, e.batchSize, e.tasksEvicted)
}

// begin starts recording a new trace entry. Always returns a non-nil entry.
func (wt *writeTaskTracer) begin(beforeAtEnd bool, beforeLoadedTasks int) *writeTraceEntry {
	entry := &wt.ring[wt.pos]
	*entry = writeTraceEntry{
		beforeAtEnd:       beforeAtEnd,
		beforeLoadedTasks: beforeLoadedTasks,
	}
	return entry
}

// advance moves the ring position forward. Called after mergeTasksLocked fills in the entry.
func (wt *writeTaskTracer) advance(entry *writeTraceEntry, afterAtEnd bool, afterLoadedTasks int) {
	entry.afterAtEnd = afterAtEnd
	entry.afterLoadedTasks = afterLoadedTasks
	wt.pos = (wt.pos + 1) % writeTraceRingSize
	wt.count++
}

// dumpOnStuck logs the ring buffer contents when stuck state is detected.
// Only dumps once per stuck episode; resets when a non-stuck write occurs.
func (wt *writeTaskTracer) dumpOnStuck() {
	if wt.dumped {
		return
	}
	wt.dumped = true

	n := wt.count
	if n > writeTraceRingSize {
		n = writeTraceRingSize
	}
	if n == 0 {
		return
	}

	// Build entries oldest-first.
	start := wt.pos - n
	if start < 0 {
		start += writeTraceRingSize
	}
	var sb strings.Builder
	for i := 0; i < n; i++ {
		idx := (start + i) % writeTraceRingSize
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(wt.ring[idx].String())
	}

	wt.logger.Info("fair-reader stuck: write-path history",
		tag.NewInt("total-writes", wt.count),
		tag.NewStringTag("recent-merges", sb.String()),
	)
}

// clearStuck resets the dumped flag so we dump again on next stuck detection.
func (wt *writeTaskTracer) clearStuck() {
	wt.dumped = false
}
