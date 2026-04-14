package tests

import (
	"fmt"

	"go.temporal.io/server/tests/testcore/umpire"
)

type nexusTaskCausalityRule struct{}

func (r *nexusTaskCausalityRule) Name() string { return "nexus-task-causality" }

func (r *nexusTaskCausalityRule) Check(history []*umpire.Record) []umpire.Violation {
	dispatched := make(map[string]map[nexusTaskKind]bool)
	var violations []umpire.Violation

	for _, rec := range history {
		event, ok := rec.Fact.(*nexusTaskEvent)
		if !ok {
			continue
		}
		if event.Outcome == nexusTaskOutcomeDispatched {
			if dispatched[event.OperationID] == nil {
				dispatched[event.OperationID] = make(map[nexusTaskKind]bool)
			}
			dispatched[event.OperationID][event.Kind] = true
			continue
		}
		if dispatched[event.OperationID][event.Kind] {
			continue
		}
		violations = append(violations, umpire.Violation{
			Rule:    r.Name(),
			Message: fmt.Sprintf("%s %s without prior dispatch", event.Kind, event.Outcome),
			Tags: map[string]string{
				"operationID": event.OperationID,
				"kind":        string(event.Kind),
				"outcome":     string(event.Outcome),
			},
		})
	}

	return violations
}

type nexusTerminalConsistencyRule struct {
	ops func() []*modelOp
}

func (r *nexusTerminalConsistencyRule) Name() string { return "nexus-terminal-consistency" }

func (r *nexusTerminalConsistencyRule) Check(history []*umpire.Record) []umpire.Violation {
	completions := make(map[string]map[nexusTaskKind]bool)
	for _, rec := range history {
		event, ok := rec.Fact.(*nexusTaskEvent)
		if !ok || event.Outcome != nexusTaskOutcomeCompleted {
			continue
		}
		if completions[event.OperationID] == nil {
			completions[event.OperationID] = make(map[nexusTaskKind]bool)
		}
		completions[event.OperationID][event.Kind] = true
	}

	var violations []umpire.Violation
	for _, op := range r.ops() {
		switch op.status {
		case modelStatusCompleted:
			if completions[op.operationID][nexusTaskKindStart] {
				continue
			}
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "operation completed in model but no start completion observed",
				Tags:    map[string]string{"operationID": op.operationID},
			})
		case modelStatusCanceled:
			if completions[op.operationID][nexusTaskKindCancel] {
				continue
			}
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "operation canceled in model but no cancel completion observed",
				Tags:    map[string]string{"operationID": op.operationID},
			})
		}
	}

	return violations
}

type nexusNoPostTerminalDispatchRule struct {
	ops func() []*modelOp
}

func (r *nexusNoPostTerminalDispatchRule) Name() string { return "nexus-no-post-terminal-dispatch" }

func (r *nexusNoPostTerminalDispatchRule) Check(history []*umpire.Record) []umpire.Violation {
	lastTerminalSeq := make(map[string]int64)
	for _, rec := range history {
		event, ok := rec.Fact.(*nexusTaskEvent)
		if !ok || event.Outcome == nexusTaskOutcomeDispatched {
			continue
		}
		if rec.Seq > lastTerminalSeq[event.OperationID] {
			lastTerminalSeq[event.OperationID] = rec.Seq
		}
	}

	var violations []umpire.Violation
	for _, rec := range history {
		event, ok := rec.Fact.(*nexusTaskEvent)
		if !ok || event.Outcome != nexusTaskOutcomeDispatched {
			continue
		}
		termSeq, hasTerminal := lastTerminalSeq[event.OperationID]
		if !hasTerminal || rec.Seq <= termSeq {
			continue
		}
		for _, op := range r.ops() {
			if op.operationID != event.OperationID || !isTerminalStatus(op.status) {
				continue
			}
			violations = append(violations, umpire.Violation{
				Rule:    r.Name(),
				Message: "task dispatched after operation reached terminal state",
				Tags: map[string]string{
					"operationID": event.OperationID,
					"kind":        string(event.Kind),
					"eventSeq":    fmt.Sprintf("%d", rec.Seq),
					"terminalSeq": fmt.Sprintf("%d", termSeq),
				},
			})
		}
	}

	return violations
}

func isTerminalStatus(s modelOpStatus) bool {
	return s == modelStatusCompleted || s == modelStatusCanceled || s == modelStatusFailed || s == modelStatusTerminated
}

func (m *nexusPropModel) RegisterRules() {
	m.Umpire.AddRule(&nexusTaskCausalityRule{})
	m.Umpire.AddRule(&nexusTerminalConsistencyRule{ops: m.sortedOps})
	m.Umpire.AddRule(&nexusNoPostTerminalDispatchRule{ops: m.sortedOps})
}
