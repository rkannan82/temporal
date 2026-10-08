package workercommands

import (
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/tasktoken"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

// BuildCommands builds fully populated worker commands for dispatch.
//
// Commands are constructed from two sources:
//  1. Mutable state scan: always performed to find reconstructable commands
//     (e.g., activity cancellations derived from pending activity state).
//  2. Explicit commands: any commands in task.Commands that carry their own
//     payload (e.g., old-format tasks with populated task tokens, or future
//     command types that aren't reconstructable from mutable state).
//
// During rolling deploys, both sources may produce the same command. This is
// harmless because worker commands are idempotent.
func BuildCommands(
	task *tasks.WorkerCommandsTask,
	ms historyi.MutableState,
) ([]*workerpb.WorkerCommand, error) {
	// 1. Scan mutable state for reconstructable commands.
	var commands []*workerpb.WorkerCommand
	if ms != nil {
		var err error
		commands, err = buildCommandsFromMutableState(ms, task.Destination)
		if err != nil {
			return nil, err
		}
	}

	// 2. Include explicit commands from the task (backward compat + future extensibility).
	for _, cmd := range task.Commands {
		switch c := cmd.GetType().(type) {
		case *workerpb.WorkerCommand_CancelActivity:
			if len(c.CancelActivity.GetTaskToken()) > 0 {
				commands = append(commands, cmd)
			}
			// Empty-token cancel markers are skipped — already covered by the scan.
		default:
			// Future command types with explicit payloads: include as-is.
			commands = append(commands, cmd)
		}
	}

	return commands, nil
}

// buildCommandsFromMutableState scans mutable state for pending commands that
// need to be sent to the given control queue.
func buildCommandsFromMutableState(
	ms historyi.MutableState,
	controlQueue string,
) ([]*workerpb.WorkerCommand, error) {
	var commands []*workerpb.WorkerCommand

	cancelCommands, err := buildCancelCommands(ms, controlQueue)
	if err != nil {
		return nil, err
	}
	commands = append(commands, cancelCommands...)

	// Future: add scans for other reconstructable command types here.

	return commands, nil
}

// buildCancelCommands finds activities matching the control queue that are
// eligible for cancellation and builds CancelActivityCommand entries with
// full task tokens.
func buildCancelCommands(
	ms historyi.MutableState,
	controlQueue string,
) ([]*workerpb.WorkerCommand, error) {
	serializer := tasktoken.NewSerializer()
	wfKey := ms.GetWorkflowKey()
	nsID := ms.GetNamespaceEntry().ID().String()
	workflowRunning := ms.IsWorkflowExecutionRunning()

	var commands []*workerpb.WorkerCommand
	for _, ai := range ms.GetPendingActivityInfos() {
		if ai.WorkerControlTaskQueue != controlQueue {
			continue
		}
		if ai.StartedClock == nil {
			continue
		}
		// Activity is eligible if cancel was requested or the workflow is closed.
		if !ai.CancelRequested && workflowRunning {
			continue
		}

		tokenBytes, err := serializer.Serialize(tasktoken.NewActivityTaskToken(
			nsID,
			wfKey.WorkflowID,
			wfKey.RunID,
			ai.ScheduledEventId,
			ai.ActivityId,
			ai.ActivityType.GetName(),
			ai.Attempt,
			ai.StartedClock,
			ai.Version,
			ai.StartVersion,
			nil,
			0,
		))
		if err != nil {
			return nil, err
		}

		commands = append(commands, &workerpb.WorkerCommand{
			Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{
					TaskToken: tokenBytes,
				},
			},
		})
	}
	return commands, nil
}
