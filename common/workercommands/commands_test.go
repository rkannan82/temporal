package workercommands

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workerpb "go.temporal.io/api/worker/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBuildCommands_OldFormatPassThrough(t *testing.T) {
	task := &tasks.WorkerCommandsTask{
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{TaskToken: []byte("existing-token")},
			}},
		},
		Destination: "control-queue",
	}

	commands, err := BuildCommands(task, nil)
	require.NoError(t, err)
	require.Len(t, commands, 1)
	require.Equal(t, []byte("existing-token"), commands[0].GetCancelActivity().GetTaskToken())
}

func TestBuildCommands_EmptyCommands(t *testing.T) {
	task := &tasks.WorkerCommandsTask{
		Destination: "control-queue",
	}

	commands, err := BuildCommands(task, nil)
	require.NoError(t, err)
	require.Empty(t, commands)
}

func TestBuildCommands_NewFormat_BuildsFromMutableState(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := interfaces.NewMockMutableState(ctrl)

	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id"))
	ms.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "ns-id", Name: "test-ns"},
		&persistencespb.NamespaceConfig{},
		"",
	))
	ms.EXPECT().IsWorkflowExecutionRunning().Return(false)
	ms.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		1: {
			ScheduledEventId:       1,
			ActivityId:             "activity-1",
			ActivityType:           &commonpb.ActivityType{Name: "MyActivity"},
			Attempt:                1,
			StartedClock:           &clockspb.VectorClock{ClusterId: 1, Clock: 100},
			WorkerControlTaskQueue: "control-queue",
			CancelRequested:        true,
			Version:                1,
			StartVersion:           1,
			ScheduledTime:          timestamppb.Now(),
		},
	})

	task := &tasks.WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{},
			}},
		},
		Destination: "control-queue",
	}

	commands, err := BuildCommands(task, ms)
	require.NoError(t, err)
	require.Len(t, commands, 1)
	require.NotEmpty(t, commands[0].GetCancelActivity().GetTaskToken())
}

func TestBuildCommands_NewFormat_NoMatchingActivities(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := interfaces.NewMockMutableState(ctrl)

	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id"))
	ms.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "ns-id", Name: "test-ns"},
		&persistencespb.NamespaceConfig{},
		"",
	))
	ms.EXPECT().IsWorkflowExecutionRunning().Return(false)
	ms.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		1: {
			ScheduledEventId:       1,
			ActivityId:             "activity-1",
			WorkerControlTaskQueue: "other-queue", // different queue
			CancelRequested:        true,
			StartedClock:           &clockspb.VectorClock{ClusterId: 1, Clock: 100},
		},
	})

	task := &tasks.WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{},
			}},
		},
		Destination: "control-queue",
	}

	commands, err := BuildCommands(task, ms)
	require.NoError(t, err)
	require.Empty(t, commands)
}

func TestBuildCommands_NewFormat_SkipsRunningWorkflowWithoutCancelRequested(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := interfaces.NewMockMutableState(ctrl)

	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id"))
	ms.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "ns-id", Name: "test-ns"},
		&persistencespb.NamespaceConfig{},
		"",
	))
	ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
	ms.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		1: {
			ScheduledEventId:       1,
			ActivityId:             "activity-1",
			WorkerControlTaskQueue: "control-queue",
			CancelRequested:        false, // not cancel-requested
			StartedClock:           &clockspb.VectorClock{ClusterId: 1, Clock: 100},
		},
	})

	task := &tasks.WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{},
			}},
		},
		Destination: "control-queue",
	}

	commands, err := BuildCommands(task, ms)
	require.NoError(t, err)
	require.Empty(t, commands)
}

func TestBuildCommands_NewFormat_SkipsActivityWithoutStartedClock(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := interfaces.NewMockMutableState(ctrl)

	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id"))
	ms.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "ns-id", Name: "test-ns"},
		&persistencespb.NamespaceConfig{},
		"",
	))
	ms.EXPECT().IsWorkflowExecutionRunning().Return(false)
	ms.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		1: {
			ScheduledEventId:       1,
			ActivityId:             "activity-1",
			WorkerControlTaskQueue: "control-queue",
			CancelRequested:        true,
			StartedClock:           nil, // not started
		},
	})

	task := &tasks.WorkerCommandsTask{
		WorkflowKey: definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
		Commands: []*workerpb.WorkerCommand{
			{Type: &workerpb.WorkerCommand_CancelActivity{
				CancelActivity: &workerpb.CancelActivityCommand{},
			}},
		},
		Destination: "control-queue",
	}

	commands, err := BuildCommands(task, ms)
	require.NoError(t, err)
	require.Empty(t, commands)
}
