package nexusoperation

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/protoutils"
)

func TestIsWaitStageReached(t *testing.T) {
	t.Parallel()

	ctx := &chasm.MockContext{}
	allStatuses := protoutils.EnumValues[nexusoperationpb.OperationStatus]()

	tests := []struct {
		name       string
		waitStage  enumspb.NexusOperationWaitStage
		reached    []nexusoperationpb.OperationStatus
		notReached []nexusoperationpb.OperationStatus
	}{
		{
			name:       "Unspecified",
			waitStage:  enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED,
			notReached: allStatuses,
		},
		{
			name:      "Started",
			waitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			reached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_STARTED,
				nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
				nexusoperationpb.OPERATION_STATUS_FAILED,
				nexusoperationpb.OPERATION_STATUS_CANCELED,
				nexusoperationpb.OPERATION_STATUS_TERMINATED,
				nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			},
			notReached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
			},
		},
		{
			name:      "Closed",
			waitStage: enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED,
			reached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
				nexusoperationpb.OPERATION_STATUS_FAILED,
				nexusoperationpb.OPERATION_STATUS_CANCELED,
				nexusoperationpb.OPERATION_STATUS_TERMINATED,
				nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			},
			notReached: []nexusoperationpb.OperationStatus{
				nexusoperationpb.OPERATION_STATUS_UNSPECIFIED,
				nexusoperationpb.OPERATION_STATUS_SCHEDULED,
				nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
				nexusoperationpb.OPERATION_STATUS_STARTED,
			},
		},
	}

	coveredWaitStages := []enumspb.NexusOperationWaitStage{}
	for _, tt := range tests {
		coveredWaitStages = append(coveredWaitStages, tt.waitStage)
		t.Run(tt.name, func(t *testing.T) {
			op := newTestOperation()

			coveredStatuses := append(slices.Clone(tt.reached), tt.notReached...)
			require.ElementsMatch(t, allStatuses, coveredStatuses)

			for _, status := range tt.reached {
				op.Status = status
				require.Truef(t, op.isWaitStageReached(ctx, tt.waitStage), "expected %s to match %s", status, tt.waitStage)
			}

			for _, status := range tt.notReached {
				op.Status = status
				require.Falsef(t, op.isWaitStageReached(ctx, tt.waitStage), "expected %s not to match %s", status, tt.waitStage)
			}
		})
	}

	allWaitStages := protoutils.EnumValues[enumspb.NexusOperationWaitStage]()
	require.ElementsMatch(t, allWaitStages, coveredWaitStages)
}

func TestDescribeOutcome(t *testing.T) {
	t.Parallel()

	successResult := payload.EncodeString("result")
	outcomeFailure := &failurepb.Failure{Message: "outcome failure"}
	lastAttemptFailure := &failurepb.Failure{Message: "last attempt failure"}

	successOutcome := &nexusoperationpb.OperationOutcome{
		Variant: &nexusoperationpb.OperationOutcome_Successful_{
			Successful: &nexusoperationpb.OperationOutcome_Successful{Result: successResult},
		},
	}
	failedOutcome := &nexusoperationpb.OperationOutcome{
		Variant: &nexusoperationpb.OperationOutcome_Failed_{
			Failed: &nexusoperationpb.OperationOutcome_Failed{Failure: outcomeFailure},
		},
	}

	tests := []struct {
		name            string
		status          nexusoperationpb.OperationStatus
		outcome         *nexusoperationpb.OperationOutcome // nil means no outcome set
		lastAttempt     *failurepb.Failure
		expectedResult  *commonpb.Payload
		expectedFailure *failurepb.Failure
	}{
		{
			name:            "TimedOut_PrefersLastAttemptFailureOverOutcome",
			status:          nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			outcome:         failedOutcome,
			lastAttempt:     lastAttemptFailure,
			expectedFailure: lastAttemptFailure,
		},
		{
			name:            "TimedOut_FallsBackToOutcomeFailure",
			status:          nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
			outcome:         failedOutcome,
			expectedFailure: outcomeFailure,
		},
		{
			name:            "NoOutcome_ReturnsLastAttemptFailure",
			status:          nexusoperationpb.OPERATION_STATUS_FAILED,
			lastAttempt:     lastAttemptFailure,
			expectedFailure: lastAttemptFailure,
		},
		{
			name:           "Successful",
			status:         nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
			outcome:        successOutcome,
			expectedResult: successResult,
		},
		{
			name:            "Failed",
			status:          nexusoperationpb.OPERATION_STATUS_FAILED,
			outcome:         failedOutcome,
			expectedFailure: outcomeFailure,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &chasm.MockMutableContext{}
			op := NewOperation(&nexusoperationpb.OperationState{Status: tc.status, LastAttemptFailure: tc.lastAttempt})
			if tc.outcome != nil {
				op.Outcome = chasm.NewDataField(ctx, tc.outcome)
			}

			result, failure := op.describeOutcome(ctx)
			protorequire.ProtoEqual(t, tc.expectedResult, result)
			protorequire.ProtoEqual(t, tc.expectedFailure, failure)
		})
	}
}
