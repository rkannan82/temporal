package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

func TestPanicRecoveryInterceptor_PanicWithError(t *testing.T) {
	interceptor := PanicRecoveryInterceptor(log.NewTestLogger())
	handler := func(ctx context.Context, req any) (any, error) {
		panic(errors.New("something broke"))
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	require.Nil(t, resp)
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
	require.Equal(t, "something broke", internalErr.Message)
}

func TestPanicRecoveryInterceptor_PanicWithString(t *testing.T) {
	interceptor := PanicRecoveryInterceptor(log.NewTestLogger())
	handler := func(ctx context.Context, req any) (any, error) {
		panic("string panic")
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	require.Nil(t, resp)
	var internalErr *serviceerror.Internal
	require.ErrorAs(t, err, &internalErr)
	require.Contains(t, internalErr.Message, "string panic")
}

func TestPanicRecoveryInterceptor_NoPanic(t *testing.T) {
	interceptor := PanicRecoveryInterceptor(log.NewTestLogger())
	expected := "response"
	handler := func(ctx context.Context, req any) (any, error) {
		return expected, nil
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	require.NoError(t, err)
	require.Equal(t, expected, resp)
}
