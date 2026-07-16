package interceptor

import (
	"context"

	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

func PanicRecoveryInterceptor(logger log.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, retErr error) {
		defer log.CapturePanic(logger, &retErr)
		return handler(ctx, req)
	}
}
