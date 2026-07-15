package interceptor

import (
	"context"
	"fmt"
	"runtime/debug"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
)

func PanicRecoveryInterceptor(logger log.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, retErr error) {
		defer func() {
			if panicObj := recover(); panicObj != nil {
				err, ok := panicObj.(error)
				if !ok {
					err = fmt.Errorf("panic: %v", panicObj)
				}

				st := string(debug.Stack())
				logger.Error("Panic is captured", tag.SysStackTrace(st), tag.Error(err))

				resp = nil
				retErr = serviceerror.NewInternal(err.Error())
			}
		}()
		return handler(ctx, req)
	}
}
