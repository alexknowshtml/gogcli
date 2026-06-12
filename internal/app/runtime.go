package app

import (
	"context"
	"io"
	"net/http"

	"google.golang.org/api/drive/v3"
)

type IO struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

type (
	DriveServiceFactory func(context.Context, string) (*drive.Service, error)
	DriveDownloadFunc   func(context.Context, *drive.Service, string) (*http.Response, error)
	DriveExportFunc     func(context.Context, *drive.Service, string, string) (*http.Response, error)
)

type Services struct {
	Drive         DriveServiceFactory
	DriveDownload DriveDownloadFunc
	DriveExport   DriveExportFunc
}

type Runtime struct {
	IO       IO
	Services Services
}

type runtimeContextKey struct{}

func WithRuntime(ctx context.Context, runtime *Runtime) context.Context {
	return context.WithValue(ctx, runtimeContextKey{}, runtime)
}

func FromContext(ctx context.Context) (*Runtime, bool) {
	runtime, ok := ctx.Value(runtimeContextKey{}).(*Runtime)
	return runtime, ok && runtime != nil
}

func IOFromContext(ctx context.Context) (IO, bool) {
	runtime, ok := FromContext(ctx)
	if !ok {
		return IO{}, false
	}

	return runtime.IO, true
}
