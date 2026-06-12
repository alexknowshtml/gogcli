package app

import (
	"context"
	"io"
	"net/http"

	"google.golang.org/api/docs/v1"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/people/v1"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"
)

type IO struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

type (
	DocsServiceFactory   func(context.Context, string) (*docs.Service, error)
	DriveServiceFactory  func(context.Context, string) (*drive.Service, error)
	GmailServiceFactory  func(context.Context, string) (*gmail.Service, error)
	PeopleServiceFactory func(context.Context, string) (*people.Service, error)
	SheetsServiceFactory func(context.Context, string) (*sheets.Service, error)
	SlidesServiceFactory func(context.Context, string) (*slides.Service, error)
	DriveDownloadFunc    func(context.Context, *drive.Service, string) (*http.Response, error)
	DriveExportFunc      func(context.Context, *drive.Service, string, string) (*http.Response, error)
)

type Services struct {
	Docs           DocsServiceFactory
	Drive          DriveServiceFactory
	Gmail          GmailServiceFactory
	PeopleContacts PeopleServiceFactory
	Sheets         SheetsServiceFactory
	Slides         SlidesServiceFactory
	DriveDownload  DriveDownloadFunc
	DriveExport    DriveExportFunc
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
