package app

import (
	"context"
	"io"
	"net/http"

	"google.golang.org/api/calendar/v3"
	"google.golang.org/api/classroom/v1"
	"google.golang.org/api/cloudidentity/v1"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/forms/v1"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/people/v1"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"
	"google.golang.org/api/tasks/v1"

	"github.com/steipete/gogcli/internal/zoom"
)

type IO struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

type (
	CalendarServiceFactory      func(context.Context, string) (*calendar.Service, error)
	ClassroomServiceFactory     func(context.Context, string) (*classroom.Service, error)
	CloudIdentityServiceFactory func(context.Context, string) (*cloudidentity.Service, error)
	DocsServiceFactory          func(context.Context, string) (*docs.Service, error)
	DocsHTTPClientFactory       func(context.Context, string) (*http.Client, error)
	DriveServiceFactory         func(context.Context, string) (*drive.Service, error)
	FormsServiceFactory         func(context.Context, string) (*forms.Service, error)
	GmailServiceFactory         func(context.Context, string) (*gmail.Service, error)
	PeopleServiceFactory        func(context.Context, string) (*people.Service, error)
	SheetsServiceFactory        func(context.Context, string) (*sheets.Service, error)
	SlidesServiceFactory        func(context.Context, string) (*slides.Service, error)
	TasksServiceFactory         func(context.Context, string) (*tasks.Service, error)
	ZoomMeetingClientFactory    func(string) (ZoomMeetingClient, error)
	DriveDownloadFunc           func(context.Context, *drive.Service, string) (*http.Response, error)
	DriveExportFunc             func(context.Context, *drive.Service, string, string) (*http.Response, error)
)

type ZoomMeetingClient interface {
	CreateMeeting(context.Context, string, zoom.CreateMeetingRequest) (*zoom.Meeting, error)
	DeleteMeeting(context.Context, string) error
}

type Services struct {
	Calendar        CalendarServiceFactory
	Classroom       ClassroomServiceFactory
	CloudIdentity   CloudIdentityServiceFactory
	Docs            DocsServiceFactory
	DocsHTTP        DocsHTTPClientFactory
	Drive           DriveServiceFactory
	Forms           FormsServiceFactory
	Gmail           GmailServiceFactory
	PeopleContacts  PeopleServiceFactory
	PeopleDirectory PeopleServiceFactory
	PeopleOther     PeopleServiceFactory
	Sheets          SheetsServiceFactory
	Slides          SlidesServiceFactory
	Tasks           TasksServiceFactory
	Zoom            ZoomMeetingClientFactory
	DriveDownload   DriveDownloadFunc
	DriveExport     DriveExportFunc
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
