package cmd

import (
	"context"
	"io"
	"net/http"
	"os"

	"google.golang.org/api/calendar/v3"
	"google.golang.org/api/classroom/v1"
	"google.golang.org/api/cloudidentity/v1"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/people/v1"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"

	"github.com/steipete/gogcli/internal/app"
	"github.com/steipete/gogcli/internal/googleapi"
	"github.com/steipete/gogcli/internal/googleauth"
)

func newDefaultRuntime() *app.Runtime {
	return &app.Runtime{
		IO: app.IO{
			In:  os.Stdin,
			Out: os.Stdout,
			Err: os.Stderr,
		},
		Services: app.Services{
			Calendar:      googleapi.NewCalendar,
			Classroom:     googleapi.NewClassroom,
			CloudIdentity: newCloudIdentityService,
			Docs:          googleapi.NewDocs,
			DocsHTTP: func(ctx context.Context, account string) (*http.Client, error) {
				return googleapi.NewHTTPClient(ctx, googleauth.ServiceDocs, account)
			},
			Drive:           googleapi.NewDrive,
			Gmail:           googleapi.NewGmail,
			PeopleContacts:  newPeopleContactsService,
			PeopleDirectory: newPeopleDirectoryService,
			PeopleOther:     newPeopleOtherContactsService,
			Sheets:          googleapi.NewSheets,
			Slides:          googleapi.NewSlides,
			Zoom:            newZoomMeetingClient,
			DriveDownload:   driveDownload,
			DriveExport:     driveExportDownload,
		},
	}
}

func normalizedRuntime(runtime *app.Runtime) *app.Runtime {
	defaults := newDefaultRuntime()
	if runtime == nil {
		return defaults
	}
	normalized := *runtime
	if normalized.IO.In == nil {
		normalized.IO.In = defaults.IO.In
	}
	if normalized.IO.Out == nil {
		normalized.IO.Out = defaults.IO.Out
	}
	if normalized.IO.Err == nil {
		normalized.IO.Err = defaults.IO.Err
	}
	if normalized.Services.Calendar == nil {
		normalized.Services.Calendar = defaults.Services.Calendar
	}
	if normalized.Services.Classroom == nil {
		normalized.Services.Classroom = defaults.Services.Classroom
	}
	if normalized.Services.CloudIdentity == nil {
		normalized.Services.CloudIdentity = defaults.Services.CloudIdentity
	}
	if normalized.Services.Drive == nil {
		normalized.Services.Drive = defaults.Services.Drive
	}
	if normalized.Services.Docs == nil {
		normalized.Services.Docs = defaults.Services.Docs
	}
	if normalized.Services.DocsHTTP == nil {
		normalized.Services.DocsHTTP = defaults.Services.DocsHTTP
	}
	if normalized.Services.Gmail == nil {
		normalized.Services.Gmail = defaults.Services.Gmail
	}
	if normalized.Services.PeopleContacts == nil {
		normalized.Services.PeopleContacts = defaults.Services.PeopleContacts
	}
	if normalized.Services.PeopleDirectory == nil {
		normalized.Services.PeopleDirectory = defaults.Services.PeopleDirectory
	}
	if normalized.Services.PeopleOther == nil {
		normalized.Services.PeopleOther = defaults.Services.PeopleOther
	}
	if normalized.Services.Sheets == nil {
		normalized.Services.Sheets = defaults.Services.Sheets
	}
	if normalized.Services.Slides == nil {
		normalized.Services.Slides = defaults.Services.Slides
	}
	if normalized.Services.Zoom == nil {
		normalized.Services.Zoom = defaults.Services.Zoom
	}
	if normalized.Services.DriveDownload == nil {
		normalized.Services.DriveDownload = defaults.Services.DriveDownload
	}
	if normalized.Services.DriveExport == nil {
		normalized.Services.DriveExport = defaults.Services.DriveExport
	}
	return &normalized
}

func commandIO(ctx context.Context) app.IO {
	commandIO := newDefaultRuntime().IO
	if runtimeIO, ok := app.IOFromContext(ctx); ok {
		if runtimeIO.In != nil {
			commandIO.In = runtimeIO.In
		}
		if runtimeIO.Out != nil {
			commandIO.Out = runtimeIO.Out
		}
		if runtimeIO.Err != nil {
			commandIO.Err = runtimeIO.Err
		}
	}
	return commandIO
}

func stdoutWriter(ctx context.Context) io.Writer {
	return commandIO(ctx).Out
}

func stderrWriter(ctx context.Context) io.Writer {
	return commandIO(ctx).Err
}

func calendarService(ctx context.Context, account string) (*calendar.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Calendar != nil {
		return runtime.Services.Calendar(ctx, account)
	}
	return googleapi.NewCalendar(ctx, account)
}

func classroomService(ctx context.Context, account string) (*classroom.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Classroom != nil {
		return runtime.Services.Classroom(ctx, account)
	}
	return googleapi.NewClassroom(ctx, account)
}

func cloudIdentityService(ctx context.Context, account string) (*cloudidentity.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.CloudIdentity != nil {
		return runtime.Services.CloudIdentity(ctx, account)
	}
	return newCloudIdentityService(ctx, account)
}

func driveService(ctx context.Context, account string) (*drive.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Drive != nil {
		return runtime.Services.Drive(ctx, account)
	}
	return googleapi.NewDrive(ctx, account)
}

func docsService(ctx context.Context, account string) (*docs.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Docs != nil {
		return runtime.Services.Docs(ctx, account)
	}
	return googleapi.NewDocs(ctx, account)
}

func docsHTTPClient(ctx context.Context, account string) (*http.Client, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.DocsHTTP != nil {
		return runtime.Services.DocsHTTP(ctx, account)
	}
	return googleapi.NewHTTPClient(ctx, googleauth.ServiceDocs, account)
}

func gmailService(ctx context.Context, account string) (*gmail.Service, error) {
	return gmailServiceFactory(ctx)(ctx, account)
}

func gmailServiceFactory(ctx context.Context) app.GmailServiceFactory {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Gmail != nil {
		return runtime.Services.Gmail
	}
	return googleapi.NewGmail
}

func peopleContactsService(ctx context.Context, account string) (*people.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.PeopleContacts != nil {
		return runtime.Services.PeopleContacts(ctx, account)
	}
	return newPeopleContactsService(ctx, account)
}

func peopleDirectoryService(ctx context.Context, account string) (*people.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.PeopleDirectory != nil {
		return runtime.Services.PeopleDirectory(ctx, account)
	}
	return newPeopleDirectoryService(ctx, account)
}

func peopleOtherContactsService(ctx context.Context, account string) (*people.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.PeopleOther != nil {
		return runtime.Services.PeopleOther(ctx, account)
	}
	return newPeopleOtherContactsService(ctx, account)
}

func sheetsService(ctx context.Context, account string) (*sheets.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Sheets != nil {
		return runtime.Services.Sheets(ctx, account)
	}
	return googleapi.NewSheets(ctx, account)
}

func slidesService(ctx context.Context, account string) (*slides.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Slides != nil {
		return runtime.Services.Slides(ctx, account)
	}
	return googleapi.NewSlides(ctx, account)
}

func zoomMeetingClient(ctx context.Context, alias string) (app.ZoomMeetingClient, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Zoom != nil {
		return runtime.Services.Zoom(alias)
	}
	return newZoomMeetingClient(alias)
}

func driveDownloadRequest(ctx context.Context, svc *drive.Service, fileID string) (*http.Response, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.DriveDownload != nil {
		return runtime.Services.DriveDownload(ctx, svc, fileID)
	}
	return driveDownload(ctx, svc, fileID)
}

func driveExportRequest(ctx context.Context, svc *drive.Service, fileID, mimeType string) (*http.Response, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.DriveExport != nil {
		return runtime.Services.DriveExport(ctx, svc, fileID, mimeType)
	}
	return driveExportDownload(ctx, svc, fileID, mimeType)
}
