package cmd

import (
	"context"
	"io"
	"net/http"
	"os"

	admin "google.golang.org/api/admin/directory/v1"
	analyticsadmin "google.golang.org/api/analyticsadmin/v1beta"
	analyticsdata "google.golang.org/api/analyticsdata/v1beta"
	"google.golang.org/api/calendar/v3"
	"google.golang.org/api/chat/v1"
	"google.golang.org/api/classroom/v1"
	"google.golang.org/api/cloudidentity/v1"
	"google.golang.org/api/docs/v1"
	"google.golang.org/api/drive/v3"
	formsapi "google.golang.org/api/forms/v1"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/people/v1"
	searchconsoleapi "google.golang.org/api/searchconsole/v1"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"
	"google.golang.org/api/tasks/v1"

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
			AdminDirectory: googleapi.NewAdminDirectory,
			AdminOrgUnit:   googleapi.NewAdminDirectoryOrgUnit,
			AnalyticsAdmin: googleapi.NewAnalyticsAdmin,
			AnalyticsData:  googleapi.NewAnalyticsData,
			Calendar:       googleapi.NewCalendar,
			Chat:           googleapi.NewChat,
			Classroom:      googleapi.NewClassroom,
			CloudIdentity:  googleapi.NewCloudIdentityGroups,
			Docs:           googleapi.NewDocs,
			DocsHTTP: func(ctx context.Context, account string) (*http.Client, error) {
				return googleapi.NewHTTPClient(ctx, googleauth.ServiceDocs, account)
			},
			Drive:           googleapi.NewDrive,
			Forms:           googleapi.NewForms,
			Gmail:           googleapi.NewGmail,
			PeopleContacts:  googleapi.NewPeopleContacts,
			PeopleDirectory: googleapi.NewPeopleDirectory,
			PeopleOther:     googleapi.NewPeopleOtherContacts,
			Photos:          newPhotosClient,
			PhotosPicker:    newPhotosPickerClient,
			SearchConsole:   googleapi.NewSearchConsole,
			Sheets:          googleapi.NewSheets,
			Slides:          googleapi.NewSlides,
			Tasks:           googleapi.NewTasks,
			Zoom:            newZoomMeetingClient,
			DriveDownload:   driveDownload,
			DriveExport:     driveExportDownload,
			OpenURL:         openPhotosPickerBrowser,
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
	if normalized.Services.AdminDirectory == nil {
		normalized.Services.AdminDirectory = defaults.Services.AdminDirectory
	}
	if normalized.Services.AdminOrgUnit == nil {
		normalized.Services.AdminOrgUnit = defaults.Services.AdminOrgUnit
	}
	if normalized.Services.AnalyticsAdmin == nil {
		normalized.Services.AnalyticsAdmin = defaults.Services.AnalyticsAdmin
	}
	if normalized.Services.AnalyticsData == nil {
		normalized.Services.AnalyticsData = defaults.Services.AnalyticsData
	}
	if normalized.Services.Calendar == nil {
		normalized.Services.Calendar = defaults.Services.Calendar
	}
	if normalized.Services.Chat == nil {
		normalized.Services.Chat = defaults.Services.Chat
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
	if normalized.Services.Forms == nil {
		normalized.Services.Forms = defaults.Services.Forms
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
	if normalized.Services.Photos == nil {
		normalized.Services.Photos = defaults.Services.Photos
	}
	if normalized.Services.PhotosPicker == nil {
		normalized.Services.PhotosPicker = defaults.Services.PhotosPicker
	}
	if normalized.Services.SearchConsole == nil {
		normalized.Services.SearchConsole = defaults.Services.SearchConsole
	}
	if normalized.Services.Sheets == nil {
		normalized.Services.Sheets = defaults.Services.Sheets
	}
	if normalized.Services.Slides == nil {
		normalized.Services.Slides = defaults.Services.Slides
	}
	if normalized.Services.Tasks == nil {
		normalized.Services.Tasks = defaults.Services.Tasks
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
	if normalized.Services.OpenURL == nil {
		normalized.Services.OpenURL = defaults.Services.OpenURL
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

func adminDirectoryService(ctx context.Context, account string) (*admin.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.AdminDirectory != nil {
		return runtime.Services.AdminDirectory(ctx, account)
	}
	return googleapi.NewAdminDirectory(ctx, account)
}

func adminOrgUnitDirectoryService(ctx context.Context, account string) (*admin.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.AdminOrgUnit != nil {
		return runtime.Services.AdminOrgUnit(ctx, account)
	}
	return googleapi.NewAdminDirectoryOrgUnit(ctx, account)
}

func analyticsAdminService(ctx context.Context, account string) (*analyticsadmin.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.AnalyticsAdmin != nil {
		return runtime.Services.AnalyticsAdmin(ctx, account)
	}
	return googleapi.NewAnalyticsAdmin(ctx, account)
}

func analyticsDataService(ctx context.Context, account string) (*analyticsdata.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.AnalyticsData != nil {
		return runtime.Services.AnalyticsData(ctx, account)
	}
	return googleapi.NewAnalyticsData(ctx, account)
}

func calendarService(ctx context.Context, account string) (*calendar.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Calendar != nil {
		return runtime.Services.Calendar(ctx, account)
	}
	return googleapi.NewCalendar(ctx, account)
}

func chatService(ctx context.Context, account string) (*chat.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Chat != nil {
		return runtime.Services.Chat(ctx, account)
	}
	return googleapi.NewChat(ctx, account)
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
	return googleapi.NewCloudIdentityGroups(ctx, account)
}

func photosService(ctx context.Context, account string) (*googleapi.PhotosClient, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Photos != nil {
		return runtime.Services.Photos(ctx, account)
	}
	return newPhotosClient(ctx, account)
}

func photosPickerService(ctx context.Context, account string) (*googleapi.PhotosPickerClient, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.PhotosPicker != nil {
		return runtime.Services.PhotosPicker(ctx, account)
	}
	return newPhotosPickerClient(ctx, account)
}

func openURL(ctx context.Context, uri string) error {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.OpenURL != nil {
		return runtime.Services.OpenURL(ctx, uri)
	}
	return openPhotosPickerBrowser(ctx, uri)
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

func formsService(ctx context.Context, account string) (*formsapi.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Forms != nil {
		return runtime.Services.Forms(ctx, account)
	}
	return googleapi.NewForms(ctx, account)
}

func searchConsoleService(ctx context.Context, account string) (*searchconsoleapi.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.SearchConsole != nil {
		return runtime.Services.SearchConsole(ctx, account)
	}
	return googleapi.NewSearchConsole(ctx, account)
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
	return googleapi.NewPeopleContacts(ctx, account)
}

func peopleDirectoryService(ctx context.Context, account string) (*people.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.PeopleDirectory != nil {
		return runtime.Services.PeopleDirectory(ctx, account)
	}
	return googleapi.NewPeopleDirectory(ctx, account)
}

func peopleOtherContactsService(ctx context.Context, account string) (*people.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.PeopleOther != nil {
		return runtime.Services.PeopleOther(ctx, account)
	}
	return googleapi.NewPeopleOtherContacts(ctx, account)
}

func sheetsService(ctx context.Context, account string) (*sheets.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Sheets != nil {
		return runtime.Services.Sheets(ctx, account)
	}
	return googleapi.NewSheets(ctx, account)
}

func tasksService(ctx context.Context, account string) (*tasks.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Tasks != nil {
		return runtime.Services.Tasks(ctx, account)
	}
	return googleapi.NewTasks(ctx, account)
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
