package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	admin "google.golang.org/api/admin/directory/v1"
	analyticsadmin "google.golang.org/api/analyticsadmin/v1beta"
	analyticsdata "google.golang.org/api/analyticsdata/v1beta"
	"google.golang.org/api/chat/v1"
	"google.golang.org/api/cloudidentity/v1"
	"google.golang.org/api/drive/v3"
	formsapi "google.golang.org/api/forms/v1"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/people/v1"
	searchconsoleapi "google.golang.org/api/searchconsole/v1"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/slides/v1"

	"github.com/steipete/gogcli/internal/app"
	"github.com/steipete/gogcli/internal/googleapi"
)

func TestExecuteRuntimeRoutesMigratedCommandOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	runtime := &app.Runtime{IO: app.IO{
		In:  strings.NewReader(""),
		Out: &stdout,
		Err: &stderr,
	}}

	if err := executeWithRuntime([]string{"--json", "version"}, runtime); err != nil {
		t.Fatalf("executeWithRuntime() error = %v, stderr = %q", err, stderr.String())
	}

	var got map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not JSON: %v\nstdout=%q", err, stdout.String())
	}
	if got["version"] == "" {
		t.Fatalf("stdout = %#v, want version", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestExecuteRuntimeRoutesEarlyErrors(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	runtime := &app.Runtime{IO: app.IO{
		In:  strings.NewReader(""),
		Out: &stdout,
		Err: &stderr,
	}}

	err := executeWithRuntime([]string{"--json", "--plain", "version"}, runtime)
	if err == nil || ExitCode(err) != 2 {
		t.Fatalf("executeWithRuntime() error = %v, want exit code 2", err)
	}
	if !strings.Contains(stderr.String(), "cannot combine --json and --plain") {
		t.Fatalf("stderr = %q, want output mode error", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestAdminDirectoryServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &admin.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		AdminDirectory: func(_ context.Context, account string) (*admin.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := adminDirectoryService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("adminDirectoryService() error = %v", err)
	}
	if got != want {
		t.Fatalf("adminDirectoryService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestAdminOrgUnitDirectoryServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &admin.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		AdminOrgUnit: func(_ context.Context, account string) (*admin.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := adminOrgUnitDirectoryService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("adminOrgUnitDirectoryService() error = %v", err)
	}
	if got != want {
		t.Fatalf("adminOrgUnitDirectoryService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestCloudIdentityServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &cloudidentity.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		CloudIdentity: func(_ context.Context, account string) (*cloudidentity.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := cloudIdentityService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("cloudIdentityService() error = %v", err)
	}
	if got != want {
		t.Fatalf("cloudIdentityService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestPhotosServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &googleapi.PhotosClient{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		Photos: func(_ context.Context, account string) (*googleapi.PhotosClient, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := photosService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("photosService() error = %v", err)
	}
	if got != want {
		t.Fatalf("photosService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestPhotosPickerServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &googleapi.PhotosPickerClient{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		PhotosPicker: func(_ context.Context, account string) (*googleapi.PhotosPickerClient, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := photosPickerService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("photosPickerService() error = %v", err)
	}
	if got != want {
		t.Fatalf("photosPickerService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestOpenURLUsesRuntimeOperation(t *testing.T) {
	t.Parallel()

	var gotURI string
	runtime := &app.Runtime{Services: app.Services{
		OpenURL: func(_ context.Context, uri string) error {
			gotURI = uri
			return nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	if err := openURL(ctx, "https://example.com/picker"); err != nil {
		t.Fatalf("openURL() error = %v", err)
	}
	if gotURI != "https://example.com/picker" {
		t.Fatalf("URI = %q, want picker URI", gotURI)
	}
}

func TestAnalyticsAdminServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &analyticsadmin.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		AnalyticsAdmin: func(_ context.Context, account string) (*analyticsadmin.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := analyticsAdminService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("analyticsAdminService() error = %v", err)
	}
	if got != want {
		t.Fatalf("analyticsAdminService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestAnalyticsDataServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &analyticsdata.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		AnalyticsData: func(_ context.Context, account string) (*analyticsdata.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := analyticsDataService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("analyticsDataService() error = %v", err)
	}
	if got != want {
		t.Fatalf("analyticsDataService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestSearchConsoleServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &searchconsoleapi.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		SearchConsole: func(_ context.Context, account string) (*searchconsoleapi.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := searchConsoleService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("searchConsoleService() error = %v", err)
	}
	if got != want {
		t.Fatalf("searchConsoleService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestDriveServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &drive.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		Drive: func(_ context.Context, account string) (*drive.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := driveService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("driveService() error = %v", err)
	}
	if got != want {
		t.Fatalf("driveService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestChatServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &chat.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		Chat: func(_ context.Context, account string) (*chat.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := chatService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("chatService() error = %v", err)
	}
	if got != want {
		t.Fatalf("chatService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestFormsServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &formsapi.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		Forms: func(_ context.Context, account string) (*formsapi.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := formsService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("formsService() error = %v", err)
	}
	if got != want {
		t.Fatalf("formsService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestSlidesServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &slides.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		Slides: func(_ context.Context, account string) (*slides.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := slidesService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("slidesService() error = %v", err)
	}
	if got != want {
		t.Fatalf("slidesService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestRequireGmailServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &gmail.Service{}
	var factoryAccount string
	runtime := &app.Runtime{Services: app.Services{
		Gmail: func(_ context.Context, account string) (*gmail.Service, error) {
			factoryAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	gotAccount, got, err := requireGmailService(ctx, &RootFlags{Account: "test@example.com"})
	if err != nil {
		t.Fatalf("requireGmailService() error = %v", err)
	}
	if got != want {
		t.Fatalf("requireGmailService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("required account = %q, want test@example.com", gotAccount)
	}
	if factoryAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", factoryAccount)
	}
}

func TestPeopleContactsServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &people.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		PeopleContacts: func(_ context.Context, account string) (*people.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := peopleContactsService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("peopleContactsService() error = %v", err)
	}
	if got != want {
		t.Fatalf("peopleContactsService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestSheetsServiceUsesRuntimeFactory(t *testing.T) {
	t.Parallel()

	want := &sheets.Service{}
	var gotAccount string
	runtime := &app.Runtime{Services: app.Services{
		Sheets: func(_ context.Context, account string) (*sheets.Service, error) {
			gotAccount = account
			return want, nil
		},
	}}
	ctx := app.WithRuntime(context.Background(), runtime)

	got, err := sheetsService(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sheetsService() error = %v", err)
	}
	if got != want {
		t.Fatalf("sheetsService() = %p, want %p", got, want)
	}
	if gotAccount != "test@example.com" {
		t.Fatalf("factory account = %q, want test@example.com", gotAccount)
	}
}

func TestDriveDownloadOperationsUseRuntimeServices(t *testing.T) {
	t.Parallel()

	svc := &drive.Service{}
	downloadResponse := &http.Response{Body: io.NopCloser(strings.NewReader(""))}
	exportResponse := &http.Response{Body: io.NopCloser(strings.NewReader(""))}
	var gotDownloadID string
	var gotExportID string
	var gotExportMIME string
	ctx := app.WithRuntime(context.Background(), &app.Runtime{Services: app.Services{
		DriveDownload: func(_ context.Context, gotSvc *drive.Service, fileID string) (*http.Response, error) {
			if gotSvc != svc {
				t.Fatalf("download service = %p, want %p", gotSvc, svc)
			}
			gotDownloadID = fileID
			return downloadResponse, nil
		},
		DriveExport: func(_ context.Context, gotSvc *drive.Service, fileID, mimeType string) (*http.Response, error) {
			if gotSvc != svc {
				t.Fatalf("export service = %p, want %p", gotSvc, svc)
			}
			gotExportID = fileID
			gotExportMIME = mimeType
			return exportResponse, nil
		},
	}})

	gotDownload, err := driveDownloadRequest(ctx, svc, "download-id")
	t.Cleanup(func() { _ = gotDownload.Body.Close() })
	if err != nil || gotDownload != downloadResponse || gotDownloadID != "download-id" {
		t.Fatalf("driveDownloadRequest() = (%p, %v, %q)", gotDownload, err, gotDownloadID)
	}
	gotExport, err := driveExportRequest(ctx, svc, "export-id", "application/pdf")
	t.Cleanup(func() { _ = gotExport.Body.Close() })
	if err != nil || gotExport != exportResponse || gotExportID != "export-id" || gotExportMIME != "application/pdf" {
		t.Fatalf("driveExportRequest() = (%p, %v, %q, %q)", gotExport, err, gotExportID, gotExportMIME)
	}
}

func TestCommandIOMergesPartialRuntime(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	ctx := app.WithRuntime(context.Background(), &app.Runtime{
		IO: app.IO{Out: &stdout},
	})

	got := commandIO(ctx)
	if got.Out != &stdout {
		t.Fatalf("stdout = %T, want injected buffer", got.Out)
	}
	if got.In == nil || got.Err == nil {
		t.Fatalf("commandIO() = %#v, want default input and stderr", got)
	}
}
