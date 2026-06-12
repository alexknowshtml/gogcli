package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/api/drive/v3"

	"github.com/steipete/gogcli/internal/app"
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
