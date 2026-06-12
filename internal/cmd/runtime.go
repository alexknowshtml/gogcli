package cmd

import (
	"context"
	"io"
	"net/http"
	"os"

	"google.golang.org/api/drive/v3"

	"github.com/steipete/gogcli/internal/app"
	"github.com/steipete/gogcli/internal/googleapi"
)

// Mutable until legacy command tests inject services through Runtime.
var newDriveService = googleapi.NewDrive

func newDefaultRuntime() *app.Runtime {
	return &app.Runtime{
		IO: app.IO{
			In:  os.Stdin,
			Out: os.Stdout,
			Err: os.Stderr,
		},
		Services: app.Services{
			Drive:         newDriveService,
			DriveDownload: driveDownload,
			DriveExport:   driveExportDownload,
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
	if normalized.Services.Drive == nil {
		normalized.Services.Drive = defaults.Services.Drive
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

func driveService(ctx context.Context, account string) (*drive.Service, error) {
	if runtime, ok := app.FromContext(ctx); ok && runtime.Services.Drive != nil {
		return runtime.Services.Drive(ctx, account)
	}
	return newDriveService(ctx, account)
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
