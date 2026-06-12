package cmd

import (
	"context"
	"net/http/httptest"
	"testing"

	"google.golang.org/api/people/v1"

	"github.com/steipete/gogcli/internal/app"
)

func newPeopleServiceFromServer(t *testing.T, srv *httptest.Server) *people.Service {
	t.Helper()
	return newGoogleTestServiceWithEndpoint(t, srv.Client(), srv.URL+"/", people.NewService)
}

func withPeopleContactsTestService(ctx context.Context, svc *people.Service) context.Context {
	return withPeopleTestServices(ctx, peopleTestServices{
		Contacts: fixedPeopleTestService(svc),
	})
}

type peopleTestServices struct {
	Contacts  app.PeopleServiceFactory
	Directory app.PeopleServiceFactory
	Other     app.PeopleServiceFactory
}

func fixedPeopleTestService(svc *people.Service) app.PeopleServiceFactory {
	return func(context.Context, string) (*people.Service, error) {
		return svc, nil
	}
}

func withPeopleTestServices(ctx context.Context, services peopleTestServices) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	runtime := &app.Runtime{}
	if existing, ok := app.FromContext(ctx); ok {
		*runtime = *existing
	}
	if services.Contacts != nil {
		runtime.Services.PeopleContacts = services.Contacts
	}
	if services.Directory != nil {
		runtime.Services.PeopleDirectory = services.Directory
	}
	if services.Other != nil {
		runtime.Services.PeopleOther = services.Other
	}
	return app.WithRuntime(ctx, runtime)
}

func executeWithPeopleTestServices(t *testing.T, args []string, services peopleTestServices) executeTestResult {
	t.Helper()
	return executeWithTestRuntime(t, args, &app.Runtime{Services: app.Services{
		PeopleContacts:  services.Contacts,
		PeopleDirectory: services.Directory,
		PeopleOther:     services.Other,
	}})
}
