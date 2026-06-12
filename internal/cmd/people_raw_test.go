package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/api/people/v1"
)

func newPeopleRawTestServer(t *testing.T, status int, body map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/people/") || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		if status != 0 {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": status, "message": "mock error"},
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
}

func withMockPeopleContactsService(t *testing.T, ctx context.Context, srv *httptest.Server) context.Context {
	t.Helper()
	svc := newGoogleTestServiceWithEndpoint(t, srv.Client(), srv.URL+"/", people.NewService)
	return withPeopleContactsTestService(ctx, svc)
}

func fullPersonResponse() map[string]any {
	return map[string]any{
		"resourceName": "people/c1",
		"etag":         "abc",
		"names": []map[string]any{
			{"displayName": "Ada Lovelace", "givenName": "Ada", "familyName": "Lovelace"},
		},
		"emailAddresses": []map[string]any{
			{"value": "ada@example.com"},
		},
	}
}

func TestPeopleRaw_HappyPath(t *testing.T) {
	srv := newPeopleRawTestServer(t, 0, fullPersonResponse())
	defer srv.Close()

	ctx := withMockPeopleContactsService(t, rawTestContext(t), srv)
	flags := &RootFlags{Account: "a@b.com"}
	out := captureStdout(t, func() {
		if err := runKong(t, &PeopleRawCmd{}, []string{"people/c1"}, ctx, flags); err != nil {
			t.Fatalf("run: %v", err)
		}
	})

	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, out)
	}
	if got["resourceName"] != "people/c1" {
		t.Fatalf("expected resourceName=people/c1, got: %v", got["resourceName"])
	}
	if _, ok := got["names"]; !ok {
		t.Fatalf("expected names in raw output")
	}
}

func TestContactsRaw_HappyPath(t *testing.T) {
	srv := newPeopleRawTestServer(t, 0, fullPersonResponse())
	defer srv.Close()

	ctx := withMockPeopleContactsService(t, rawTestContext(t), srv)
	flags := &RootFlags{Account: "a@b.com"}
	out := captureStdout(t, func() {
		if err := runKong(t, &ContactsRawCmd{}, []string{"people/c1"}, ctx, flags); err != nil {
			t.Fatalf("run: %v", err)
		}
	})

	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, out)
	}
	if got["resourceName"] != "people/c1" {
		t.Fatalf("expected resourceName=people/c1, got: %v", got["resourceName"])
	}
}

func TestContactsRaw_EmailResolvesContactResource(t *testing.T) {
	var gotGet bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/people/me/connections") && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"connections": []map[string]any{
					{
						"resourceName":   "people/c1",
						"emailAddresses": []map[string]any{{"value": "ada@example.com"}},
					},
				},
			})
		case strings.Contains(r.URL.Path, "/people/c1") && r.Method == http.MethodGet:
			gotGet = true
			_ = json.NewEncoder(w).Encode(fullPersonResponse())
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx := withMockPeopleContactsService(t, rawTestContext(t), srv)
	flags := &RootFlags{Account: "a@b.com"}
	out := captureStdout(t, func() {
		if err := runKong(t, &ContactsRawCmd{}, []string{"ada@example.com"}, ctx, flags); err != nil {
			t.Fatalf("run: %v", err)
		}
	})

	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, out)
	}
	if got["resourceName"] != "people/c1" {
		t.Fatalf("expected resourceName=people/c1, got: %v", got["resourceName"])
	}
	if !gotGet {
		t.Fatalf("expected People.Get for resolved contact resource")
	}
}

func TestContactsRaw_EmailAmbiguousContactsFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/people/me/connections") && r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"connections": []map[string]any{
					{
						"resourceName":   "people/c1",
						"emailAddresses": []map[string]any{{"value": "ada@example.com"}},
					},
					{
						"resourceName":   "people/c2",
						"emailAddresses": []map[string]any{{"value": "ada@example.com"}},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx := withMockPeopleContactsService(t, rawTestContext(t), srv)
	flags := &RootFlags{Account: "a@b.com"}
	_ = captureStdout(t, func() {
		if err := runKong(t, &ContactsRawCmd{}, []string{"ada@example.com"}, ctx, flags); err != nil {
			if !strings.Contains(err.Error(), "matched multiple contacts") {
				t.Fatalf("unexpected error: %v", err)
			}
			return
		}
		t.Fatalf("expected ambiguous contact error")
	})
}

func TestPeopleRaw_APIError(t *testing.T) {
	srv := newPeopleRawTestServer(t, http.StatusInternalServerError, nil)
	defer srv.Close()

	ctx := withMockPeopleContactsService(t, rawTestContext(t), srv)
	flags := &RootFlags{Account: "a@b.com"}
	_ = captureStdout(t, func() {
		if err := runKong(t, &PeopleRawCmd{}, []string{"people/c1"}, ctx, flags); err == nil {
			t.Fatalf("expected error on 500")
		}
	})
}

func TestPeopleRaw_EmptyID(t *testing.T) {
	ctx := rawTestContext(t)
	flags := &RootFlags{Account: "a@b.com"}
	if err := (&PeopleRawCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected error on empty id")
	}
}

func TestContactsRaw_EmptyID(t *testing.T) {
	ctx := rawTestContext(t)
	flags := &RootFlags{Account: "a@b.com"}
	if err := (&ContactsRawCmd{}).Run(ctx, flags); err == nil {
		t.Fatalf("expected error on empty id")
	}
}
