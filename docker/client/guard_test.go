package client

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const id = "2e303154764f0a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123"

// fakeDocker serves on a unix socket and records every request that reaches it.
type fakeDocker struct {
	mu   sync.Mutex
	seen []string
}

func (f *fakeDocker) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

func startFakeDocker(t *testing.T) (*fakeDocker, *http.Client) {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDocker{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen = append(f.seen, r.Method+" "+r.URL.RequestURI())
		f.mu.Unlock()
		w.Write([]byte("{}"))
	}))
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	return f, New(socket)
}

func send(c *http.Client, method, target string) error {
	req, err := http.NewRequestWithContext(context.Background(), method, "http://docker"+target, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func TestGuard_AllowsReadOnlyRequests(t *testing.T) {
	f, c := startFakeDocker(t)
	targets := []string{
		"/info",
		"/containers/json?all=true",
		"/containers/" + id + "/stats?stream=false&one-shot=true",
		"/containers/" + id + "/json",
	}
	for _, target := range targets {
		if err := send(c, http.MethodGet, target); err != nil {
			t.Errorf("GET %s: expected allowed, got %v", target, err)
		}
	}
	got := f.requests()
	if len(got) != len(targets) {
		t.Fatalf("expected %d requests to reach docker, got %v", len(targets), got)
	}
	for i, target := range targets {
		if got[i] != "GET "+target {
			t.Errorf("expected docker to receive GET %s, got %s", target, got[i])
		}
	}
}

func TestGuard_RefusesEverythingElse(t *testing.T) {
	f, c := startFakeDocker(t)
	cases := []struct{ method, target string }{
		// Anything that changes docker, even on an allowed path
		{http.MethodPost, "/containers/" + id + "/stop"},
		{http.MethodPost, "/containers/" + id + "/restart"},
		{http.MethodPost, "/containers/" + id + "/kill"},
		{http.MethodPost, "/containers/" + id + "/exec"},
		{http.MethodPost, "/containers/create"},
		{http.MethodPost, "/images/create?fromImage=alpine"},
		{http.MethodDelete, "/containers/" + id},
		{http.MethodPut, "/containers/" + id + "/archive?path=/"},
		{http.MethodPost, "/info"},
		{http.MethodPost, "/containers/json?all=true"},
		{http.MethodHead, "/info"},
		// GETs that are not on the list
		{http.MethodGet, "/containers/" + id + "/logs?stdout=true"},
		{http.MethodGet, "/containers/" + id + "/export"},
		{http.MethodGet, "/containers/" + id + "/archive?path=/etc"},
		{http.MethodGet, "/containers/" + id + "/attach/ws"},
		{http.MethodGet, "/events"},
		{http.MethodGet, "/secrets"},
		{http.MethodGet, "/containers/json"},
		{http.MethodGet, "/containers/json?all=true&size=true"},
		{http.MethodGet, "/containers/" + id + "/stats"},
		{http.MethodGet, "/containers/" + id + "/stats?stream=true"},
		{http.MethodGet, "/v1.41/info"},
		// Ids that are not a full 64-hex id
		{http.MethodGet, "/containers/postgres-postgres-1/json"},
		{http.MethodGet, "/containers/2e303154764f/json"},
		{http.MethodGet, "/containers/" + strings.ToUpper(id) + "/json"},
		{http.MethodGet, "/containers/" + id + "/../../images/json"},
		{http.MethodGet, "/containers/" + id + "%2Fstop/json"},
	}
	for _, tc := range cases {
		err := send(c, tc.method, tc.target)
		if err == nil || !strings.Contains(err.Error(), "docker: refused") {
			t.Errorf("%s %s: expected refused, got %v", tc.method, tc.target, err)
		}
	}
	if got := f.requests(); len(got) != 0 {
		t.Fatalf("refused requests must never reach docker, got %v", got)
	}
}
