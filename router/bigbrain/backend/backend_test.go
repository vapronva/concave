package backend_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"git.horse/vapronva/concave/router/bigbrain/backend"
)

const tokenHeader = "X-Convex-Control-Plane-Token"

func TestPromote_SendsControlPlaneTokenWhenSet(t *testing.T) {
	t.Parallel()
	var got string
	var seen bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get(tokenHeader)
		_, seen = r.Header[tokenHeader]
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := backend.New("s3cr3t")
	code, err := c.Promote(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("want 200, got %d", code)
	}
	if !seen || got != "s3cr3t" {
		t.Fatalf("want %s header = s3cr3t, got %q (present=%v)", tokenHeader, got, seen)
	}
}

func TestDemote_OmitsTokenHeaderWhenEmpty(t *testing.T) {
	t.Parallel()
	var seen bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, seen = r.Header[tokenHeader]
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := backend.New("")
	if _, err := c.Demote(context.Background(), srv.URL); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if seen {
		t.Fatalf("empty token must not send the %s header", tokenHeader)
	}
}
