package backend_test

import (
	"context"
	"io"
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
	c := backend.New(map[string]string{"dev": "s3cr3t"})
	code, err := c.Promote(context.Background(), "dev", srv.URL)
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
	c := backend.New(nil)
	if _, err := c.Demote(context.Background(), "dev", srv.URL); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if seen {
		t.Fatalf("empty token must not send the %s header", tokenHeader)
	}
}

func TestLeadership_DecodesLeaseAndNullLease(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		body     string
		isLeader bool
		hasLease bool
	}{
		{`{"is_leader":true,"latest_ts":42,"lease_ts":99}`, true, true},
		{`{"is_leader":true,"latest_ts":43,"lease_ts":null}`, true, false},
		{`{"is_leader":false,"latest_ts":44,"lease_ts":null}`, false, false},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, tc.body)
		}))
		l, err := backend.New(nil).Leadership(context.Background(), "dev", srv.URL)
		srv.Close()
		if err != nil {
			t.Fatalf("Leadership: %v", err)
		}
		if l.LatestTS == 0 {
			t.Fatalf("latest_ts not decoded from %s", tc.body)
		}
		if l.IsLeader != tc.isLeader || (l.LeaseTS != nil) != tc.hasLease {
			t.Fatalf("unexpected leader/lease combination from %s: %+v", tc.body, l)
		}
	}
}

func TestLeadership_RejectsNonOKAndInvalidJSON(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		code int
		body string
	}{
		{http.StatusForbidden, `{}`},
		{http.StatusOK, `{`},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.code)
			_, _ = io.WriteString(w, tc.body)
		}))
		_, err := backend.New(nil).Leadership(context.Background(), "dev", srv.URL)
		srv.Close()
		if err == nil {
			t.Fatalf("code=%d body=%q: expected error", tc.code, tc.body)
		}
	}
}
