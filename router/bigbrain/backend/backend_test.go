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

func TestActuation_SendsTokenAndObservedLease(t *testing.T) {
	t.Parallel()
	var token, path, query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token = r.Header.Get(tokenHeader)
		path, query = r.URL.Path, r.URL.RawQuery
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := backend.New(map[string]string{"dev": "s3cr3t"})
	code, err := c.Promote(context.Background(), "dev", srv.URL)
	if err != nil || code != http.StatusAccepted {
		t.Fatalf("promote: code=%d err=%v", code, err)
	}
	if token != "s3cr3t" || path != "/instance/promote" || query != "" {
		t.Fatalf("promote request: token=%q path=%q query=%q", token, path, query)
	}
	lease := uint64(150)
	if _, err = c.Demote(context.Background(), "dev", srv.URL, &lease); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if path != "/instance/demote" || query != "lease_ts=150" {
		t.Fatalf("demote must name the observed lease: path=%q query=%q", path, query)
	}
	if _, err = c.Demote(context.Background(), "dev", srv.URL, nil); err != nil || query != "" {
		t.Fatalf("demote without an observed lease must be unconditional: query=%q err=%v", query, err)
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
