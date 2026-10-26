package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
)

type Leadership struct {
	Role                string  `json:"role"`
	IsLeader            bool    `json:"is_leader"`
	LatestTS            uint64  `json:"latest_ts"`
	LeaseTS             *uint64 `json:"lease_ts"`
	LeaseUnverifiedSecs *uint64 `json:"lease_unverified_secs"`
	TailerHealthy       bool    `json:"tailer_healthy"`
}

const (
	maxBodyBytes            = 4096
	controlPlaneTokenHeader = "X-Convex-Control-Plane-Token" //nolint:gosec // HTTP header name, not a credential
)

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

type Client struct {
	http   *http.Client
	dial   dialFunc
	tokens map[string]string
}

func New(tokens map[string]string) *Client {
	return &Client{
		http:   &http.Client{CheckRedirect: noRedirect},
		dial:   (&net.Dialer{}).DialContext,
		tokens: tokens,
	}
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func (c *Client) Leadership(ctx context.Context, base string) (Leadership, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/instance/leadership", nil)
	if err != nil {
		return Leadership{}, err
	}
	status, body, err := do(c.http, req)
	if err != nil {
		return Leadership{}, err
	}
	if status != http.StatusOK {
		return Leadership{}, fmt.Errorf("leadership %s: status %d: %s", base, status, bytes.TrimSpace(body))
	}
	var l Leadership
	if derr := json.Unmarshal(body, &l); derr != nil {
		return Leadership{}, fmt.Errorf("leadership %s: decode: %w", base, derr)
	}
	return l, nil
}

func (c *Client) Promote(ctx context.Context, deployment, base string) (int, bool, error) {
	return c.post(ctx, deployment, base+"/instance/promote")
}

func (c *Client) Demote(ctx context.Context, deployment, base string, leaseTS *uint64) (int, error) {
	url := base + "/instance/demote"
	if leaseTS != nil {
		url += "?lease_ts=" + strconv.FormatUint(*leaseTS, 10)
	}
	status, _, err := c.post(ctx, deployment, url)
	return status, err
}

func (c *Client) post(ctx context.Context, deployment, url string) (int, bool, error) {
	var written atomic.Bool
	hc := &http.Client{
		CheckRedirect: noRedirect,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := c.dial(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &trackedConn{Conn: conn, written: &written}, nil
			},
		},
	}
	defer hc.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return 0, false, err
	}
	c.setControlPlaneToken(req, deployment)
	status, _, err := do(hc, req)
	if status == 0 {
		return 0, written.Load(), err
	}
	return status, true, nil
}

type trackedConn struct {
	net.Conn

	written *atomic.Bool
}

func (c *trackedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.written.Store(true)
	}
	return n, err
}

func do(hc *http.Client, req *http.Request) (int, []byte, error) {
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func (c *Client) setControlPlaneToken(req *http.Request, deployment string) {
	if token := c.tokens[deployment]; token != "" {
		req.Header.Set(controlPlaneTokenHeader, token)
	}
}
