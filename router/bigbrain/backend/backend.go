package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Leadership struct {
	Role                string  `json:"role"`
	IsLeader            bool    `json:"is_leader"`
	LatestTS            uint64  `json:"latest_ts"`
	LeaseTS             *uint64 `json:"lease_ts"`
	LeaseUnverifiedSecs *uint64 `json:"lease_unverified_secs"`
}

const (
	maxBodyBytes            = 4096
	controlPlaneTokenHeader = "X-Convex-Control-Plane-Token" //nolint:gosec // HTTP header name, not a credential
	clientTimeout           = 10 * time.Second
)

type Client struct {
	poll   *http.Client
	act    *http.Client
	tokens map[string]string
}

func New(tokens map[string]string) *Client {
	transport := http.DefaultTransport
	return &Client{
		poll:   &http.Client{Transport: transport, Timeout: clientTimeout},
		act:    &http.Client{Transport: transport},
		tokens: tokens,
	}
}

func (c *Client) Leadership(ctx context.Context, deployment, base string) (Leadership, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/instance/leadership", nil)
	if err != nil {
		return Leadership{}, err
	}
	c.setControlPlaneToken(req, deployment)
	status, body, err := do(c.poll, req)
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

func (c *Client) Promote(ctx context.Context, deployment, base string) (int, error) {
	return c.post(ctx, deployment, base+"/instance/promote")
}

func (c *Client) Demote(ctx context.Context, deployment, base string) (int, error) {
	return c.post(ctx, deployment, base+"/instance/demote")
}

func (c *Client) post(ctx context.Context, deployment, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return 0, err
	}
	c.setControlPlaneToken(req, deployment)
	status, _, err := do(c.act, req)
	return status, err
}

func do(hc *http.Client, req *http.Request) (int, []byte, error) {
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func (c *Client) setControlPlaneToken(req *http.Request, deployment string) {
	if token := c.tokens[deployment]; token != "" {
		req.Header.Set(controlPlaneTokenHeader, token)
	}
}
