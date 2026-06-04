package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Leadership struct {
	Role     string  `json:"role"`
	IsLeader bool    `json:"is_leader"`
	LatestTS uint64  `json:"latest_ts"`
	LeaseTS  *uint64 `json:"lease_ts"`
}

const (
	maxBodyBytes            = 4096
	controlPlaneTokenHeader = "X-Convex-Control-Plane-Token" //nolint:gosec // false positive
)

type Client struct {
	http  *http.Client
	token string
}

func New(token string) *Client {
	return &Client{http: &http.Client{}, token: token}
}

func (c *Client) Leadership(ctx context.Context, base string) (Leadership, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/instance/leadership", nil)
	if err != nil {
		return Leadership{}, err
	}
	resp, err := c.http.Do(req) //nolint:bodyclose // closed via drainClose
	if err != nil {
		return Leadership{}, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return Leadership{}, fmt.Errorf("leadership %s: status %d", base, resp.StatusCode)
	}
	var l Leadership
	if derr := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&l); derr != nil {
		return Leadership{}, fmt.Errorf("leadership %s: decode: %w", base, derr)
	}
	return l, nil
}

func (c *Client) Promote(ctx context.Context, base string) (int, error) {
	return c.post(ctx, base+"/instance/promote")
}

func (c *Client) Demote(ctx context.Context, base string) (int, error) {
	return c.post(ctx, base+"/instance/demote")
}

func (c *Client) post(ctx context.Context, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return 0, err
	}
	if c.token != "" {
		req.Header.Set(controlPlaneTokenHeader, c.token)
	}
	resp, err := c.http.Do(req) //nolint:bodyclose // closed via drainClose
	if err != nil {
		return 0, err
	}
	defer drainClose(resp.Body)
	return resp.StatusCode, nil
}

func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, maxBodyBytes))
	_ = rc.Close()
}
