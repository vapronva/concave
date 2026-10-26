package backend

import (
	"context"
	"io"
	"net"
	"testing"
)

type unwritableConn struct {
	net.Conn
}

func (unwritableConn) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestPost_ZeroByteWriteFailureIsNotSent(t *testing.T) {
	t.Parallel()
	c := New(nil)
	c.dial = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
		return unwritableConn{Conn: client}, nil
	}
	code, sent, err := c.Promote(context.Background(), "dev", "http://backend")
	if err == nil || code != 0 || sent {
		t.Fatalf("a request whose flush failed before any byte left the socket is not sent: code=%d sent=%v err=%v",
			code, sent, err)
	}
}
