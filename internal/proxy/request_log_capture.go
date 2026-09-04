package proxy

import (
	"bytes"
	"io"
	"net/http"
	"sync/atomic"
)

// captureRequestHeaders serializes headers to canonical wire format for
// request-log payload capture.
func captureRequestHeaders(header http.Header) []byte {
	if header == nil {
		return nil
	}
	var buf bytes.Buffer
	_ = header.Clone().Write(&buf)
	return buf.Bytes()
}

// headerWireLen returns canonical wire-format header bytes length.
func headerWireLen(header http.Header) int64 {
	if header == nil || len(header) == 0 {
		return 0
	}
	var buf bytes.Buffer
	_ = header.Write(&buf)
	return int64(buf.Len())
}

func captureHeadersWithLimit(header http.Header, maxBytes int) ([]byte, int, bool) {
	payload := captureRequestHeaders(header)
	totalLen := len(payload)
	if totalLen == 0 {
		return nil, 0, false
	}
	if maxBytes >= 0 && totalLen > maxBytes {
		return payload[:maxBytes], totalLen, true
	}
	return payload, totalLen, false
}

type payloadCaptureReadCloser struct {
	rc       io.ReadCloser
	maxBytes int
	payload  bytes.Buffer
	totalLen int
}

func newPayloadCaptureReadCloser(rc io.ReadCloser, maxBytes int) *payloadCaptureReadCloser {
	return &payloadCaptureReadCloser{
		rc:       rc,
		maxBytes: maxBytes,
	}
}

func (c *payloadCaptureReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.totalLen += n
		if c.maxBytes < 0 {
			_, _ = c.payload.Write(p[:n])
		} else {
			remaining := c.maxBytes - c.payload.Len()
			if remaining > 0 {
				if n <= remaining {
					_, _ = c.payload.Write(p[:n])
				} else {
					_, _ = c.payload.Write(p[:remaining])
				}
			}
		}
	}
	return n, err
}

func (c *payloadCaptureReadCloser) Close() error {
	return c.rc.Close()
}

func (c *payloadCaptureReadCloser) Payload() []byte {
	return c.payload.Bytes()
}

func (c *payloadCaptureReadCloser) TotalLen() int {
	return c.totalLen
}

func (c *payloadCaptureReadCloser) Truncated() bool {
	return c.totalLen > c.payload.Len()
}

// countingReadCloser wraps a body stream and records total read bytes.
type countingReadCloser struct {
	rc    io.ReadCloser
	total int64
	// onReadErr, when set, is called once with the first non-EOF read error.
	//
	// A mid-body failure cannot be recorded after the fact: httputil.ReverseProxy
	// reports it by panicking with http.ErrAbortHandler, which skips every
	// statement following its ServeHTTP call. The failure therefore has to be
	// captured at the moment the read fails.
	onReadErr func(error)
	notified  atomic.Bool
}

func newCountingReadCloser(rc io.ReadCloser) *countingReadCloser {
	return &countingReadCloser{rc: rc}
}

// newCountingReadCloserWithErrHook counts bytes and reports the first read
// error through onReadErr. io.EOF is a normal end of body, not an error.
func newCountingReadCloserWithErrHook(
	rc io.ReadCloser,
	onReadErr func(error),
) *countingReadCloser {
	return &countingReadCloser{rc: rc, onReadErr: onReadErr}
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 {
		c.total += int64(n)
	}
	if err != nil && err != io.EOF && c.onReadErr != nil && c.notified.CompareAndSwap(false, true) {
		c.onReadErr(err)
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rc.Close()
}

func (c *countingReadCloser) Total() int64 {
	return c.total
}

// countingReadWriteCloser wraps a bidirectional stream and records
// bytes read/written independently.
type countingReadWriteCloser struct {
	rwc        io.ReadWriteCloser
	totalRead  int64
	totalWrite int64
}

func newCountingReadWriteCloser(rwc io.ReadWriteCloser) *countingReadWriteCloser {
	return &countingReadWriteCloser{rwc: rwc}
}

func (c *countingReadWriteCloser) Read(p []byte) (int, error) {
	n, err := c.rwc.Read(p)
	if n > 0 {
		c.totalRead += int64(n)
	}
	return n, err
}

func (c *countingReadWriteCloser) Write(p []byte) (int, error) {
	n, err := c.rwc.Write(p)
	if n > 0 {
		c.totalWrite += int64(n)
	}
	return n, err
}

func (c *countingReadWriteCloser) Close() error {
	return c.rwc.Close()
}

func (c *countingReadWriteCloser) TotalRead() int64 {
	return c.totalRead
}

func (c *countingReadWriteCloser) TotalWrite() int64 {
	return c.totalWrite
}
