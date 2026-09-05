package proxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// Recheck of the tunnel half-close regression.
//
// A client that shuts down only its write direction (shutdown(WR), i.e.
// CloseWrite) still expects to read the response. To honour that, the tunnel
// pump calls closeWriteConn on the upstream connection when the client->upstream
// copy ends: every wrapper in the outbound chain must forward CloseWrite, or the
// pump falls back to closing both directions and the response is lost.
//
// The chains below mirror what production actually builds:
//
//	node CONNECT : tlsLatencyConn(countingConn(dialCancelConn(raw)))
//	transport    : tlsLatencyConn(countingConn(dialObserverConn(raw)))
//	bypass       : countingConn(raw)

const (
	recheckRequest  = "GET /payload HTTP/1.1\r\n\r\n"
	recheckResponse = "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\npayload"
)

// halfCloseEchoServer accepts one connection, reads requestLen bytes, then
// answers and closes. It reports how many bytes it managed to read.
func halfCloseEchoServer(t *testing.T, ln net.Listener, requestLen int, response string) chan int {
	t.Helper()
	read := make(chan int, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			read <- -1
			return
		}
		defer conn.Close()
		buf := make([]byte, requestLen)
		n, err := io.ReadFull(conn, buf)
		if err != nil {
			read <- n
			return
		}
		// Deliberate gap: the reply lands after the client has already
		// half-closed, which is exactly the "response lost" window.
		time.Sleep(50 * time.Millisecond)
		if _, err := conn.Write([]byte(response)); err != nil {
			read <- n
			return
		}
		read <- n
	}()
	return read
}

// runHalfCloseTunnelAgainst drives pumpPreparedTunnelReader over a real TCP
// client pair with upstreamConn as the proxy-side outbound connection, and
// returns what the client managed to read.
func runHalfCloseTunnelAgainst(t *testing.T, upstreamConn net.Conn, serverRead chan int) (string, tunnelRelayResult) {
	t.Helper()

	clientLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen client: %v", err)
	}
	defer clientLn.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := clientLn.Accept()
		accepted <- conn
	}()

	clientRaw, err := net.Dial("tcp", clientLn.Addr().String())
	if err != nil {
		t.Fatalf("dial client: %v", err)
	}
	clientTCP, ok := clientRaw.(*net.TCPConn)
	if !ok {
		t.Fatalf("client conn type: got %T, want *net.TCPConn", clientRaw)
	}
	defer clientTCP.Close()

	proxyClientConn := <-accepted
	if proxyClientConn == nil {
		t.Fatal("client accept failed")
	}

	resultCh := make(chan tunnelRelayResult, 1)
	go func() {
		// connCloseNotifier is what countingListener.Accept hands the proxy for
		// the inbound side, so keep it in the chain.
		resultCh <- pumpPreparedTunnelReader(
			&connCloseNotifier{Conn: proxyClientConn, sink: newCountingConnTestSink()},
			proxyClientConn,
			&preparedTunnel{upstreamConn: upstreamConn, recordResult: func(bool) {}},
			tunnelPumpOptions{requireBidirectionalTraffic: true},
		)
	}()

	if _, err := clientTCP.Write([]byte(recheckRequest)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := clientTCP.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
	if err := clientTCP.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	got, readErr := io.ReadAll(clientTCP)

	var result tunnelRelayResult
	select {
	case result = <-resultCh:
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel pump did not finish")
	}

	select {
	case n := <-serverRead:
		if n != len(recheckRequest) {
			t.Fatalf("upstream read: got %d, want %d", n, len(recheckRequest))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream server did not finish")
	}

	if readErr != nil {
		t.Fatalf("client read: %v", readErr)
	}
	return string(got), result
}

// Node tunnel chain: dialCancelConn sits at the bottom, exactly as
// dialTunnelConn returns it.
func TestRecheck_NodeTunnelSurvivesClientHalfClose(t *testing.T) {
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer upstreamLn.Close()
	serverRead := halfCloseEchoServer(t, upstreamLn, len(recheckRequest), recheckResponse)

	raw, err := net.Dial("tcp", upstreamLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstreamConn := newTLSLatencyConn(
		newCountingConn(
			&dialCancelConn{Conn: raw, cancel: cancel},
			newCountingConnTestSink(),
		),
		nil,
	)
	_ = cancelCtx

	got, result := runHalfCloseTunnelAgainst(t, upstreamConn, serverRead)

	if got != recheckResponse {
		t.Fatalf("client got %q, want %q — the response was dropped by a full close", got, recheckResponse)
	}
	if !result.netOK {
		t.Fatalf("netOK: got false (stage=%q err=%v)", result.upstreamStage, result.upstreamErr)
	}
	if result.egressBytes != int64(len(recheckRequest)) || result.ingressBytes != int64(len(recheckResponse)) {
		t.Fatalf("bytes: egress=%d ingress=%d, want %d/%d",
			result.egressBytes, result.ingressBytes, len(recheckRequest), len(recheckResponse))
	}
}

// Same chain but with dialObserverConn at the bottom, the wrapper the HTTP
// transport path installs.
func TestRecheck_ObserverConnTunnelSurvivesClientHalfClose(t *testing.T) {
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer upstreamLn.Close()
	serverRead := halfCloseEchoServer(t, upstreamLn, len(recheckRequest), recheckResponse)

	raw, err := net.Dial("tcp", upstreamLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	st := &AttemptState{}
	st.dialAttempted.Store(true)
	st.dialSucceeded.Store(true)
	upstreamConn := newTLSLatencyConn(
		newCountingConn(
			&dialObserverConn{Conn: raw, st: st},
			newCountingConnTestSink(),
		),
		nil,
	)

	got, result := runHalfCloseTunnelAgainst(t, upstreamConn, serverRead)

	if got != recheckResponse {
		t.Fatalf("client got %q, want %q", got, recheckResponse)
	}
	if !result.netOK {
		t.Fatalf("netOK: got false (stage=%q err=%v)", result.upstreamStage, result.upstreamErr)
	}
	if st.bytesWritten() != int64(len(recheckRequest)) {
		t.Fatalf("observer counted %d written bytes, want %d", st.bytesWritten(), len(recheckRequest))
	}
}

// Bypass path: no dialCancelConn/dialObserverConn at all. Must keep working.
func TestRecheck_DirectTunnelSurvivesClientHalfClose(t *testing.T) {
	upstreamLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer upstreamLn.Close()
	serverRead := halfCloseEchoServer(t, upstreamLn, len(recheckRequest), recheckResponse)

	raw, err := net.Dial("tcp", upstreamLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()

	upstreamConn := newCountingConn(raw, newCountingConnTestSink())

	got, result := runHalfCloseTunnelAgainst(t, upstreamConn, serverRead)

	if got != recheckResponse {
		t.Fatalf("client got %q, want %q", got, recheckResponse)
	}
	if !result.netOK {
		t.Fatalf("netOK: got false (stage=%q err=%v)", result.upstreamStage, result.upstreamErr)
	}
}
