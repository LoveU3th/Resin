package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Resinat/Resin/internal/config"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/routing"
)

const (
	socks5Version                 = 0x05
	socks5MethodNoAuth            = 0x00
	socks5MethodUserPass          = 0x02
	socks5MethodNoAcceptable      = 0xFF
	socks5CommandConnect          = 0x01
	socks5AddressTypeIPv4         = 0x01
	socks5AddressTypeDomain       = 0x03
	socks5AddressTypeIPv6         = 0x04
	socks5ReplySucceeded          = 0x00
	socks5ReplyGeneralFailure     = 0x01
	socks5ReplyCommandUnsupported = 0x07
	socks5ReplyAddressUnsupported = 0x08
	socks5UserPassVersion         = 0x01
	socks5UserPassStatusSuccess   = 0x00
	socks5UserPassStatusFailure   = 0x01
	socks5PreparePollInterval     = 250 * time.Millisecond
	socks5PrepareReadChunkSize    = 4 * 1024
	socks5PreparePrefetchLimit    = 64 * 1024
)

// Socks5InboundConfig holds dependencies for the SOCKS5 inbound handler.
type Socks5InboundConfig struct {
	ProxyToken  string
	AuthVersion string
	Router      *routing.Router
	Pool        outbound.PoolAccessor
	Health      HealthRecorder
	Events      EventEmitter
	MetricsSink MetricsEventSink
}

// Socks5Inbound implements SOCKS5 CONNECT over a raw TCP connection.
type Socks5Inbound struct {
	token       string
	authVersion config.AuthVersion
	tunnel      tunnelDeps
	events      EventEmitter
}

// NewSocks5Inbound creates a new SOCKS5 inbound handler.
func NewSocks5Inbound(cfg Socks5InboundConfig) *Socks5Inbound {
	ev := cfg.Events
	if ev == nil {
		ev = NoOpEventEmitter{}
	}
	authVersion := config.NormalizeAuthVersion(cfg.AuthVersion)
	if authVersion == "" {
		authVersion = config.AuthVersionLegacyV0
	}
	return &Socks5Inbound{
		token:       cfg.ProxyToken,
		authVersion: authVersion,
		tunnel: tunnelDeps{
			router:      cfg.Router,
			pool:        cfg.Pool,
			health:      cfg.Health,
			metricsSink: cfg.MetricsSink,
		},
		events: ev,
	}
}

// ServeConn handles a SOCKS5 session on an already-accepted TCP connection.
func (s *Socks5Inbound) ServeConn(conn net.Conn, reader *bufio.Reader) {
	s.ServeConnContext(context.Background(), conn, reader)
}

// ServeConnContext handles a SOCKS5 session with a caller-provided base context.
func (s *Socks5Inbound) ServeConnContext(baseCtx context.Context, conn net.Conn, reader *bufio.Reader) {
	if conn == nil {
		return
	}
	defer conn.Close()
	if reader == nil {
		reader = bufio.NewReader(conn)
	}

	if s.authVersion != config.AuthVersionV1 {
		_, _ = conn.Write([]byte{socks5Version, socks5MethodNoAcceptable})
		return
	}

	method, ok := s.negotiateMethod(conn, reader)
	if !ok {
		return
	}

	platformName := ""
	account := ""
	if method == socks5MethodUserPass {
		var authOK bool
		platformName, account, authOK = s.authenticateUserPass(conn, reader)
		if !authOK {
			return
		}
	}

	target, replyCode, ok := readSocks5ConnectRequest(reader)
	if !ok {
		if replyCode != 0 {
			_ = writeSocks5Reply(conn, replyCode, nil)
		}
		return
	}

	lifecycle := newRequestLifecycleFromMetadata(
		s.events,
		conn.RemoteAddr().String(),
		"",
		ProxyTypeSocks5Forward,
		true,
		false,
	)
	lifecycle.setTarget(target, "")
	lifecycle.setAccount(account)
	defer lifecycle.finish()

	prepareMonitor, err := startSocks5PrepareMonitor(baseCtx, conn, reader)
	if err != nil {
		return
	}
	// The prepare monitor has drained any unread bytes it needs from reader.
	// Drop the handshake reader reference so long-lived tunnels don't retain it.
	reader = nil
	prepare := prepareConnectTunnel(prepareMonitor.Context(), s.tunnel, lifecycle, platformName, account, target)
	prepareMonitor.Stop()
	if prepare.session == nil {
		if prepare.proxyErr != nil {
			lifecycle.setProxyError(prepare.proxyErr)
			if prepare.upstreamStage != "" {
				lifecycle.setUpstreamError(prepare.upstreamStage, prepare.upstreamErr)
			}
			_ = writeSocks5Reply(conn, socks5ReplyGeneralFailure, nil)
		} else if prepareMonitor.Overflowed() {
			// We support only bounded optimistic early data before CONNECT success.
			// Treat larger pre-connect payloads as a protocol failure without
			// penalizing node health.
			lifecycle.setNetOK(false)
			lifecycle.setProxyError(ErrUpstreamRequestFailed)
			lifecycle.setUpstreamError("socks5_client_early_data_overflow", nil)
			_ = writeSocks5Reply(conn, socks5ReplyGeneralFailure, nil)
		}
		return
	}

	if err := writeSocks5Reply(conn, socks5ReplySucceeded, prepare.session.upstreamConn.LocalAddr()); err != nil {
		prepare.session.upstreamConn.Close()
		lifecycle.setUpstreamError("socks5_connect_response_write", err)
		return
	}

	pumpPreparedTunnelReader(conn, prepareMonitor.ClientReader(), prepare.session, lifecycle, tunnelPumpOptions{})
}

type socks5PrepareMonitor struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
	conn     net.Conn
	prefetch []byte
	overflow bool
}

func startSocks5PrepareMonitor(baseCtx context.Context, conn net.Conn, reader *bufio.Reader) (*socks5PrepareMonitor, error) {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(baseCtx)

	prefetch, err := drainBufferedReader(reader)
	if err != nil {
		cancel()
		return nil, err
	}

	m := &socks5PrepareMonitor{
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		conn:     conn,
		prefetch: prefetch,
	}
	if len(m.prefetch) > socks5PreparePrefetchLimit {
		m.overflow = true
		cancel()
		close(m.done)
		return m, nil
	}
	if conn == nil {
		close(m.done)
		return m, nil
	}
	go m.run()
	return m, nil
}

func drainBufferedReader(reader *bufio.Reader) ([]byte, error) {
	if reader == nil || reader.Buffered() == 0 {
		return nil, nil
	}
	prefetch := make([]byte, reader.Buffered())
	if _, err := io.ReadFull(reader, prefetch); err != nil {
		return nil, err
	}
	return prefetch, nil
}

func (m *socks5PrepareMonitor) run() {
	defer close(m.done)
	if m == nil || m.conn == nil {
		return
	}

	scratch := make([]byte, socks5PrepareReadChunkSize)
	for {
		if m.ctx.Err() != nil {
			return
		}
		remaining := socks5PreparePrefetchLimit - len(m.prefetch)

		if err := m.conn.SetReadDeadline(time.Now().Add(socks5PreparePollInterval)); err != nil {
			m.cancel()
			return
		}
		readBuf := scratch
		if remaining <= 0 {
			readBuf = readBuf[:1]
		} else if remaining < len(readBuf) {
			readBuf = readBuf[:remaining]
		}
		n, err := m.conn.Read(readBuf)
		_ = m.conn.SetReadDeadline(time.Time{})

		if n > 0 {
			if remaining <= 0 {
				m.overflow = true
				m.cancel()
				return
			}
			m.prefetch = append(m.prefetch, readBuf[:n]...)
		}
		if m.ctx.Err() != nil {
			return
		}
		if err == nil {
			continue
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			continue
		}
		m.cancel()
		return
	}
}

func (m *socks5PrepareMonitor) Context() context.Context {
	if m == nil || m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

func (m *socks5PrepareMonitor) Stop() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
		if m.conn != nil {
			_ = m.conn.SetReadDeadline(time.Now())
		}
		if m.done != nil {
			<-m.done
		}
		if m.conn != nil {
			_ = m.conn.SetReadDeadline(time.Time{})
		}
	})
}

func (m *socks5PrepareMonitor) ClientReader() io.Reader {
	if m == nil || m.conn == nil {
		return nil
	}
	if len(m.prefetch) == 0 {
		return m.conn
	}
	return io.MultiReader(bytes.NewReader(m.prefetch), m.conn)
}

func (m *socks5PrepareMonitor) Overflowed() bool {
	if m == nil {
		return false
	}
	return m.overflow
}

func (s *Socks5Inbound) negotiateMethod(conn net.Conn, reader *bufio.Reader) (byte, bool) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return 0, false
	}
	if header[0] != socks5Version {
		return 0, false
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, methods); err != nil {
		return 0, false
	}

	selected := byte(socks5MethodNoAcceptable)
	if s.token != "" {
		if containsSocks5Method(methods, socks5MethodUserPass) {
			selected = socks5MethodUserPass
		}
	} else {
		switch {
		case containsSocks5Method(methods, socks5MethodUserPass):
			selected = socks5MethodUserPass
		case containsSocks5Method(methods, socks5MethodNoAuth):
			selected = socks5MethodNoAuth
		}
	}

	if _, err := conn.Write([]byte{socks5Version, selected}); err != nil {
		return 0, false
	}
	if selected == socks5MethodNoAcceptable {
		return 0, false
	}
	return selected, true
}

func (s *Socks5Inbound) authenticateUserPass(conn net.Conn, reader *bufio.Reader) (string, string, bool) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return "", "", false
	}
	if header[0] != socks5UserPassVersion {
		_, _ = conn.Write([]byte{socks5UserPassVersion, socks5UserPassStatusFailure})
		return "", "", false
	}

	username := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, username); err != nil {
		return "", "", false
	}

	plen := []byte{0}
	if _, err := io.ReadFull(reader, plen); err != nil {
		return "", "", false
	}
	password := make([]byte, int(plen[0]))
	if _, err := io.ReadFull(reader, password); err != nil {
		return "", "", false
	}

	if s.token != "" && string(password) != s.token {
		_, _ = conn.Write([]byte{socks5UserPassVersion, socks5UserPassStatusFailure})
		return "", "", false
	}

	if _, err := conn.Write([]byte{socks5UserPassVersion, socks5UserPassStatusSuccess}); err != nil {
		return "", "", false
	}

	platformName, account := parseV1PlatformAccountIdentity(string(username))
	return platformName, account, true
}

func containsSocks5Method(methods []byte, candidate byte) bool {
	for _, method := range methods {
		if method == candidate {
			return true
		}
	}
	return false
}

func readSocks5ConnectRequest(reader *bufio.Reader) (string, byte, bool) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return "", 0, false
	}
	if header[0] != socks5Version {
		return "", socks5ReplyGeneralFailure, false
	}
	if header[1] != socks5CommandConnect {
		return "", socks5ReplyCommandUnsupported, false
	}

	host, ok := readSocks5Address(reader, header[3])
	if !ok {
		if header[3] != socks5AddressTypeIPv4 && header[3] != socks5AddressTypeDomain && header[3] != socks5AddressTypeIPv6 {
			return "", socks5ReplyAddressUnsupported, false
		}
		return "", socks5ReplyGeneralFailure, false
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(reader, portBuf); err != nil {
		return "", 0, false
	}
	port := strconv.Itoa(int(binary.BigEndian.Uint16(portBuf)))
	return net.JoinHostPort(host, port), 0, true
}

func readSocks5Address(reader *bufio.Reader, atyp byte) (string, bool) {
	switch atyp {
	case socks5AddressTypeIPv4:
		ip := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(reader, ip); err != nil {
			return "", false
		}
		return net.IP(ip).String(), true
	case socks5AddressTypeIPv6:
		ip := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(reader, ip); err != nil {
			return "", false
		}
		return net.IP(ip).String(), true
	case socks5AddressTypeDomain:
		size := []byte{0}
		if _, err := io.ReadFull(reader, size); err != nil {
			return "", false
		}
		domain := make([]byte, int(size[0]))
		if _, err := io.ReadFull(reader, domain); err != nil {
			return "", false
		}
		return string(domain), true
	default:
		return "", false
	}
}

func writeSocks5Reply(w io.Writer, replyCode byte, addr net.Addr) error {
	atyp := byte(socks5AddressTypeIPv4)
	hostBytes := []byte{0, 0, 0, 0}
	port := uint16(0)

	if tcpAddr, ok := addr.(*net.TCPAddr); ok && tcpAddr != nil {
		if ip4 := tcpAddr.IP.To4(); ip4 != nil {
			atyp = socks5AddressTypeIPv4
			hostBytes = append([]byte(nil), ip4...)
		} else if ip16 := tcpAddr.IP.To16(); ip16 != nil {
			atyp = socks5AddressTypeIPv6
			hostBytes = append([]byte(nil), ip16...)
		}
		if tcpAddr.Port >= 0 && tcpAddr.Port <= 65535 {
			port = uint16(tcpAddr.Port)
		}
	}

	resp := make([]byte, 0, 6+len(hostBytes))
	resp = append(resp, socks5Version, replyCode, 0x00, atyp)
	resp = append(resp, hostBytes...)
	resp = binary.BigEndian.AppendUint16(resp, port)
	_, err := w.Write(resp)
	return err
}
