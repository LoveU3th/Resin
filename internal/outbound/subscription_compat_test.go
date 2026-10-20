package outbound

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/subscription"
	sbTLS "github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
)

func TestParsedQUICSubscriptionTLSConfig(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{
			name:  "hysteria2 URI",
			input: "hysteria2://password@example.com:443?alpn=h3&fp=chrome&sni=example.com",
		},
		{
			name:  "Clash hysteria",
			input: `{"proxies":[{"name":"hy","type":"hysteria","server":"example.com","port":443,"auth-str":"password","up":30,"down":100,"client-fingerprint":"chrome","sni":"example.com"}]}`,
		},
		{
			name:  "Clash VMess QUIC",
			input: `{"proxies":[{"name":"vmess","type":"vmess","server":"example.com","port":443,"uuid":"00000000-0000-0000-0000-000000000000","network":"quic","tls":true,"fp":"chrome","sni":"example.com"}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes, err := subscription.ParseGeneralSubscription([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if len(nodes) != 1 {
				t.Fatalf("expected 1 parsed node, got %d", len(nodes))
			}
			var options struct {
				option.ServerOptions
				option.OutboundTLSOptionsContainer
			}
			if err := json.Unmarshal(nodes[0].RawOptions, &options); err != nil {
				t.Fatal(err)
			}
			if options.TLS == nil || !options.TLS.Enabled {
				t.Fatal("QUIC requires TLS")
			}
			tlsConfig, err := sbTLS.NewClient(context.Background(), options.Server, *options.TLS)
			if err != nil {
				t.Fatal(err)
			}
			// QUIC asks sing-box for a standard TLS config when connecting.
			// A uTLS config instead fails here with "unsupported usage for uTLS".
			if _, err := tlsConfig.Config(); err != nil {
				t.Fatalf("parsed subscription cannot provide QUIC TLS config: %v", err)
			}
		})
	}
}

func TestParsedVMessTCPHTTPHeaderOnWire(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	deadline := time.Now().Add(5 * time.Second)
	if err := listener.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	payload := fmt.Sprintf(`{"v":"2","ps":"HTTP-header-test","add":"127.0.0.1","port":%d,"id":"00000000-0000-0000-0000-000000000000","aid":0,"scy":"auto","net":"tcp","type":"http","host":"front.example.com","path":"/tunnel","tls":"none"}`, listener.Addr().(*net.TCPAddr).Port)
	nodes, err := subscription.ParseGeneralSubscription([]byte("vmess://" + base64.StdEncoding.EncodeToString([]byte(payload))))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 parsed node, got %d", len(nodes))
	}
	b := newTestSingboxBuilder(t)
	defer b.Close()
	ob, err := b.Build(nodes[0].RawOptions)
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := ob.(io.Closer); ok {
		defer closer.Close()
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	conn, err := ob.DialContext(ctx, "tcp", M.ParseSocksaddr("example.com:80"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("probe")); err != nil {
		t.Fatal(err)
	}
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if err := peer.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	request, err := http.ReadRequest(bufio.NewReader(peer))
	if err != nil {
		t.Fatalf("VMess stream must start with HTTP header obfuscation: %v", err)
	}
	defer request.Body.Close()
	if request.Method != http.MethodPut || request.URL.Path != "/tunnel" || request.Host != "front.example.com" {
		t.Fatalf("unexpected HTTP header: method=%s path=%s host=%s", request.Method, request.URL.Path, request.Host)
	}
}
