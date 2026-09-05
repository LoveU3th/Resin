package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Resinat/Resin/internal/node"
	M "github.com/sagernet/sing/common/metadata"
)

// The slow/broken distinction must hold on the reverse path too, not just the
// forward one: a slow origin must lower the health score without evicting the
// node, while a node that cannot be dialed is still isolated.
func TestReverseSlowNodeIsNotTreatedAsDead(t *testing.T) {
	slow := newSlowServer()
	defer slow.Close()
	su, _ := url.Parse(slow.URL)

	envSlow := newFIEnv(t, 1)
	envSlow.outbounds[0].setDial(realDial(M.ParseSocksaddr(su.Host)))
	rpSlow := NewReverseProxy(ReverseProxyConfig{
		Router:         envSlow.router,
		Pool:           envSlow.pool,
		PlatformLookup: envSlow.pool,
		Health:         envSlow.pool,
		Events:         newMockEventEmitter(),
		Failover:       FailoverConfig{Enabled: true, MaxAttempts: 2, AttemptBudget: 120 * time.Millisecond},
		OutboundTransport: OutboundTransportConfig{
			DialTimeout: time.Second, ResponseHeaderTimeout: 5 * time.Second,
		},
	})
	entrySlow, _ := envSlow.pool.GetEntry(envSlow.hashes[0])

	for i := 0; i < 3; i++ {
		if !envSlow.plat.View().Contains(envSlow.hashes[0]) {
			t.Logf("reverse slow node left the routable view after %d requests", i)
			break
		}
		req := httptest.NewRequest(http.MethodGet,
			"http://resin.test/tok/plat:acct-1/http/"+su.Host+"/slow", nil)
		w := httptest.NewRecorder()
		rpSlow.ServeHTTP(w, req)
		if w.Code != http.StatusGatewayTimeout {
			t.Errorf("reverse slow status: got %d, want 504", w.Code)
		}
		time.Sleep(80 * time.Millisecond)
	}

	t.Logf("reverse slow: failureCount=%d circuitOpen=%v health=%.3f view=%v",
		entrySlow.FailureCount.Load(), entrySlow.IsCircuitOpen(), entrySlow.HealthScore(),
		envSlow.plat.View().Contains(envSlow.hashes[0]))

	if entrySlow.IsCircuitOpen() {
		t.Errorf("a merely slow node was evicted on the reverse path (failureCount=%d): "+
			"the slow/broken split is not reaching the breaker", entrySlow.FailureCount.Load())
	}
	if !envSlow.plat.View().Contains(envSlow.hashes[0]) {
		t.Errorf("slow node left the routable view on the reverse path")
	}
	// It must still be penalised: slowness is a real signal, just not a fatal one.
	if entrySlow.HealthScore() >= 1.0 {
		t.Errorf("slow node was not penalised at all: health=%.3f", entrySlow.HealthScore())
	}
	var _ = node.PassiveStageTransfer
}
