package saasclient

// Тесты хэш-аутентификации «hash(идентификатор + соль)» (контракт с Collector v6.1).
// Известный вектор согласован с collector/main.py (_hmac_sig) и collector/send_test.py —
// менять формулу можно ТОЛЬКО обновив все три места.

import (
	"net/http"
	"path/filepath"
	"testing"

	"chagent/internal/state"
)

func TestSignKnownVector(t *testing.T) {
	c := &Client{key: "s3cret"}
	// Python: hmac.new(b"s3cret", b"11111111-1111-1111-1111-111111111111", sha256).hexdigest()
	const want = "84f81d76bb5adb3f2af1d4e803ae103255144d30f6114c7334edc43159446a95"
	got := c.sign("11111111-1111-1111-1111-111111111111")
	if got != want {
		t.Fatalf("sign: вектор разошёлся:\n got  %s\n want %s", got, want)
	}
	if got := c.sign(""); got != "" {
		t.Fatalf("sign(empty ident) = %q, хочу пусто", got)
	}
}

func TestAuthHeadersModes(t *testing.T) {
	dir := t.TempDir()
	st, err := state.Load(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	st.Mutate(func(d *state.Data) {
		d.AgentUUID = "11111111-1111-1111-1111-111111111111"
		d.ClientID = "22222222-2222-2222-2222-222222222222"
	})
	c := &Client{key: "s3cret", st: st}

	// agent-режим: только agent-заголовки
	req, _ := http.NewRequest(http.MethodPost, "/v2/clients/register", nil)
	c.authHeaders(req, "agent")
	if got := req.Header.Get("X-Agent-Id"); got != st.Get().AgentUUID {
		t.Fatalf("X-Agent-Id = %q", got)
	}
	if req.Header.Get("X-Agent-Hash") != c.sign(st.Get().AgentUUID) {
		t.Fatalf("X-Agent-Hash не совпал со sign(agent_uuid)")
	}
	if req.Header.Get("X-Client-Id") != "" {
		t.Fatalf("agent-режим не должен ставить X-Client-Id")
	}

	// client-режим: только client-заголовки
	req2, _ := http.NewRequest(http.MethodGet, "/v2/rules/latest", nil)
	c.authHeaders(req2, "client")
	if got := req2.Header.Get("X-Client-Id"); got != st.Get().ClientID {
		t.Fatalf("X-Client-Id = %q", got)
	}
	if req2.Header.Get("X-Client-Hash") != c.sign(st.Get().ClientID) {
		t.Fatalf("X-Client-Hash не совпал со sign(client_id)")
	}
	if req2.Header.Get("X-Agent-Hash") != "" {
		t.Fatalf("client-режим не должен ставить X-Agent-Hash")
	}
}

func TestAuthHeadersEmptyKeyOff(t *testing.T) {
	dir := t.TempDir()
	st, err := state.Load(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	c := &Client{key: "", st: st} // соль не задана — auth off (пилот)
	req, _ := http.NewRequest(http.MethodGet, "/v2/rules/latest", nil)
	c.authHeaders(req, "auto")
	if req.Header.Get("X-Agent-Hash") != "" || req.Header.Get("X-Client-Hash") != "" {
		t.Fatalf("без соли заголовки подписи не ставятся")
	}
}
