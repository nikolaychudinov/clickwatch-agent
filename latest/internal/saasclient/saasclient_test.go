package saasclient

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"chagent/internal/buffer"
	"chagent/internal/config"
	"chagent/internal/state"
)

// captureServer — тестовый приёмник инжеста: пишет полученные payload
// и отдаёт заданный статус.
type captureServer struct {
	mu     sync.Mutex
	bodies [][]byte
	status int
}

func (s *captureServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.bodies = append(s.bodies, body)
		st := s.status
		s.mu.Unlock()
		if st != http.StatusOK {
			w.WriteHeader(st)
			_, _ = w.Write([]byte("test reject"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
}

func (s *captureServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.bodies)
}

func (s *captureServer) setStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
}

type testEnv struct {
	srv  *captureServer
	http *httptest.Server
	c    *Client
	buf  *buffer.B
	dir  string
}

func newTestEnv(t *testing.T, status int) *testEnv {
	t.Helper()
	cs := &captureServer{status: status}
	httpSrv := httptest.NewServer(cs.handler())
	t.Cleanup(httpSrv.Close)

	dir := t.TempDir()
	buf, err := buffer.New(dir, 1<<20)
	if err != nil {
		t.Fatalf("buffer.New: %v", err)
	}
	st, err := state.Load(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("state.Load: %v", err)
	}
	var send config.Send
	send.EndpointBase = httpSrv.URL
	send.EventsEndpoint = "/v2/agent/events"
	send.HTTPTimeout.Duration = 5 * time.Second
	var sync config.Sync
	var stCfg config.Status
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := New(log, send, sync, stCfg, "test-key", "1.2.0", st, buf)
	return &testEnv{srv: cs, http: httpSrv, c: c, buf: buf, dir: dir}
}

// parseEnvelope разбирает payload конверта.
func parseEnvelope(t *testing.T, raw []byte) (agent struct {
	AgentUUID string `json:"agent_uuid"`
	AgentSeq  uint64 `json:"agent_seq"`
}, records []map[string]any) {
	t.Helper()
	var env struct {
		Agent struct {
			AgentUUID string `json:"agent_uuid"`
			AgentSeq  uint64 `json:"agent_seq"`
		} `json:"agent"`
		HostTime string           `json:"host_time"`
		Records  []map[string]any `json:"records"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("разбор конверта: %v", err)
	}
	return env.Agent, env.Records
}

// Успешный цикл: очередь опустошается, конверт содержит records и
// монотонно растущий agent_seq.
func TestFlushSuccess(t *testing.T) {
	env := newTestEnv(t, http.StatusOK)
	pA := "/v2/ingest/c1/ch.parts.too_many_parts"
	pB := "/v2/ingest/c1/ch.queries.exceptions"
	env.c.Enqueue(pA, map[string]any{"uid": "ch.parts.too_many_parts", "status": "warning"})
	env.c.Enqueue(pB, map[string]any{"uid": "ch.queries.exceptions", "status": "none"})

	env.c.Flush(context.Background())

	if n, _ := env.buf.Pending(); n != 0 {
		t.Fatalf("очередь не опустела: %d", n)
	}
	if env.srv.count() != 2 {
		t.Fatalf("запросов %d, ожидается 2 (по одному на путь)", env.srv.count())
	}
	env.srv.mu.Lock()
	b1, b2 := env.srv.bodies[0], env.srv.bodies[1]
	env.srv.mu.Unlock()

	a1, recs1 := parseEnvelope(t, b1)
	a2, recs2 := parseEnvelope(t, b2)
	if len(recs1) != 1 || recs1[0]["uid"] != "ch.parts.too_many_parts" {
		t.Fatalf("первый конверт: %v", recs1)
	}
	if len(recs2) != 1 || recs2[0]["uid"] != "ch.queries.exceptions" {
		t.Fatalf("второй конверт: %v", recs2)
	}
	if a1.AgentSeq >= a2.AgentSeq {
		t.Fatalf("agent_seq не рос: %d затем %d", a1.AgentSeq, a2.AgentSeq)
	}
	if a1.AgentUUID == "" {
		t.Fatal("agent_uuid пуст")
	}
	if env.c.sentTotal.Load() != 2 {
		t.Fatalf("sentTotal = %d", env.c.sentTotal.Load())
	}
}

// Временный отказ: задачи остаются в очереди с backoff; после «исполнения»
// backoff досылаются.
func TestFlushTemporaryFailureThenRecovery(t *testing.T) {
	env := newTestEnv(t, http.StatusInternalServerError)
	path := "/v2/ingest/c1/ch.memory.crashes"
	env.c.Enqueue(path, map[string]any{"uid": "ch.memory.crashes"})

	env.c.Flush(context.Background())

	if n, _ := env.buf.Pending(); n != 1 {
		t.Fatalf("задача должна остаться в очереди, pending=%d", n)
	}
	if env.srv.count() != 1 {
		t.Fatalf("запросов %d", env.srv.count())
	}
	// немедленный повтор до истечения backoff не отправляет
	env.c.Flush(context.Background())
	if env.srv.count() != 1 {
		t.Fatalf("backoff не работает: запросов %d", env.srv.count())
	}
	// форсируем срок попытки в прошлое и включаем успех на приёме
	items, names := env.buf.Batch(50, 1<<20, time.Now().Add(16*time.Minute))
	if len(items) != 1 || items[0].Attempts != 1 {
		t.Fatalf("items %v", items)
	}
	_ = env.buf.Defer(names, time.Now().Add(-time.Second))
	env.srv.setStatus(http.StatusOK)
	env.c.Flush(context.Background())

	if n, _ := env.buf.Pending(); n != 0 {
		t.Fatalf("после восстановления очередь не пуста: %d", n)
	}
	if env.c.sentTotal.Load() != 1 {
		t.Fatalf("sentTotal = %d", env.c.sentTotal.Load())
	}
	if env.srv.count() != 2 {
		t.Fatalf("запросов %d, ожидается 2", env.srv.count())
	}
}

// HTTP 4xx — необратимая потеря: задача из очереди удаляется.
func TestFlushPermanentFailure(t *testing.T) {
	env := newTestEnv(t, http.StatusBadRequest)
	env.c.Enqueue("/v2/ingest/c1/ch.broken.rule", map[string]any{"uid": "ch.broken.rule"})

	env.c.Flush(context.Background())

	if n, bytes := env.buf.Pending(); n != 0 || bytes != 0 {
		t.Fatalf("4xx должен очистить задачу: pending=%d bytes=%d", n, bytes)
	}
	if env.c.failedTotal.Load() != 1 {
		t.Fatalf("failedTotal = %d", env.c.failedTotal.Load())
	}
}

// События ставятся в ту же очередь.
func TestEventsPath(t *testing.T) {
	env := newTestEnv(t, http.StatusOK)
	env.c.Enqueue(env.c.EventsPath(), map[string]any{"event": "ch_unreachable"})
	if n, _ := env.buf.Pending(); n != 1 {
		t.Fatalf("событие не в очереди: %d", n)
	}
	env.c.Flush(context.Background())
	if n, _ := env.buf.Pending(); n != 0 {
		t.Fatalf("очередь не опустела: %d", n)
	}
	env.srv.mu.Lock()
	last := env.srv.bodies[len(env.srv.bodies)-1]
	env.srv.mu.Unlock()
	_, recs := parseEnvelope(t, last)
	if len(recs) != 1 || recs[0]["event"] != "ch_unreachable" {
		t.Fatalf("event payload: %v", recs)
	}
}

// Разрыв между Enqueue и Flush: перезапуск клиента с тем же каталогом
// сохраняет задачи (гарантия «ничего не теряется»).
func TestQueueSurvivesRestart(t *testing.T) {
	env := newTestEnv(t, http.StatusOK)
	env.c.Enqueue("/v2/agent/events", map[string]any{"event": "x"})

	// «новый процесс»: тот же каталог, новый клиент очереди
	b2, err := buffer.New(env.dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := b2.Pending(); n != 1 {
		t.Fatalf("после «рестарта» в очереди %d, ожидается 1", n)
	}
}
