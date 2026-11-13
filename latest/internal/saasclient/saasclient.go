// Package saasclient: HTTP-клиент SaaS AGENT → SaaS по протокол обмена с сервером:
// register (эндпоинт 1), rules fetch (3), per-rule инжест (4, требование 6),
// events (5), статус клиента (7). Конверт (envelope, разд. 2) собирается в
// момент отправки, инжест и события идут через персистентную очередь
// (internal/buffer): запись на диске до подтверждения, рестарт ничего
// не теряет, повторные попытки с экспоненциальным backoff.
package saasclient

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"chagent/internal/buffer"
	"chagent/internal/config"
	"chagent/internal/state"
)

// AgentInfo — блок agent конверта.
type AgentInfo struct {
	AgentUUID    string `json:"agent_uuid"`
	AgentVersion string `json:"agent_version"`
	AgentSeq     uint64 `json:"agent_seq"`
}

// Envelope — обязательная обвязка каждого запроса (protocol, разд. 2).
type Envelope struct {
	Agent    AgentInfo        `json:"agent"`
	HostTime string           `json:"host_time"`
	DBTime   *string          `json:"db_time,omitempty"`
	Records  []map[string]any `json:"records"`
}

// Client — клиент SaaS.
type Client struct {
	log           *slog.Logger
	cfg           config.Send
	rulesEndpoint string
	statusPath    string
	channel       string
	key           string
	agentVer      string
	st            *state.Store
	buf           *buffer.B
	hc            *http.Client
	dbTime        func() string

	sentTotal   atomic.Int64 // задач отправлено успешно (за всё время процесса)
	failedTotal atomic.Int64 // задач потеряно необратимо (HTTP 4xx)
}

// New создаёт клиент; agentVersion — версия сборки (конверт), dbTime — источник времени CH.
func New(log *slog.Logger, send config.Send, sync config.Sync, stCfg config.Status,
	apiKey, agentVersion string, st *state.Store, buf *buffer.B) *Client {
	rulesEp := sync.RulesEndpoint
	if rulesEp == "" {
		rulesEp = "/v2/rules/latest"
	}
	statusP := stCfg.StatusEndpoint
	if statusP == "" {
		statusP = "/v2/clients/{{client_id}}/status"
	}
	channel := sync.Channel
	if channel == "" {
		channel = "stable"
	}
	c := &Client{
		log:           log,
		cfg:           send,
		rulesEndpoint: rulesEp,
		statusPath:    statusP,
		channel:       channel,
		key:           apiKey,
		agentVer:      agentVersion,
		st:            st,
		buf:           buf,
		hc:            &http.Client{Timeout: send.HTTPTimeout.Duration},
	}
	if buf != nil {
		if n, _ := buf.Pending(); n > 0 {
			log.Info("очередь отправки восстановлена после рестарта, будет дослана", "записей", n)
		}
		if legacy := buf.LegacyCount(); legacy > 0 {
			log.Warn("устаревшие записи прежнего формата буфера удалены", "count", legacy)
		}
	}
	return c
}

// SetDBTime регистрирует функцию времени ClickHouse.
func (c *Client) SetDBTime(fn func() string) { c.dbTime = fn }

// EventsPath — путь канала событий (self-метрики и события агента).
func (c *Client) EventsPath() string { return c.cfg.EventsEndpoint }

func (c *Client) newEnvelope(records []map[string]any) ([]byte, error) {
	var dbInfo *string
	if c.dbTime != nil {
		if s := c.dbTime(); s != "" {
			dbInfo = &s
		}
	}
	env := Envelope{
		Agent: AgentInfo{
			AgentUUID:    c.st.Get().AgentUUID,
			AgentVersion: c.agentVer,
			AgentSeq:     c.st.NextSeq(),
		},
		HostTime: time.Now().UTC().Format(time.RFC3339),
		DBTime:   dbInfo,
		Records:  records,
	}
	return json.Marshal(env)
}

func (c *Client) fullURL(path string) string {
	return c.cfg.EndpointBase + path
}

// sign — HMAC-SHA256(соль, идентификатор). Соль — identity.api_key_ref
// (общий секрет с коллектором). Пользовательская формула «hash(client_id + соль)»
// в стойкой форме: HMAC не раскрывает соль и не поддаётся удлиннению сообщения.
func (c *Client) sign(ident string) string {
	if c.key == "" || ident == "" {
		return ""
	}
	mac := hmac.New(sha256.New, []byte(c.key))
	mac.Write([]byte(ident))
	return hex.EncodeToString(mac.Sum(nil))
}

// authHeaders — хэш-аутентификация вместо заголовка Authorization:
// режим "agent" (register, каналы до выдачи client_id): X-Agent-Id + X-Agent-Hash;
// режим "client" (всё после register): X-Client-Id + X-Client-Hash;
// режим "auto": client, если client_id уже выдан, иначе agent.
func (c *Client) authHeaders(req *http.Request, mode string) {
	if c.key == "" {
		return // соль не задана — auth off (пилот)
	}
	switch mode {
	case "client":
		if cid := c.st.Get().ClientID; cid != "" {
			req.Header.Set("X-Client-Id", cid)
			req.Header.Set("X-Client-Hash", c.sign(cid))
		}
	case "agent":
		if auid := c.st.Get().AgentUUID; auid != "" {
			req.Header.Set("X-Agent-Id", auid)
			req.Header.Set("X-Agent-Hash", c.sign(auid))
		}
	default: // auto
		if cid := c.st.Get().ClientID; cid != "" {
			req.Header.Set("X-Client-Id", cid)
			req.Header.Set("X-Client-Hash", c.sign(cid))
		} else if auid := c.st.Get().AgentUUID; auid != "" {
			req.Header.Set("X-Agent-Id", auid)
			req.Header.Set("X-Agent-Hash", c.sign(auid))
		}
	}
}

// postBytes отправляет готовый payload; классифицирует отказы:
// сеть/5xx → временная (задача остаётся в очереди), HTTP 4xx → постоянная.
func (c *Client) postBytes(ctx context.Context, path string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.fullURL(path), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("saas: запрос: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.authHeaders(req, "auto")
	resp, err := c.hc.Do(req)
	if err != nil {
		return errTemporary{err}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 500 {
		return errTemporary{fmt.Errorf("saas: HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))}
	}
	if resp.StatusCode >= 400 {
		return errPermanent{fmt.Errorf("saas: HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))}
	}
	return nil
}

type errTemporary struct{ err error }

func (e errTemporary) Error() string { return e.err.Error() }

type errPermanent struct{ err error }

func (e errPermanent) Error() string { return e.err.Error() }

func isPermanent(err error) bool {
	_, ok := err.(errPermanent)
	return ok
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// Register — POST регистрации (эндпоинт 1). payload: инвентарь ноды.
func (c *Client) Register(ctx context.Context, payload map[string]any) (string, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.fullURL(c.cfg.RegisterEndpoint), bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authHeaders(req, "auto")
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("saas: register HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.ClientID == "" {
		return "", fmt.Errorf("saas: register: в ответе нет client_id")
	}
	return out.ClientID, nil
}

// FetchRules — GET набора правил (эндпоинт 3) с If-None-Match.
// ФТ-4.1: параметры channel и min_agent_version в строке запроса.
func (c *Client) FetchRules(ctx context.Context, etag string) ([]byte, bool, error) {
	q := url.Values{}
	q.Set("channel", c.channel)
	q.Set("min_agent_version", c.agentVer)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.fullURL(c.rulesEndpoint)+"?"+q.Encode(), nil)
	if err != nil {
		return nil, false, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	c.authHeaders(req, "auto")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil, false, nil
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		if err != nil {
			return nil, false, err
		}
		return body, true, nil
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, false, fmt.Errorf("saas: rules HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
}

// ClientStatus — опрос статуса клиента (эндпоинт 7).
func (c *Client) ClientStatus(ctx context.Context) (string, error) {
	cid := c.st.Get().ClientID
	if cid == "" {
		return "", fmt.Errorf("saas: статус без client_id")
	}
	path := strings.ReplaceAll(c.statusPath, "{{client_id}}", cid)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.fullURL(path), nil)
	if err != nil {
		return "", err
	}
	c.authHeaders(req, "client")
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("saas: status HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var out struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.State == "" {
		return "", fmt.Errorf("saas: status: в ответе нет state")
	}
	return out.State, nil
}

// Enqueue (Enqueue прототипа) — постановка записи в персистентную очередь.
// Файл задачи пишется на диск до отправки; рестарт/крах ничего не теряют.
// Ошибка IO — единственный случай потери (пишется в лог).
func (c *Client) Enqueue(path string, rec map[string]any) {
	if c.buf == nil {
		return
	}
	if err := c.buf.Append(path, []map[string]any{rec}); err != nil {
		c.failedTotal.Add(1)
		c.log.Error("saas: запись не сохранена в очередь", "path", path, "err", err.Error())
	}
}

// StartFlusher запускает цикл флаша по send.flush_interval; первый цикл —
// сразу, чтобы дослать очередь, восстановленную после рестарта.
func (c *Client) StartFlusher(ctx context.Context) {
	interval := c.cfg.FlushInterval.Duration
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		c.Flush(ctx)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.Flush(ctx)
			}
		}
	}()
}

// Flush — один цикл отправки: берёт готовые задачи из очереди, формирует
// конверт в момент отправки (agent_seq остаётся монотонным), на успех
// удаляет, на временный отказ откладывает с экспоненциальным backoff,
// HTTP 4xx — необратимая потеря (ошибка в данных повторами не лечится).
func (c *Client) Flush(ctx context.Context) {
	if c.buf == nil {
		return
	}
	maxRec := c.cfg.BatchMaxResults
	if maxRec <= 0 {
		maxRec = 50
	}
	maxBytes := int64(c.cfg.BatchMaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	for i := 0; i < 500; i++ {
		items, names := c.buf.Batch(maxRec, maxBytes, time.Now())
		if len(items) == 0 {
			break
		}
		recs := make([]map[string]any, 0, len(items))
		for _, it := range items {
			for _, raw := range it.Records {
				var m map[string]any
				if err := json.Unmarshal(raw, &m); err == nil {
					recs = append(recs, m)
				}
			}
		}
		if len(recs) == 0 {
			c.commitQuietly(names)
			continue
		}
		payload, err := c.newEnvelope(recs)
		if err != nil {
			c.log.Error("saas: сбор конверта", "err", err.Error())
			break
		}
		err = c.postBytes(ctx, items[0].Path, payload)
		switch {
		case err == nil:
			if !c.commitQuietly(names) {
				return
			}
			c.sentTotal.Add(int64(len(items)))
		case isPermanent(err):
			c.log.Error("saas: записи отклонены SaaS и удалены из очереди",
				"path", items[0].Path, "записей", len(items), "err", err.Error())
			if !c.commitQuietly(names) {
				return
			}
			c.failedTotal.Add(int64(len(items)))
		default:
			attempts := items[len(items)-1].Attempts + 1
			pause := c.backoffFor(attempts)
			if derr := c.buf.Defer(names, time.Now().Add(pause)); derr != nil {
				c.log.Error("saas: откладывание задач очереди", "err", derr.Error())
				return
			}
			c.log.Warn("saas: приём недоступен, попытка отложена",
				"path", items[0].Path, "записей", len(items),
				"попытка", attempts, "следующая_через", pause.String())
			return
		}
	}
	c.logQueueStats()
}

func (c *Client) commitQuietly(names []string) bool {
	if err := c.buf.Commit(names); err != nil {
		c.log.Error("saas: удаление отправленных задач из очереди", "err", err.Error())
		return false
	}
	return true
}

// backoffFor — экспоненциальная пауза между попытками: 2s, 4s, … до 15m.
// Потолка попыток нет: данные не теряем, их судьбу при длительной паузе
// решает лимит storage.buffer_max_bytes.
func (c *Client) backoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := 2 * time.Second
	for i := 1; i < attempt && d < 15*time.Minute; i++ {
		d *= 2
	}
	if d > 15*time.Minute {
		d = 15 * time.Minute
	}
	return d
}

// logQueueStats — статистика очереди для протокола наблюдаемости
// (прототип: pending/done/failed на каждый цикл флаша).
func (c *Client) logQueueStats() {
	pending, bytes := c.buf.Pending()
	c.log.Info("очередь отправки",
		"в_очереди", pending,
		"байт", bytes,
		"отправлено_всего", c.sentTotal.Load(),
		"потеряно_всего", c.failedTotal.Load())
}
