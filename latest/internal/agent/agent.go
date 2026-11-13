// Package agent: оркестратор — склейка всех подсистем агента
// (документация по использованию разд. 5–14). Циклы жизнедеятельности — в файле loops.go.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"chagent/internal/access"
	"chagent/internal/buffer"
	"chagent/internal/chclient"
	"chagent/internal/chversion"
	"chagent/internal/config"
	"chagent/internal/plan"
	"chagent/internal/runner"
	"chagent/internal/rules"
	"chagent/internal/saasclient"
	"chagent/internal/scheduler"
	"chagent/internal/selfloop"
	"chagent/internal/state"
	"chagent/internal/status"
)

const registerRetryInterval = 5 * time.Minute

// probeCache — снимок последнего успешного probe-запроса и здоровья ClickHouse.
type probeCache struct {
	mu          sync.Mutex
	ok          bool
	down        bool
	downSince   time.Time
	ver         string
	mode        string
	dbUnix      int64
	probeError  string
	nextBackoff time.Duration
}

func (p *probeCache) isDown() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.down
}

func (p *probeCache) online() bool { return !p.isDown() }

func (p *probeCache) version() chversion.V {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, err := chversion.Parse(p.ver)
	if err != nil {
		return chversion.V{Major: 21, Minor: 8}
	}
	return v
}

func (p *probeCache) dbTime() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ok || p.dbUnix == 0 {
		return ""
	}
	return time.Unix(p.dbUnix, 0).UTC().Format(time.RFC3339)
}

// A — агент целиком.
type A struct {
	cfg  config.Config
	log  *slog.Logger
	st   *state.Store
	emb  *rules.Ruleset
	ov   *rules.Overlay

	cat   atomic.Pointer[rules.Catalog]
	planP atomic.Pointer[plan.Plan]

	client *chclient.Client
	acc    *access.Cache
	probe  probeCache

	sched   *scheduler.S
	results chan runner.Out
	ing     *saasclient.Client
	self    *selfloop.S

	pausedA    atomic.Bool
	saasDyn    atomic.Value // состояние SaaS: active | maintenance | suspended | ...
	agentVer   string

	heldMu       sync.Mutex
	heldNoClient []map[string]any // результаты, удержанные до появления client_id

	lastUnreachAlert atomic.Value // time.Time последней рассылки «CH недоступен»

	// источники self-метрик:
	lastFetchStatus atomic.Int64 // 0 ok / 1 no-change / 2 error
	lastFetchOK     atomic.Int64 // unix последней успешной сверки правил
	yamlUpdatedAt   atomic.Int64 // когда публиковался каталог после смены слоя
	rowsCollected   atomic.Int64 // суммарно строк за жизнь (ch.agent.work.rows_collected_total)
}

// New собирает агента (конфиг считается уже загруженным и валидным).
func New(cfg config.Config, log *slog.Logger, emb *rules.Ruleset, ov *rules.Overlay, stPath string,
	cli *config.ClientFile) (*A, error) {
	st, err := state.Load(stPath)
	if err != nil {
		return nil, err
	}
	// per-client provisioning (config.yaml): client_id обязателен к использованию
	// сразу — state.json перекрывается файлом (файл — источник правды о клиенте)
	if cli != nil && cli.ClientID != "" {
		st.Mutate(func(d *state.Data) { d.ClientID = cli.ClientID })
		log.Info("клиент определён из config.yaml", "client_id", cli.ClientID)
	}

	host := strings.TrimSpace(cfg.Clickhouse.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	dial := cfg.Behavior.BackoffInitial.Duration
	if cfg.Clickhouse.ConnectionTimeout.Duration > 0 {
		dial = cfg.Clickhouse.ConnectionTimeout.Duration
	}
	client := chclient.New(host, cfg.Clickhouse.HTTPPort, cfg.Clickhouse.User,
		cfg.CHPassword(), dial)

	bufDir := strings.TrimSpace(cfg.Storage.BufferDir)
	if bufDir == "" {
		bufDir = filepath.Join(filepath.Dir(cfg.Storage.StateFile), "buffer")
	}
	bf, err := buffer.New(bufDir, cfg.Storage.BufferMaxBytes)
	if err != nil {
		return nil, err
	}

	version := strings.TrimSpace(cfg.Identity.AgentVersion)
	if version == "" {
		version = "1.2.0"
	}
	// соль подписи: per-client (config.yaml client_salt) или fleet (api_key_ref)
	clientSalt := cfg.Identity.APIKey()
	if cli != nil && cli.ClientSalt != "" {
		clientSalt = cli.ClientSalt
	}

	results := make(chan runner.Out, 512)
	ing := saasclient.New(log, cfg.Send, cfg.Sync, cfg.Status, clientSalt, version, st, bf)
	sl := selfloop.New(log, ing)

	a := &A{
		cfg:     cfg,
		log:     log,
		st:      st,
		emb:     emb,
		ov:      ov,
		client:  client,
		acc:     access.New(),
		results: results,
		ing:     ing,
		self:    sl,
		agentVer: version,
	}
	a.probe.nextBackoff = cfg.Behavior.BackoffInitial.Duration
	a.probe.nextBackoffMuFix()
	// поле db_time конверта — кеш времени ClickHouse из probe-запроса
	ing.SetDBTime(a.probe.dbTime)

	s2 := scheduler.New(log, runner.Deps{
		Client:     client,
		Access:     a.acc,
		MaxRows:    emb.Defaults.MaxRowsPerRule,
		MaxCell:    emb.Defaults.MaxCellBytes,
		DefaultQS:  emb.Defaults.QuerySettings,
		MaxThreads: cfg.Limits.MaxThreadsPerQuery,
	}, cfg.Limits,
		func() bool { return a.probe.isDown() },
		func() bool { return a.pausedA.Load() },
		results)
	a.sched = s2

	if cfg.Identity.InsecureSkipVerify {
		log.Warn("identity.insecure_skip_verify = true: проверка TLS-сертификатов SaaS отключена (только тестовые стенды)")
	}
	return a, nil
}

// nextBackoffMuFix — резерв: файловая структура времени backoff ведётся в loops.go
// через локальные переменные; здесь лишь гарантируем ненулевое стартовое значение.
func (p *probeCache) nextBackoffMuFix() {
	if p.nextBackoff <= 0 {
		p.nextBackoff = 10 * time.Second
	}
}

// RegisterSelfGauges — привязка поставщиков self-метрик к их uid.
// Реестр закрыт 11 uid встроенного файла; новые self-метрики кодом запрещены.
func (a *A) RegisterSelfGauges() {
	a.self.SetGauge("ch.agent.rules.dataset_version", func() float64 {
		if c := a.cat.Load(); c != nil {
			return float64(c.Meta.DatasetVersion)
		}
		return 0
	})
	a.self.SetGauge("ch.agent.rules.dataset_age_hours", func() float64 {
		c := a.cat.Load()
		if c == nil || c.Meta.ReleasedAt == "" {
			return 0
		}
		t, err := time.Parse("2006-01-02", c.Meta.ReleasedAt)
		if err != nil {
			return 0
		}
		return time.Since(t).Hours()
	})
	a.self.SetGauge("ch.agent.rules.last_fetch_status", func() float64 {
		return float64(a.lastFetchStatus.Load())
	})
	a.self.SetGauge("ch.agent.rules.last_fetch_age_hours", func() float64 {
		u := a.lastFetchOK.Load()
		if u == 0 {
			return 0
		}
		return time.Since(time.Unix(u, 0)).Hours()
	})
	a.self.SetGauge("ch.agent.ch.unreachable_seconds", func() float64 {
		a.probe.mu.Lock()
		defer a.probe.mu.Unlock()
		if a.probe.down && !a.probe.downSince.IsZero() {
			return time.Since(a.probe.downSince).Seconds()
		}
		return 0
	})
	a.self.SetGauge("ch.agent.access.denied_tables_count", func() float64 {
		return float64(a.acc.DeniedCount())
	})
	a.self.SetGauge("ch.agent.access.last_change_hours", func() float64 {
		return a.acc.LastChangeHours()
	})
	a.self.SetGauge("ch.agent.status", func() float64 {
		switch a.effectiveAgentState() {
		case "dormant":
			return 1
		case "no_client":
			return 2
		}
		return 0
	})
	a.self.SetGauge("ch.agent.time.drift_ms", func() float64 {
		a.probe.mu.Lock()
		defer a.probe.mu.Unlock()
		if !a.probe.ok || a.probe.dbUnix == 0 {
			return 0
		}
		d := float64(a.probe.dbUnix*1000) - float64(time.Now().UnixMilli())
		if d < 0 {
			d = -d
		}
		return d
	})
	a.self.SetGauge("ch.agent.rules.yaml_updated_at", func() float64 {
		return float64(a.yamlUpdatedAt.Load())
	})
	a.self.SetGauge("ch.agent.work.rows_collected_total", func() float64 {
		return float64(a.rowsCollected.Load())
	})
}

// effectiveAgentState — режим активности: no_client старше dormant.
func (a *A) effectiveAgentState() string {
	if a.st.Get().ClientID == "" {
		return "no_client"
	}
	switch st, _ := a.saasDyn.Load().(string); st {
	case "suspended", "revoked", "trial_expired":
		return "dormant"
	}
	return "active"
}

// publish publishes каталог, пересобирает план и обновляет зависимости подсистем.
func (a *A) publish(ctx context.Context, cat *rules.Catalog, why string) {
	a.cat.Store(cat)
	a.st.Mutate(func(d *state.Data) {
		d.Ruleset = state.Ruleset{
			DatasetVersion: cat.Meta.DatasetVersion,
			DatasetHash:    cat.Meta.DatasetHash,
			ReleasedAt:     cat.Meta.ReleasedAt,
			Source:         cat.Meta.Source,
		}
	})
	a.self.SetRules(cat.SelfRules)
	a.yamlUpdatedAt.Store(time.Now().Unix())

	p := plan.Build(cat, a.planEnv())
	a.planP.Store(p)
	if a.sched != nil {
		a.sched.Update(cat, p.Runnables)
	}

	a.log.Info("каталог опубликован",
		"источник", why,
		"dataset_version", cat.Meta.DatasetVersion,
		"исполняемых", len(p.Runnables),
		"статических", len(p.Static),
		"прогноз_исполнений_в_сутки", p.ForecastPerDay)
	if p.ForecastPerDay > a.cfg.Limits.DailyQueryBudget {
		a.log.Warn("прогноз исполнений превышает дневной бюджет: повысить limits.daily_query_budget или частоты правил",
			"forecast", p.ForecastPerDay, "budget", a.cfg.Limits.DailyQueryBudget)
	}

	// доступность таблиц исполняемых правил — сразу после публикации
	if a.probe.online() && len(p.Runnables) > 0 {
		tables := make([]string, 0, 32)
		for _, r := range p.Runnables {
			tables = append(tables, r.Tables...)
		}
		ctxT, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := a.acc.EnsureKnown(ctxT, a.client, tables); err != nil {
			a.log.Debug("первичный кэш доступов не построен", "err", err.Error())
		}
		cancel()
	}
}

// planEnv — окружение планирования: версия из probe, иначе заявленная в конфиге.
func (a *A) planEnv() plan.Env {
	minV, err := chversion.Parse(a.cfg.Limits.MinCHVersion)
	if err != nil {
		minV = chversion.V{Major: 21, Minor: 8}
	}
	ver := a.probe.version()
	if !a.probe.online() {
		if v, perr := chversion.Parse(a.cfg.Clickhouse.Version); perr == nil {
			ver = v
		}
	}
	mode := a.probe.Mode()
	if mode == "" {
		mode = strings.TrimSpace(a.cfg.Clickhouse.ReplicationMode)
	}
	if mode == "" {
		mode = "standalone"
	}
	return plan.Env{Version: ver, MinVersion: minV, Mode: mode}
}

// Run — главный цикл до отмены контекста.
func (a *A) Run(ctx context.Context) error {
	cat, err := rules.BuildCatalog(a.emb, nil, a.ov)
	if err != nil {
		return fmt.Errorf("agent: слияние слоёв: %w", err)
	}
	a.RegisterSelfGauges()
	a.publish(ctx, cat, "embedded+local")

	a.self.Run(ctx)
	a.ing.StartFlusher(ctx)
	go a.sched.Run(ctx)
	go a.pump(ctx)
	go a.probeLoop(ctx)
	go a.registerLoop(ctx)
	go a.statusLoop(ctx)
	go a.syncLoop(ctx)
	go a.accessLoop(ctx)

	a.log.Info("агент запущен",
		"agent_uuid", a.st.Get().AgentUUID,
		"dataset_version", cat.Meta.DatasetVersion,
		"ch_http", fmt.Sprintf("127.0.0.1:%d", a.cfg.Clickhouse.HTTPPort),
		"saas", a.cfg.Send.EndpointBase)
	<-ctx.Done()
	return ctx.Err()
}

// pump — приём результатов из планировщика.
func (a *A) pump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case out, ok := <-a.results:
			if !ok {
				return
			}
			a.handleResult(ctx, out)
		}
	}
}

// handleResult — конверсия в payload протокола, edge-detection статусов, доставка.
func (a *A) handleResult(ctx context.Context, out runner.Out) {
	if out.Rows > 0 {
		a.rowsCollected.Add(int64(out.Rows))
	}
	raw, merr := json.Marshal(out)
	if merr != nil {
		a.log.Error("pump: сериализация результата", "uid", out.UID, "err", merr.Error())
		return
	}
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		return
	}
	// статусы-переходы отправляются только при смене (документация по использованию разд. 10).
	// Порядок «очередь → штамп состояния»: запись попадает на диск до
	// отметки в state.json, поэтому крах между шагами даёт повтор (не потерю);
	// при обратном порядке событие терялось навсегда.
	if status.IsStatic(out.Status) {
		if a.st.Get().LastStatuses[out.UID] == out.Status {
			return
		}
		a.deliver(ctx, out.UID, rec)
		a.st.Mutate(func(d *state.Data) { d.LastStatuses[out.UID] = out.Status })
		return
	}
	a.deliver(ctx, out.UID, rec)
}

// deliver — удержание до client_id либо персистентная очередь отправки.
func (a *A) deliver(ctx context.Context, uid string, rec map[string]any) {
	_ = ctx
	cid := a.st.Get().ClientID
	if cid == "" {
		a.heldMu.Lock()
		if len(a.heldNoClient) < 4096 {
			a.heldNoClient = append(a.heldNoClient, rec)
			a.heldMu.Unlock()
			return
		}
		a.heldMu.Unlock()
		a.log.Warn("pump: буфер held переполнен, результат отброшен", "uid", uid)
		return
	}
	a.ing.Enqueue(a.ingestPath(cid, uid), rec)
	a.flushHeld()
}

func (a *A) flushHeld() {
	a.heldMu.Lock()
	held := a.heldNoClient
	a.heldNoClient = nil
	a.heldMu.Unlock()
	cid := a.st.Get().ClientID
	for _, rec := range held {
		uid, _ := rec["uid"].(string)
		a.ing.Enqueue(a.ingestPath(cid, uid), rec) // путь пересчитывается с реальным client_id
	}
}

// ingestPath — шаблон send.ingest_path_template с подстановками.
func (a *A) ingestPath(clientID, uid string) string {
	p := a.cfg.Send.IngestPathTemplate
	if p == "" {
		p = "/v2/ingest/{{client_id}}/{{rule_uid}}"
	}
	if clientID == "" {
		clientID = "unknown"
	}
	p = strings.ReplaceAll(p, "{{client_id}}", clientID)
	p = strings.ReplaceAll(p, "{{rule_uid}}", uid)
	return p
}

// postEvent — событие в канал /v2/agent/events через персистентную очередь:
// как и инжест правил, переживает рестарт и недоступность приёма.
func (a *A) postEvent(ctx context.Context, rec map[string]any) {
	a.ing.Enqueue(a.ing.EventsPath(), rec)
}
