package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"chagent/internal/chclient"
	"chagent/internal/chversion"
	"chagent/internal/evaluate"
	"chagent/internal/rules"
	"chagent/internal/state"
)

// --- probe: здоровье ClickHouse и часы ---------------------------------------

var probeErrMax = 200

// probeLoop — цикл здоровья CH: SELECT-версия+время с экспоненциальным backoff
// (формат agent.yaml, блок behavior). Недоступный CH не генерирует очереди задач.
func (a *A) probeLoop(ctx context.Context) {
	for {
		p, err := a.client.Probe(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			a.onProbeOK(ctx, p)
		} else {
			a.onProbeFail(ctx, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.probe.currentBackoff()):
		}
	}
}

func (p *probeCache) currentBackoff() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.nextBackoff <= 0 {
		return 10 * time.Second
	}
	return p.nextBackoff
}

func (p *probeCache) setMode(m string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m != "" && p.mode == "" {
		p.mode = m
	}
}

// Mode — обнаруженный режим репликации ("" до первого детекта).
func (p *probeCache) Mode() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

func (p *probeCache) setDown(down bool, errStr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		p.downSince = time.Now()
		p.probeError = errStr
	} else {
		p.downSince = time.Time{}
		p.probeError = ""
	}
}

// onProbeOK — успешный probe: обновление кэша версии/времени; при первом
// успехе или смене версии/режима — перепубликация плана.
func (a *A) onProbeOK(ctx context.Context, p *chclient.Probe) {
	a.probe.mu.Lock()
	wasDown := a.probe.down
	wasProbed := a.probe.ok
	versionChanged := a.probe.ver != p.Version
	needMode := a.probe.mode == ""
	a.probe.mu.Unlock()

	a.probe.mu.Lock()
	a.probe.ok = true
	a.probe.ver = p.Version
	a.probe.dbUnix = p.DBUnix
	a.probe.nextBackoff = a.cfg.Behavior.CHProbeInterval.Duration
	a.probe.mu.Unlock()

	if wasDown {
		downtime := time.Since(a.probeDownSince())
		a.probe.setDown(false, "")
		a.log.Info("clickhouse восстановлен", "downtime_seconds", downtime.Seconds())
		a.postEvent(ctx, map[string]any{
			"event": "ch_recovered", "downtime_seconds": downtime.Seconds(),
		})
	}
	mode := ""
	if needMode {
		mode = a.detectMode(ctx)
		if mode != "" {
			a.probe.setMode(mode)
		}
	}
	if !wasProbed || versionChanged || mode != "" {
		if c := a.cat.Load(); c != nil {
			a.publish(ctx, c, "probe: обновление окружения (версия/режим)")
		}
	}
}

func (a *A) probeDownSince() time.Time {
	a.probe.mu.Lock()
	defer a.probe.mu.Unlock()
	return a.probe.downSince
}

// onProbeFail — отказ probe: режим down + события ch_unreachable (edge + повтор раз в behavior.unreachable_alert_repeat).
func (a *A) onProbeFail(ctx context.Context, err error) {
	msg := err.Error()
	if len(msg) > probeErrMax {
		msg = msg[:probeErrMax]
	}
	was := a.probe.isDown()
	a.probe.setDown(true, msg)

	interval := a.cfg.Behavior.CHProbeInterval.Duration
	mult := a.cfg.Behavior.BackoffMultiplier
	maxb := a.cfg.Behavior.BackoffMax.Duration
	a.probe.mu.Lock()
	if a.probe.nextBackoff <= 0 {
		a.probe.nextBackoff = interval
	}
	nb := time.Duration(float64(a.probe.nextBackoff) * mult)
	if mult < 1 {
		nb = interval
	}
	if nb > maxb {
		nb = maxb
	}
	a.probe.nextBackoff = nb
	a.probe.mu.Unlock()

	if !was {
		a.log.Warn("clickhouse недоступен", "err", msg)
		a.postEvent(ctx, map[string]any{
			"event": "ch_unreachable", "since": time.Now().UTC().Format(time.RFC3339), "probe_error": msg,
		})
		a.lastUnreachAlert.Store(time.Now())
		return
	}
	last := time.Time{}
	if v := a.lastUnreachAlert.Load(); v != nil {
		if t, ok := v.(time.Time); ok {
			last = t
		}
	}
	if last.IsZero() || time.Since(last) >= a.cfg.Behavior.UnreachableAlertRepeat.Duration {
		a.postEvent(ctx, map[string]any{
			"event": "ch_unreachable", "since": probeTimeFormat(a.probeDownSince()), "probe_error": msg,
		})
		a.lastUnreachAlert.Store(time.Now())
	}
}

func probeTimeFormat(t time.Time) string {
	if t.IsZero() {
		return time.Now().UTC().Format(time.RFC3339)
	}
	return t.UTC().Format(time.RFC3339)
}

// detectMode — самодетект режима репликации: system.replicas непуст → replicated.
func (a *A) detectMode(ctx context.Context) string {
	mode := strings.TrimSpace(a.cfg.Clickhouse.ReplicationMode)
	if mode != "" {
		return mode // значение инсталлятора имеет приоритет
	}
	rows, err := a.client.Query(ctx, "SELECT count() AS cnt FROM system.replicas")
	if err != nil || len(rows) == 0 {
		return "standalone"
	}
	if v, ok := evaluate.CoerceFloat(rows[0]["cnt"]); ok && v > 0 {
		return "replicated"
	}
	return "standalone"
}

// --- регистрация и статус клиента --------------------------------------------

// registerLoop: пока нет client_id — регистрация каждые registerRetryInterval.
// Один начальный register выполняется ВСЕГДА (обновление инвентаря), в т.ч. когда
// client_id задан per-client из config.yaml (сервер вернёт тот же client_id).
func (a *A) registerLoop(ctx context.Context) {
	first := true
	for {
		if ctx.Err() != nil {
			return
		}
		if a.st.Get().ClientID == "" || first {
			cid, err := a.ing.Register(ctx, a.inventoryPayload())
			if err == nil && cid != "" {
				a.st.Mutate(func(d *state.Data) { d.ClientID = cid })
				a.log.Info("зарегистрирован в SaaS", "client_id", cid)
				a.flushHeld()
			} else if err != nil && ctx.Err() == nil {
				a.log.Debug("регистрация не удалась, повтор", "err", err.Error())
			}
			first = false
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(registerRetryInterval):
		}
	}
}

// inventoryPayload — payload регистрации (инвентарь ноды).
func (a *A) inventoryPayload() map[string]any {
	ver := ""
	if a.probe.online() {
		ver = a.probe.version().String()
	}
	if ver == "" {
		ver = a.cfg.Clickhouse.Version
	}
	mode := a.probe.Mode()
	if mode == "" {
		mode = strings.TrimSpace(a.cfg.Clickhouse.ReplicationMode)
	}
	if mode == "" {
		mode = "standalone"
	}
	return map[string]any{
		"agent": map[string]any{
			"agent_uuid":    a.st.Get().AgentUUID,
			"agent_version": a.agentVer,
			"tenant_id":     a.cfg.Identity.TenantID,
		},
		"node":        a.cfg.Node,
		"clickhouse": map[string]any{
			"version":          ver,
			"cluster_names":    a.cfg.Clickhouse.ClusterNames,
			"replication_mode": mode,
			"tcp_port":         a.cfg.Clickhouse.TCPPort,
			"http_port":        a.cfg.Clickhouse.HTTPPort,
		},
		"labels":      a.cfg.Labels,
		"captured_at": time.Now().UTC().Format(time.RFC3339),
	}
}

// statusLoop — опрос статуса клиента (требование 8): актив/спящий режим/ms grace.
func (a *A) statusLoop(ctx context.Context) {
	interval := a.cfg.Status.PollInterval.Duration
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	grace := a.cfg.Status.CacheState.Duration
	var unknownSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		if a.st.Get().ClientID == "" {
			continue
		}
		st, err := a.ing.ClientStatus(ctx)
		switch {
		case err != nil:
			if unknownSince.IsZero() {
				unknownSince = time.Now()
			}
			if time.Since(unknownSince) > grace {
				a.transition(ctx, "suspended") // failsafe против «зомби-агентов»
			} else {
				a.transition(ctx, "unknown")
			}
			continue
		default:
			unknownSince = time.Time{}
			switch st {
			case "active", "maintenance":
				a.transition(ctx, st)
			case "suspended", "revoked", "trial_expired":
				a.transition(ctx, "suspended")
			default:
				a.transition(ctx, "unknown")
			}
		}
	}
}

// transition — смена состояния SaaS с событиями dormant_entered/exited.
func (a *A) transition(ctx context.Context, to string) {
	prev, _ := a.saasDyn.Load().(string)
	if to == prev {
		return
	}
	a.saasDyn.Store(to)

	dormant := func(s string) bool {
		return s == "suspended" || s == "revoked" || s == "trial_expired"
	}
	prevDorm, newDorm := dormant(prev), dormant(to)
	a.pausedA.Store(newDorm)

	a.log.Info("состояние клиента SaaS", "было", prev, "стало", to, "сбор", pauseWord(newDorm))
	switch {
	case !prevDorm && newDorm:
		a.postEvent(ctx, map[string]any{"event": "dormant_entered", "state": to})
	case prevDorm && !newDorm:
		a.postEvent(ctx, map[string]any{"event": "dormant_exited"})
	}
}

func pauseWord(b bool) string {
	if b {
		return "приостановлен"
	}
	return "активен"
}

// --- синхронизация правил -----------------------------------------------------

// syncLoop — цикл сверка набора правил с SaaS (ФТ-4, документация по использованию разд. 12).
func (a *A) syncLoop(ctx context.Context) {
	interval := a.cfg.Sync.PollInterval.Duration
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		if paused, _ := a.saasDyn.Load().(string); paused == "suspended" || paused == "revoked" || paused == "trial_expired" {
			continue // спящий режим: сверка бессмысленна
		}
		a.syncOnce(ctx)
	}
}

func (a *A) syncOnce(ctx context.Context) {
	etag := ""
	winner := a.cat.Load()
	if winner != nil {
		etag = winner.Meta.DatasetHash
	}
	body, changed, err := a.ing.FetchRules(ctx, etag)
	if err != nil {
		a.lastFetchStatus.Store(2)
		a.log.Debug("sync: сверка не удалась", "err", err.Error())
		return
	}
	if !changed {
		a.lastFetchStatus.Store(1)
		a.lastFetchOK.Store(time.Now().Unix())
		return
	}

	layer, err := parseAndValidateLayer(body)
	if err != nil {
		a.lastFetchStatus.Store(2)
		a.log.Error("sync: скачанный слой отброшен", "err", err.Error())
		a.postEvent(ctx, map[string]any{"event": "config_invalid", "cause": err.Error()})
		return
	}

	// канал: агент применяет только канал sync.channel
	if layer.Channel != a.cfg.Sync.Channel {
		a.log.Info("sync: слой канала не совпал, отложен", "layer_channel", layer.Channel,
			"expect", a.cfg.Sync.Channel)
		a.lastFetchStatus.Store(1)
		a.lastFetchOK.Store(time.Now().Unix())
		return
	}
	// min_agent_version: слой старше способностей агента — отложить до апгрейда
	if minV, perr := chversion.Parse(layer.MinAgentVersion); perr == nil {
		if agV, aerr := chversion.Parse(a.agentVer); aerr == nil && chversion.Cmp(minV, agV) > 0 {
			a.log.Warn("sync: набор требует более новую версию агента, отложен",
				"min_agent_version", layer.MinAgentVersion, "agent_version", a.agentVer)
			a.lastFetchStatus.Store(1)
			a.lastFetchOK.Store(time.Now().Unix())
			return
		}
	}
	// ранние версии не применяются повторно
	if winner != nil && layer.DatasetVersion <= winner.Meta.DatasetVersion {
		a.lastFetchStatus.Store(1)
		a.lastFetchOK.Store(time.Now().Unix())
		return
	}

	if err := a.persistSaasLayer(body); err != nil {
		a.log.Warn("sync: сохранение скачанного слоя", "err", err.Error())
	}

	old := a.cat.Load()
	cat, err := rules.BuildCatalog(a.emb, layer, a.ov)
	if err != nil {
		a.lastFetchStatus.Store(2)
		a.log.Error("sync: слияние слоёв не удалось", "err", err.Error())
		return
	}
	diff := cat.DiffAgainst(old)
	a.lastFetchStatus.Store(0)
	a.lastFetchOK.Store(time.Now().Unix())
	a.publish(ctx, cat, fmt.Sprintf("saas: dataset_version %d", layer.DatasetVersion))

	a.log.Info("набор правил обновлён",
		"added", len(diff.Added), "removed", len(diff.Removed),
		"thresholds_changed", len(diff.ThresholdsChanged))
	a.postEvent(ctx, map[string]any{
		"event":              "rules_updated",
		"from_version":       diff.VersionFrom,
		"to_version":         diff.VersionTo,
		"added":              diff.Added,
		"removed":            diff.Removed,
		"thresholds_changed": diff.ThresholdsChanged,
	})
}

// persistSaasLayer — материал слоя 2: rules_saas.yaml (слой 3 rules.yaml не трогается).
// Ротация: предыдущие версии сдвигаются в .bak-1…​.bak-5 (документация по использованию разд. 12).
func (a *A) persistSaasLayer(body []byte) error {
	dir := strings.TrimSpace(a.cfg.Storage.RulesDir)
	if dir == "" {
		dir = filepath.Dir(a.cfg.Storage.StateFile)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	target := filepath.Join(dir, "rules_saas.yaml")
	rotateBaks(target, 5)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

// rotateBaks — старшая копия .bak-5 удаляется, каждая младшая сдвигается на
// шаг вперёд, текущий файл перезаписывается с позиции .bak-1.
func rotateBaks(target string, n int) {
	if n < 1 {
		return
	}
	_ = os.Remove(fmt.Sprintf("%s.bak-%d", target, n))
	for i := n - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.bak-%d", target, i)
		if _, err := os.Stat(src); err != nil {
			continue
		}
		_ = os.Rename(src, fmt.Sprintf("%s.bak-%d", target, i+1))
	}
	if _, err := os.Stat(target); err == nil {
		_ = os.Rename(target, target+".bak-1")
	}
}

// --- доступ -------------------------------------------------------------------

// accessLoop — переоценка закрытых ранее таблиц по behavior.access_recheck_interval.
// Изменение состава закрытых таблиц (потеря/возврат прав после инцидента)
// уходит событием access_changed: DBA видит момент деградации в health_log,
// при этом правила по остальным таблицам продолжают выгрузку штатно.
func (a *A) accessLoop(ctx context.Context) {
	interval := a.cfg.Behavior.AccessRecheckInterval.Duration
	if interval <= 0 {
		interval = 30 * time.Minute
	}
	var prevDenied []string
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		if !a.probe.online() {
			continue
		}
		fixed := a.acc.RefreshDenied(ctx, a.client)
		if fixed > 0 {
			a.log.Info("доступ восстановлен для таблиц", "count", fixed,
				"denied", len(a.acc.DeniedTables()))
		}
		cur := a.acc.DeniedTables()
		first := prevDenied == nil
		if first && len(cur) == 0 {
			prevDenied = cur
			continue
		}
		if first || !sameStrings(prevDenied, cur) {
			restored := make([]string, 0)
			for _, t := range prevDenied {
				if !contains(cur, t) {
					restored = append(restored, t)
				}
			}
			a.postEvent(ctx, map[string]any{
				"event":           "access_changed",
				"denied_tables":   cur,
				"restored_tables": restored,
			})
		}
		prevDenied = cur
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}
