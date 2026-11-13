package rules

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"chagent/internal/status"
)

var (
	uidRe     = regexp.MustCompile(`^ch\.[a-z0-9_]+\.[a-z0-9_]+$`)
	semverRe  = regexp.MustCompile(`^\d+\.\d+$`)
	atTokenRe = regexp.MustCompile(`(?i)\bAS\s+`)
)

var legalGroups = map[string]bool{
	"mutations_replication": true, "parts_disk": true, "queries_load": true,
	"memory_processes": true, "dbms_state": true, "background": true, "components": true,
}
var legalPriorities = map[string]bool{"P1": true, "P2": true, "P3": true}
var legalScopes = map[string]bool{"node": true, "saas": true}
var legalSources = map[string]bool{"builtin": true, "saas": true, "local": true}
var legalChannels = map[string]bool{"stable": true, "beta": true}
var forbiddenQueryTokens = []string{"clusterallreplicas", "cluster(", "$$"}

// Validate — проверки слоя (документация по использованию разд. 4).
// strict = true: строгий режим встроенного слоя (последовательность id, счёт self_rules).
// Возвращает список ошибок; слой с ошибками целиком отбрасывается вызывающей стороной.
func Validate(rs *Ruleset, strict bool) []string {
	var errs []string
	add := func(format string, a ...interface{}) {
		errs = append(errs, fmt.Sprintf(format, a...))
	}

	if rs.SchemaVersion != 1 {
		add("schema_version = %d, поддерживается 1", rs.SchemaVersion)
	}
	if rs.DatasetUID == "" {
		add("dataset_uid пуст")
	}
	if rs.DatasetVersion < 0 {
		add("dataset_version отрицательный: %d", rs.DatasetVersion)
	}
	if !legalChannels[rs.Channel] {
		add("channel %q вне алфавита (stable|beta)", rs.Channel)
	}

	seenUID := map[string]bool{}
	seenID := map[int]int{}
	idMax := 0
	for i, r := range rs.Rules {
		if r == nil {
			add("rules[%d]: nil", i)
			continue
		}
		tag := fmt.Sprintf("rule %s", r.UID)
		if r.UID == "" {
			tag = fmt.Sprintf("rules[%d]", i)
		}
		if r.UID == "" || !uidRe.MatchString(r.UID) {
			add("%s: uid пуст или не соответствует маске ch.<area>.<slug>", tag)
		} else if seenUID[r.UID] {
			add("%s: uid не уникален", tag)
		}
		seenUID[r.UID] = true

		if r.ID <= 0 {
			add("%s: id должен быть натуральным", tag)
		} else {
			if prev, dup := seenID[r.ID]; dup {
				add("%s: id %d повторяет правило %d", tag, r.ID, prev)
			}
			seenID[r.ID] = i
			if r.ID > idMax {
				idMax = r.ID
			}
		}
		if !legalGroups[r.Group] {
			add("%s: группа %q неизвестна", tag, r.Group)
		}
		if !legalPriorities[r.Priority] {
			add("%s: приоритет %q вне алфавита", tag, r.Priority)
		}
		if !legalScopes[r.Scope] {
			add("%s: scope %q вне алфавита (node|saas)", tag, r.Scope)
		}
		for _, a := range r.AppliesTo {
			if a != "standalone" && a != "replicated" && a != "shared" {
				add("%s: applies_to содержит %q вне алфавита", tag, a)
			}
		}
		if r.Since != "" && !semverRe.MatchString(r.Since) {
			add("%s: since %q не в формате мажор.минор", tag, r.Since)
		}
		if r.Until != "" && !semverRe.MatchString(r.Until) {
			add("%s: until %q не в формате мажор.минор", tag, r.Until)
		}
		if r.Since != "" && r.Until != "" {
			sv := versionPair(r.Since)
			uv := versionPair(r.Until)
			if sv > uv {
				add("%s: since %s позже until %s", tag, r.Since, r.Until)
			}
		}
		if r.Scope == "node" && strings.TrimSpace(r.Query) == "" {
			add("%s: пустой query при scope node", tag)
		}
		if !legalSources[r.ThresholdsSource] {
			add("%s: thresholds_source %q вне алфавита", tag, r.ThresholdsSource)
		}
		if r.ThresholdVersion < 0 {
			add("%s: отрицательный threshold_version", tag)
		}
		if r.EffInterval < time.Minute {
			add("%s: интервал %s короче одной минуты", tag, r.EffInterval)
		}
		if r.EffTimeout >= r.EffInterval {
			add("%s: таймаут %s не меньше интервала %s", tag, r.EffTimeout, r.EffInterval)
		}
		low := strings.ToLower(r.Query)
		for _, tok := range forbiddenQueryTokens {
			if strings.Contains(low, tok) {
				add("%s: запрещённый токен в query: %q", tag, tok)
			}
		}
		for j, th := range r.Thresholds {
			if th.Metric == "" {
				add("%s: thresholds[%d]: пустой metric", tag, j)
			}
			if !status.LegalOps[th.Op] {
				add("%s: thresholds[%d]: op %q вне алфавита", tag, j, th.Op)
			}
			if !status.LegalSeverities[th.Severity] {
				add("%s: thresholds[%d]: severity %q вне алфавита", tag, j, th.Severity)
			}
			if _, inMap := r.UnitMap[th.Metric]; !inMap && !metricInQuery(r.Query, th.Metric) {
				add("%s: порог по метрике %q отсутствует и в unit_map, и в query", tag, th.Metric)
			}
		}
		for _, t := range r.Tables {
			if !strings.HasPrefix(t, "system.") {
				add("%s: таблица %q вне префикса system.", tag, t)
			}
		}
	}

	if strict {
		for id := 1; id <= idMax; id++ {
			if _, ok := seenID[id]; !ok {
				add("strict: пропущен id %d (нет правила с этим номером)", id)
			}
		}
		if len(rs.SelfRules) != 11 {
			add("strict: self_rules = %d, ожидается 11", len(rs.SelfRules))
		}
	}

	for i, s := range rs.SelfRules {
		if s == nil {
			add("self_rules[%d]: nil", i)
			continue
		}
		if !strings.HasPrefix(s.UID, "ch.agent.") {
			add("self_rules[%d] %s: uid вне префикса ch.agent.", i, s.UID)
		}
		if s.Source != "internal" && s.Source != "clickhouse" {
			add("self_rules[%d] %s: source %q вне алфавита", i, s.UID, s.Source)
		}
		if s.Interval.Duration <= 0 {
			add("self_rules[%d] %s: нулевой интервал", i, s.UID)
		}
		for _, th := range s.Thresholds {
			if !status.LegalOps[th.Op] {
				add("self_rules[%d] %s: op %q вне алфавита", i, s.UID, th.Op)
			}
			if !status.LegalSeverities[th.Severity] {
				add("self_rules[%d] %s: severity %q вне алфавита", i, s.UID, th.Severity)
			}
		}
	}
	return errs
}

// metricInQuery — метрика объявлена колонкой запроса ('AS <metric>').
// Сравнение без учёта регистра: текст запроса уже переведён в верхний.
func metricInQuery(query, metric string) bool {
	if metric == "" {
		return false
	}
	upper := strings.ToUpper(query)
	upMetric := strings.ToUpper(metric)
	for _, m := range atTokenRe.FindAllStringIndex(upper, -1) {
		tail := upper[m[1]:]
		for _, sep := range []string{" ", "`", ",", "\n", "\t"} {
			if strings.HasPrefix(tail, upMetric+sep) {
				return true
			}
		}
	}
	return false
}

func versionPair(s string) int {
	var a, b int
	if _, err := fmt.Sscanf(s, "%d.%d", &a, &b); err != nil {
		return 0
	}
	return a*100 + b
}
