// Package runner: исполнение одного правила против ClickHouse (документация по использованию разд. 8).
package runner

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"chagent/internal/access"
	"chagent/internal/chclient"
	"chagent/internal/evaluate"
	"chagent/internal/rules"
	"chagent/internal/status"
)

// Metric — колонка результата как метрика.
type Metric struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
	Unit  string `json:"unit,omitempty"`
}

// ErrInfo — ошибка исполнения (SQL-текст наружу не выносится, ФТ-15.2).
type ErrInfo struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Out — запись результата правила; JSON-теги повторяют payload протокола
// (протокол обмена с сервером, разд. 3).
type Out struct {
	UID        string           `json:"uid"`
	ID         int              `json:"id"`
	Status     string           `json:"status"`
	EvaluatedAt string          `json:"evaluated_at"`
	Interval   string           `json:"interval"`
	DurationMS int64            `json:"duration_ms"`
	Rows       int              `json:"rows_scanned"`
	Metrics    []Metric         `json:"metrics"`
	Thresholds map[string]any   `json:"thresholds_used"`
	Ruleset    map[string]string `json:"ruleset"`
	Err        *ErrInfo         `json:"error"`
	Sample     []map[string]any `json:"sample"`
}

// Deps — общие зависимости исполнения.
type Deps struct {
	Client        *chclient.Client
	Access        *access.Cache
	MaxRows       int    // defaults.max_rows_per_rule
	MaxCell       int    // defaults.max_cell_bytes
	DefaultQS     string // defaults.query_settings
	MaxThreads    int    // limits.max_threads_per_query
}

var reMaxThreads = regexp.MustCompile(`(?i)^\s*max_threads\s*=`)

// withSettings приклейка SETTINGS с клампом max_threads.
func withSettings(sql, qs string, maxThreads int) string {
	var parts []string
	hadThreads := false
	for _, p := range strings.Split(qs, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if reMaxThreads.MatchString(p) {
			parts = append(parts, fmt.Sprintf("max_threads=%d", maxThreads))
			hadThreads = true
			continue
		}
		parts = append(parts, p)
	}
	if !hadThreads {
		parts = append(parts, fmt.Sprintf("max_threads=%d", maxThreads))
	}
	if len(parts) == 0 {
		return sql
	}
	return sql + " SETTINGS " + strings.Join(parts, ", ")
}

// failure helper
func fail(out *Out, st string, ce *chclient.Error) Out {
	out.Status = st
	if ce != nil {
		out.Err = &ErrInfo{Code: ce.Code, Message: ce.Msg}
	}
	return *out
}

// Run исполняет правило и формирует запись результата.
// meta — снимок слоя-победителя на момент постановки задачи (аудит порогов).
func Run(ctx context.Context, d Deps, r *rules.Rule, meta rules.Meta) Out {
	start := time.Now()
	out := Out{
		UID:      r.UID,
		ID:       r.ID,
		Interval: r.EffInterval.String(),
		Thresholds: map[string]any{
			"threshold_version": r.ThresholdVersion,
			"thresholds_source": r.ThresholdsSource,
		},
		Ruleset: map[string]string{
			"dataset_version": fmt.Sprintf("%d", meta.DatasetVersion),
			"dataset_hash":    meta.DatasetHash,
		},
	}
	defer func() {
		out.DurationMS = time.Since(start).Milliseconds()
		out.EvaluatedAt = start.UTC().Format(time.RFC3339)
	}()

	qs := r.QuerySettings
	if strings.TrimSpace(qs) == "" {
		qs = d.DefaultQS
	}
	maxThreads := d.MaxThreads
	if maxThreads <= 0 {
		maxThreads = 2
	}
	maxRows := d.MaxRows
	if maxRows <= 0 {
		maxRows = 10
	}
	maxCell := d.MaxCell
	if maxCell <= 0 {
		maxCell = 4096
	}
	sql := withSettings(r.Query, qs, maxThreads)

	attempts := r.EffRetry.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	backoff := r.EffRetry.BackoffInitial.Duration
	mult := r.EffRetry.BackoffMultiplier
	if mult < 1 {
		mult = 2
	}

	var rows []chclient.Row
	for a := 1; ; a++ {
		got, err := d.Client.Query(ctx, sql)
		if err == nil {
			rows = got
			break
		}
		ce, ok := chclient.AsError(err)
		if !ok {
			ce = &chclient.Error{Class: chclient.ErrSQL, Msg: truncate(err.Error(), 200)}
		}
		switch ce.Class {
		case chclient.ErrTimeout:
			return fail(&out, status.Timeout, ce)
		case chclient.ErrTableMissing:
			markTables(d, r, ce)
			return fail(&out, status.SkippedTableMissing, ce)
		case chclient.ErrNoAccess:
			markTables(d, r, ce)
			return fail(&out, status.SkippedNoAccess, ce)
		case chclient.ErrNet:
			if a < attempts {
				d := backoff
				for k := 1; k < a && d < time.Hour; k++ {
					d = time.Duration(float64(d) * mult)
				}
				select {
				case <-ctx.Done():
					return fail(&out, status.ErrorQuery, ce)
				case <-time.After(d):
				}
				continue
			}
			// сеть/CH продолжают отказывать — переписываем как исполнение с ошибкой
			return fail(&out, status.ErrorQuery, ce)
		default:
			return fail(&out, status.ErrorQuery, ce)
		}
	}

	out.Rows = len(rows)
	if len(rows) > 0 {
		first := rows[0]
		out.Metrics = make([]Metric, 0, len(first))
		for k, v := range first {
			m := Metric{Name: k, Value: v}
			if u, ok := r.UnitMap[k]; ok {
				m.Unit = u
			}
			out.Metrics = append(out.Metrics, m)
		}
		out.Sample = buildSample(rows, r, maxRows, maxCell)
	}

	if r.Scope == "saas" {
		// пороги не вычисляются агентом: сравнение делает SaaS (правила-снимки 43+).
		out.Status = status.None
		return out
	}
	var row0 chclient.Row
	if len(rows) > 0 {
		row0 = rows[0]
	}
	out.Status = evaluate.EvalStatus(r.Thresholds, row0)
	return out
}

func markTables(d Deps, r *rules.Rule, ce *chclient.Error) {
	for _, t := range r.Tables {
		d.Access.MarkOnErr(t, ce.Class)
	}
}

// buildSample — сырые строки для человека: лимиты, маски, обрезка ячеек.
func buildSample(rows []chclient.Row, r *rules.Rule, maxRows, maxCell int) []map[string]any {
	redact := map[string]bool{}
	for _, c := range r.RedactColumns {
		redact[c] = true
	}
	every := 1
	if r.Sampling != nil && r.Sampling.Enabled && r.Sampling.EveryNthRow > 0 {
		every = r.Sampling.EveryNthRow
	}
	out := make([]map[string]any, 0, maxRows)
	for i, row := range rows {
		if len(out) >= maxRows {
			break
		}
		if (i % every) != 0 {
			continue
		}
		cp := make(map[string]any, len(row))
		for k, v := range row {
			if redact[k] {
				cp[k] = "***"
				continue
			}
			if s, ok := v.(string); ok && len(s) > maxCell {
				cp[k] = s[:maxCell] + "..."
				continue
			}
			cp[k] = v
		}
		out = append(out, cp)
	}
	return out
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}
