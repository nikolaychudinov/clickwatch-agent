// Package rules: типы каталога правил (grammar описание формата правил (в README репозитория)).
package rules

import (
	"time"

	"chagent/internal/duration"
)

// DatasetHashPlaceholder — значение-заглушка в исходнике embedded файла;
// агент обязан пересчитать канонический хеш и никогда не отправлять заглушку в SaaS.
const DatasetHashPlaceholder = "sha256:computed_at_build"

// Ruleset — весь файл правил (верхний уровень).
type Ruleset struct {
	SchemaVersion   int         `yaml:"schema_version" json:"schema_version"`
	DatasetUID      string      `yaml:"dataset_uid" json:"dataset_uid"`
	DatasetVersion  int         `yaml:"dataset_version" json:"dataset_version"`
	DatasetHash     string      `yaml:"dataset_hash" json:"dataset_hash"`
	ReleasedAt      string      `yaml:"released_at" json:"released_at"`
	Channel         string      `yaml:"channel" json:"channel"`
	MinAgentVersion string      `yaml:"min_agent_version" json:"min_agent_version"`
	Defaults        Defaults    `yaml:"defaults" json:"defaults"`
	Rules           []*Rule     `yaml:"rules" json:"rules"`
	SelfRules       []*SelfRule `yaml:"self_rules" json:"self_rules"`
}

// Defaults — значения по умолчанию для полей правил.
type Defaults struct {
	Interval         duration.Duration `yaml:"interval" json:"interval"`
	Jitter           duration.Duration `yaml:"jitter" json:"jitter"`
	ExecutionTimeout duration.Duration `yaml:"execution_timeout" json:"execution_timeout"`
	MaxRowsPerRule   int               `yaml:"max_rows_per_rule" json:"max_rows_per_rule"`
	MaxCellBytes     int               `yaml:"max_cell_bytes" json:"max_cell_bytes"`
	QuerySettings    string            `yaml:"query_settings" json:"query_settings"`
}

// Sampling — прореживание для огромных источников.
type Sampling struct {
	EveryNthRow int  `yaml:"every_nth_row" json:"every_nth_row"`
	Enabled     bool `json:"every_nth_row_enabled"` // заполняется при разборе
}

// Retry — повторы только сетевых/временных отказов, не ошибок SQL.
type Retry struct {
	MaxAttempts       int               `yaml:"max_attempts" json:"max_attempts"`
	BackoffInitial    duration.Duration `yaml:"backoff_initial" json:"backoff_initial"`
	BackoffMultiplier float64           `yaml:"backoff_multiplier" json:"backoff_multiplier"`
}

// Threshold — один порог правила.
type Threshold struct {
	Metric   string  `yaml:"metric" json:"metric"`
	Op       string  `yaml:"op" json:"op"`
	Value    float64 `yaml:"value" json:"value"`
	Severity string  `yaml:"severity" json:"severity"`
}

// Rule — правило мониторинга. Эффективные поля заполняются Resolve().
type Rule struct {
	ID                  int               `yaml:"id" json:"id"`
	UID                 string            `yaml:"uid" json:"uid"`
	TitleEN             string            `yaml:"title_en" json:"title_en"`
	TitleRU             string            `yaml:"title_ru" json:"title_ru"`
	Group               string            `yaml:"group" json:"group"`
	Priority            string            `yaml:"priority" json:"priority"`
	AppliesTo           []string          `yaml:"applies_to" json:"applies_to"`
	Scope               string            `yaml:"scope" json:"scope"`
	Since               string            `yaml:"since,omitempty" json:"since,omitempty"`
	Until               string            `yaml:"until,omitempty" json:"until,omitempty"`
	Tables              []string          `yaml:"tables" json:"tables"`
	Interval            *duration.Duration `yaml:"interval,omitempty" json:"interval,omitempty"`
	IngestPath          string            `yaml:"ingest_path,omitempty" json:"ingest_path,omitempty"`
	Query               string            `yaml:"query,omitempty" json:"query,omitempty"`
	QuerySettings       string            `yaml:"query_settings,omitempty" json:"query_settings,omitempty"`
	ExecutionTimeout    *duration.Duration `yaml:"execution_timeout,omitempty" json:"execution_timeout,omitempty"`
	Thresholds          []Threshold       `yaml:"thresholds" json:"thresholds"`
	ThresholdVersion    int               `yaml:"threshold_version" json:"threshold_version"`
	ThresholdsUpdatedAt string            `yaml:"thresholds_updated_at" json:"thresholds_updated_at"`
	ThresholdsSource    string            `yaml:"thresholds_source" json:"thresholds_source"`
	ThresholdsLock      bool              `yaml:"thresholds_lock,omitempty" json:"thresholds_lock,omitempty"`
	Enabled             *bool             `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	UnitMap             map[string]string `yaml:"unit_map,omitempty" json:"unit_map,omitempty"`
	Labels              []string          `yaml:"labels,omitempty" json:"labels,omitempty"`
	Owner               string            `yaml:"owner,omitempty" json:"owner,omitempty"`
	RunbookURL          string            `yaml:"runbook_url,omitempty" json:"runbook_url,omitempty"`
	Notes               string            `yaml:"notes,omitempty" json:"notes,omitempty"`
	RedactColumns       []string          `yaml:"redact_columns,omitempty" json:"redact_columns,omitempty"`
	Sampling            *Sampling         `yaml:"sampling,omitempty" json:"sampling,omitempty"`
	Retry               *Retry            `yaml:"retry,omitempty" json:"retry,omitempty"`

	// Эффективные значения — после Resolve(); в файле не хранятся.
	EffInterval time.Duration     `yaml:"-" json:"-"`
	EffTimeout  time.Duration     `yaml:"-" json:"-"`
	EffEnabled  bool              `yaml:"-" json:"-"`
	EffRetry    Retry             `yaml:"-" json:"-"`
}

// SelfRule — self-метрика агента (блок self_rules, описание формата правил (в README репозитория) разд. 5).
type SelfRule struct {
	UID        string            `yaml:"uid" json:"uid"`
	Title      string            `yaml:"title" json:"title"`
	Source     string            `yaml:"source" json:"source"`
	Interval   duration.Duration `yaml:"interval" json:"interval"`
	Thresholds []Threshold       `yaml:"thresholds" json:"thresholds"`
	Owner      string            `yaml:"owner,omitempty" json:"owner,omitempty"`
}

// Resolve вычисляет эффективные поля из собственных полей и defaults.
func (r *Rule) Resolve(def Defaults) {
	if r.Interval != nil && r.Interval.Duration > 0 {
		r.EffInterval = r.Interval.Duration
	} else {
		r.EffInterval = def.Interval.Duration
	}
	if r.ExecutionTimeout != nil && r.ExecutionTimeout.Duration > 0 {
		r.EffTimeout = r.ExecutionTimeout.Duration
	} else {
		r.EffTimeout = def.ExecutionTimeout.Duration
	}
	r.EffEnabled = r.Enabled == nil || *r.Enabled
	r.EffRetry = builtinRetry
	if r.Retry != nil {
		r.EffRetry = *r.Retry
		if r.EffRetry.MaxAttempts <= 0 {
			r.EffRetry.MaxAttempts = builtinRetry.MaxAttempts
		}
		if r.EffRetry.BackoffInitial.Duration <= 0 {
			r.EffRetry.BackoffInitial = builtinRetry.BackoffInitial
		}
		if r.EffRetry.BackoffMultiplier <= 1 {
			r.EffRetry.BackoffMultiplier = builtinRetry.BackoffMultiplier
		}
	}
}

// builtinRetry — дефолт при отсутствии блока retry (документация по использованию разд. 4, п. 10).
var builtinRetry = Retry{
	MaxAttempts:       2,
	BackoffInitial:    duration.Duration{Duration: 5 * 1_000_000_000},
	BackoffMultiplier: 2.0,
}

// ResolveAll — для всех правил набора.
func (rs *Ruleset) ResolveAll() {
	for _, r := range rs.Rules {
		if r != nil {
			r.Resolve(rs.Defaults)
		}
	}
}
