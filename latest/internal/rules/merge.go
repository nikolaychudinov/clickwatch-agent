package rules

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"chagent/internal/duration"
)

// Meta — снимок слоя-победителя: штампуется в каждом результате исполнения.
type Meta struct {
	DatasetVersion  int    `json:"dataset_version"`
	DatasetHash     string `json:"dataset_hash"`
	ReleasedAt      string `json:"released_at"`
	Source          string `json:"source"` // embedded | saas | local-overlay
	Channel         string `json:"channel"`
	MinAgentVersion string `json:"min_agent_version"`
}

// Catalog — опубликованный (иммутабельный) набор после слияния слоёв.
type Catalog struct {
	Meta      Meta
	Rules     []*Rule
	SelfRules []*SelfRule
}

// Clone возвращает глубокую копию правила: слои не изменяют друг друга.
func (r *Rule) Clone() *Rule {
	cp := *r
	if len(r.Thresholds) > 0 {
		cp.Thresholds = append([]Threshold(nil), r.Thresholds...)
	}
	if len(r.Tables) > 0 {
		cp.Tables = append([]string(nil), r.Tables...)
	}
	if len(r.AppliesTo) > 0 {
		cp.AppliesTo = append([]string(nil), r.AppliesTo...)
	}
	if len(r.Labels) > 0 {
		cp.Labels = append([]string(nil), r.Labels...)
	}
	if len(r.RedactColumns) > 0 {
		cp.RedactColumns = append([]string(nil), r.RedactColumns...)
	}
	if r.UnitMap != nil {
		cp.UnitMap = make(map[string]string, len(r.UnitMap))
		for k, v := range r.UnitMap {
			cp.UnitMap[k] = v
		}
	}
	return &cp
}

// OverlayRule — запись локального override: перекрывает только
// thresholds / enabled / interval (описание формата правил (в README репозитория) разд. 1).
type OverlayRule struct {
	UID        string             `yaml:"uid"`
	Thresholds *[]Threshold       `yaml:"thresholds,omitempty"`
	Enabled    *bool              `yaml:"enabled,omitempty"`
	Interval   *duration.Duration `yaml:"interval,omitempty"`
}

// Overlay — корень локального файла rules.yaml (слой 3).
type Overlay struct {
	Rules []OverlayRule `yaml:"rules"`
}

// LoadOverlay читает слой 3; отсутствующий файл — не ошибка (nil, nil).
func LoadOverlay(path string) (*Overlay, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("local override: %w", err)
	}
	var ov Overlay
	if err := yaml.Unmarshal(raw, &ov); err != nil {
		return nil, fmt.Errorf("local override: %w", err)
	}
	return &ov, nil
}

// Diff — свод изменений каталога (для события rules_updated).
type Diff struct {
	Added              []string
	Removed            []string
	ThresholdsChanged  []string
	SelfChanged        bool
	VersionFrom        int
	VersionTo          int
}

// DiffAgainst сравнивает два каталога по uid и ревизии порогов.
func (c *Catalog) DiffAgainst(old *Catalog) Diff {
	var d Diff
	if c != nil {
		d.VersionTo = c.Meta.DatasetVersion
	}
	if old != nil {
		d.VersionFrom = old.Meta.DatasetVersion
	}
	newIdx := map[string]*Rule{}
	if c != nil {
		for _, r := range c.Rules {
			newIdx[r.UID] = r
		}
	}
	oldIdx := map[string]*Rule{}
	if old != nil {
		for _, r := range old.Rules {
			oldIdx[r.UID] = r
		}
	}
	for uid, r := range newIdx {
		if _, ok := oldIdx[uid]; !ok {
			d.Added = append(d.Added, uid)
			continue
		}
		if oldIdx[uid].ThresholdVersion != r.ThresholdVersion ||
			oldIdx[uid].ThresholdsUpdatedAt != r.ThresholdsUpdatedAt {
			d.ThresholdsChanged = append(d.ThresholdsChanged, uid)
		}
	}
	for uid := range oldIdx {
		if _, ok := newIdx[uid]; !ok {
			d.Removed = append(d.Removed, uid)
		}
	}
	if (c == nil) != (old == nil) || c != nil && old != nil && len(c.SelfRules) != len(old.SelfRules) {
		d.SelfChanged = true
	}
	return d
}

// BuildCatalog — слияние трёх слоёв:
//  1. embedded (база),
//  2. слой SaaS (новее, если дан),
//  3. локальный overlay (только thresholds/enabled/interval по uid).
//
// Возвращает каталог и меняет ли пороги локальный слой (информация для логов).
func BuildCatalog(emb, saas *Ruleset, ov *Overlay) (*Catalog, error) {
	if emb == nil {
		return nil, fmt.Errorf("merge: встроенный слой отсутствует")
	}
	winner, source := emb, "embedded"
	if saas != nil && saas.DatasetVersion >= emb.DatasetVersion {
		winner, source = saas, "saas"
	}

	// Копии правил слоя-победителя: оверлей меняет копии.
	rulesOut := make([]*Rule, 0, len(winner.Rules))
	byUID := make(map[string]*Rule, len(winner.Rules))
	for _, r := range winner.Rules {
		c := r.Clone()
		rulesOut = append(rulesOut, c)
		byUID[c.UID] = c
	}

	selfOut := make([]*SelfRule, 0, len(winner.SelfRules))
	for _, s := range winner.SelfRules {
		if s != nil {
			scp := *s
			if len(s.Thresholds) > 0 {
				scp.Thresholds = append([]Threshold(nil), s.Thresholds...)
			}
			selfOut = append(selfOut, &scp)
		}
	}

	if ov != nil {
		for _, or := range ov.Rules {
			r, ok := byUID[or.UID]
			if !ok {
				// локальный файл не создаёт правила — по uid наложение строго.
				continue
			}
			if or.Enabled != nil {
				e := *or.Enabled
				r.Enabled = &e
				r.ThresholdsSource = "local"
			}
			if or.Interval != nil || (or.Interval != nil && or.Interval.Duration > 0) {
				r.Interval = or.Interval
				r.ThresholdsSource = "local"
			}
			if or.Thresholds != nil && !r.ThresholdsLock {
				th := *or.Thresholds
				r.Thresholds = th
				r.ThresholdsSource = "local"
			}
		}
		// Пересчёт эффективных полей после наложения (у базы уже посчитаны).
		for _, r := range rulesOut {
			r.Resolve(winner.Defaults)
		}
	}

	return &Catalog{
		Meta: Meta{
			DatasetVersion:  winner.DatasetVersion,
			DatasetHash:     winner.DatasetHash,
			ReleasedAt:      winner.ReleasedAt,
			Source:          source,
			Channel:         winner.Channel,
			MinAgentVersion: winner.MinAgentVersion,
		},
		Rules:     rulesOut,
		SelfRules: selfOut,
	}, nil
}

// ForecastPerDay — прогноз числа исполнений в сутки для текущего каталога.
func (c *Catalog) ForecastPerDay() int {
	total := 0
	for _, r := range c.Rules {
		if r.EffInterval > 0 {
			total += int(time.Duration(24*time.Hour) / r.EffInterval)
		}
	}
	return total
}
