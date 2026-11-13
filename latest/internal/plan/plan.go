// Package plan: фильтр применимости правил к конкретной ноде (документация по использованию разд. 6).
package plan

import (
	"time"

	"chagent/internal/chversion"
	"chagent/internal/rules"
	"chagent/internal/status"
)

// Env — параметры окружения ноды, по которым фильтруется каталог.
type Env struct {
	Version    chversion.V // фактическая или заявленная версия ClickHouse
	MinVersion chversion.V // limits.min_ch_version
	Mode       string      // standalone | replicated |shared (самодетект/конфиг)
}

// Plan — итог планирования: исполняемые правила и статусы пропусков.
type Plan struct {
	Runnables      []*rules.Rule
	Static         map[string]string // uid → статус (disabled / skipped_unsupported)
	Mode           string
	Version        chversion.V
	ForecastPerDay int // сумма исполнений за 24 ч по интервалам
}

// Build строит план по каталогу.
func Build(cat *rules.Catalog, env Env) *Plan {
	p := &Plan{
		Static:  map[string]string{},
		Mode:    env.Mode,
		Version: env.Version,
	}
	if cat == nil {
		return p
	}
	belowMin := chversion.Cmp(env.Version, env.MinVersion) < 0
	for _, r := range cat.Rules {
		if r == nil {
			continue
		}
		if !r.EffEnabled {
			p.Static[r.UID] = status.Disabled
			continue
		}
		if belowMin {
			p.Static[r.UID] = status.SkippedUnsupported
			continue
		}
		if !modeAllowed(r.AppliesTo, env.Mode) {
			continue // правило вне плана целиком: в результатах не мигает
		}
		if r.Since != "" {
			if sv, err := chversion.Parse(r.Since); err == nil && !chversion.Gte(env.Version, sv) {
				p.Static[r.UID] = status.SkippedUnsupported
				continue
			}
		}
		if r.Until != "" {
			if uv, err := chversion.Parse(r.Until); err == nil && !chversion.Lte(env.Version, uv) {
				p.Static[r.UID] = status.SkippedUnsupported
				continue
			}
		}
		p.Runnables = append(p.Runnables, r)
		if r.EffInterval > 0 {
			p.ForecastPerDay += int(time.Duration(24*time.Hour) / r.EffInterval)
		}
	}
	return p
}

func modeAllowed(applies []string, mode string) bool {
	for _, a := range applies {
		if a == mode {
			return true
		}
	}
	return false
}
