// Package selfloop: телеметрия самого агента (блок self_rules, 11 метрик).
// Работает при недоступном ClickHouse — в этом её смысл
// (формат правил в README репозитория, разд. 5; документация по использованию разд. 13).
package selfloop

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"chagent/internal/evaluate"
	"chagent/internal/rules"
	"chagent/internal/saasclient"
)

// Gauge — поставщик значения self-метрики (реестр закрыт функциями agent-подсистем).
type Gauge func() float64

// S — реестр гейджей и цикл отправки.
type S struct {
	log  *slog.Logger
	ing  *saasclient.Client
	mu   sync.Mutex
	self []*rules.SelfRule
	gauges map[string]Gauge
}

// New создаёт self-цикл.
func New(log *slog.Logger, ing *saasclient.Client) *S {
	return &S{log: log, ing: ing, gauges: map[string]Gauge{}}
}

// SetRules обновляет перечень self-метрик слоем-победителем.
func (s *S) SetRules(self []*rules.SelfRule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.self = self
}

// SetGauge привязывает uid к функции-снимателю.
func (s *S) SetGauge(uid string, g Gauge) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g == nil {
		delete(s.gauges, uid)
		return
	}
	s.gauges[uid] = g
}

// metricName — имя метрики в записи: если в правиле заданы пороги, берём
// имя их метрики; иначе хвост uid. (Пороги self_rules пишут итоговые имена.)
func metricName(r *rules.SelfRule) string {
	if len(r.Thresholds) > 0 && r.Thresholds[0].Metric != "" {
		return r.Thresholds[0].Metric
	}
	parts := strings.Split(r.UID, ".")
	return parts[len(parts)-1]
}

// Run запускает по тикеру на каждое self-правило.
func (s *S) Run(ctx context.Context) {
	s.mu.Lock()
	snapshot := append([]*rules.SelfRule(nil), s.self...)
	s.mu.Unlock()
	for _, r := range snapshot {
		go s.loopRule(ctx, r)
	}
}

func (s *S) loopRule(ctx context.Context, r *rules.SelfRule) {
	if r.Interval.Duration <= 0 {
		return
	}
	t := time.NewTicker(r.Interval.Duration)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.mu.Lock()
		g := s.gauges[r.UID]
		s.mu.Unlock()
		if g == nil {
			continue // поставщик ещё не привязан (например, до первой публикации плана)
		}
		value := g()
		name := metricName(r)
		row := map[string]any{name: value}
		st := evaluate.EvalStatus(r.Thresholds, row)
		rec := map[string]any{
			"uid":          r.UID,
			"status":       st,
			"evaluated_at": time.Now().UTC().Format(time.RFC3339),
			"interval":     r.Interval.Duration.String(),
			"metrics":      []map[string]any{{"name": name, "value": value}},
		}
		// события через персистентную очередь: недоступность приёма
		// и рестарт не приводят к потере self-метрик
		s.ing.Enqueue(s.ing.EventsPath(), rec)
	}
}
