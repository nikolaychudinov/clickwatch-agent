// Package scheduler: тикеры по группам интервалов, джиттер, пул исполнителей,
// дневной бюджет исполнений (документация по использованию разд. 7). Модель: один горутин-планировщик
// и ограниченный пул workers, не горутина на правило.
package scheduler

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"chagent/internal/config"
	"chagent/internal/rules"
	"chagent/internal/runner"
)

// Task — единица исполнения, поставленная в очередь пула.
type Task struct {
	Rule *rules.Rule
	Meta rules.Meta
}

type updateMsg struct {
	cat   *rules.Catalog
	rules []*rules.Rule
}

// S — планировщик на весь процесс; каталог меняется через Update.
type S struct {
	log     *slog.Logger
	deps    runner.Deps
	limits  config.Limits
	down    func() bool
	paused  func() bool
	results chan runner.Out

	upd   chan updateMsg
	tasks chan Task

	mu      sync.Mutex
	runnable []*rules.Rule
	byUID   map[string]*rules.Rule
	nextAt  map[string]time.Time
	meta    rules.Meta
	jitter  time.Duration
	budget  int
	day     string
	used    int
	budgetHit bool
}

// New создаёт планировщик; down/paused — предикаты состояния (CH недоступен / спящий режим).
func New(log *slog.Logger, deps runner.Deps, limits config.Limits, down, paused func() bool,
	results chan runner.Out) *S {
	return &S{
		log:     log,
		deps:    deps,
		limits:  limits,
		down:    down,
		paused:  paused,
		results: results,
		upd:     make(chan updateMsg, 1),
		tasks:   make(chan Task, 512),
		jitter:  30 * time.Second,
		budget:  limits.DailyQueryBudget,
	}
}

// Update публикует новый каталог и список исполняемых правил (неплановые отфильтрованы).
func (s *S) Update(cat *rules.Catalog, runnable []*rules.Rule) {
	msg := updateMsg{cat: cat, rules: runnable}
	select {
	case s.upd <- msg:
	default:
		// предыдущее обновление ещё в очереди и перекроет
		select {
		case <-s.upd:
		default:
		}
		s.upd <- msg
	}
}

func (s *S) applyCatalogLocked(u updateMsg) {
	old := s.nextAt
	s.runnable = u.rules
	s.byUID = make(map[string]*rules.Rule, len(u.rules))
	s.nextAt = make(map[string]time.Time, len(u.rules))
	now := time.Now()
	j := s.jitter
	if j <= 0 {
		j = time.Second
	}
	for _, r := range u.rules {
		if r == nil || r.EffInterval <= 0 {
			continue
		}
		s.byUID[r.UID] = r
		if ph, ok := old[r.UID]; ok {
			s.nextAt[r.UID] = ph // фаза правила сохраняется сквозь перезагрузку
		} else {
			s.nextAt[r.UID] = now.Add(time.Duration(rand.Int64N(int64(j))))
		}
	}
	if u.cat != nil {
		s.meta = u.cat.Meta
	}
}

// Run — главный цикл; запускает workers и работает до отмены контекста.
func (s *S) Run(ctx context.Context) {
	n := s.limits.MaxConcurrentQueries
	if n <= 0 {
		n = 2
	}
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.worker(ctx)
		}()
	}
	defer func() {
		wg.Wait()
		close(s.results)
	}()

	for {
		s.mu.Lock()
		var next time.Time
		for _, at := range s.nextAt {
			if next.IsZero() || at.Before(next) {
				next = at
			}
		}
		s.mu.Unlock()

		if next.IsZero() {
			// план пуст — ждём обновление каталога
			select {
			case <-ctx.Done():
				return
			case u := <-s.upd:
				s.mu.Lock()
				s.applyCatalogLocked(u)
				s.mu.Unlock()
			}
			continue
		}

		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case u := <-s.upd:
			t.Stop()
			s.mu.Lock()
			s.applyCatalogLocked(u)
			s.mu.Unlock()
		case <-t.C:
			s.fire()
		}
	}
}

// fire — очередной тик: продвигает фазы и ставит наступившие задачи в очередь.
func (s *S) fire() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	today := now.UTC().Format("2006-01-02")
	if s.day != today {
		s.day = today
		s.used = 0
		s.budgetHit = false
	}

	for uid, at := range s.nextAt {
		if at.After(now) {
			continue
		}
		r := s.byUID[uid]
		if r == nil {
			delete(s.nextAt, uid)
			continue
		}
		iv := r.EffInterval
		// при долгом простое (CH-down и т.п.) долг по тикам не копится:
		// переносим фазу вперёд на один интервал от текущего момента.
		if now.Sub(at) > 2*iv {
			s.nextAt[uid] = now.Add(iv)
		} else {
			s.nextAt[uid] = at.Add(iv)
		}

		if s.paused != nil && s.paused() {
			continue
		}
		if s.down != nil && s.down() {
			continue
		}
		if s.used >= s.budget {
			if !s.budgetHit {
				s.budgetHit = true
				s.log.Warn("scheduler: дневной бюджет исполнений исчерпан, правила пропускаются до следующего UTC-дня",
					"budget", s.budget)
			}
			continue
		}
		s.used++
		select {
		case s.tasks <- Task{Rule: r, Meta: s.meta}:
		default:
			s.log.Warn("scheduler: очередь задач заполнена, исполнение пропущено", "uid", uid)
		}
	}
}

func (s *S) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-s.tasks:
			wctx, cancel := context.WithTimeout(ctx, t.Rule.EffTimeout)
			out := runner.Run(wctx, s.deps, t.Rule, t.Meta)
			cancel()
			select {
			case s.results <- out:
			case <-ctx.Done():
				return
			}
		}
	}
}

// BudgetHitToday — selfloop/диагностика: был ли сегодня перерасход бюджета.
func (s *S) BudgetHitToday() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.budgetHit
}

// UsedToday — исполнений израсходовано сегодня.
func (s *S) UsedToday() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}
