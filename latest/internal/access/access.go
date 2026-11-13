// Package access: кэш доступности системных таблиц (ok | missing | denied).
// Исполнитель статусов skipped_table_missing / skipped_no_access и
// self-метрики ch.agent.access.denied_tables_count (документация по использованию разд. 6).
package access

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"chagent/internal/chclient"
)

const (
	StateOK      = "ok"
	StateMissing = "missing"
	StateDenied  = "denied"
	StateUnknown = "unknown"
)

// Cache — потокобезопасная таблица состояний по полным именам ("system.replicas").
type Cache struct {
	mu         sync.Mutex
	m          map[string]string
	lastChange time.Time
}

// New создаёт пустой кэш.
func New() *Cache { return &Cache{m: map[string]string{}} }

func normalize(name string) string {
	name = strings.TrimSpace(name)
	if !strings.Contains(name, ".") {
		return "system." + name
	}
	return name
}

// EnsureKnown одним дешёвым запросом фиксирует наличие таблиц в system.tables.
// Отсутствие grants здесь не детектится — это делается по ошибкам исполнения.
func (c *Cache) EnsureKnown(ctx context.Context, cl *chclient.Client, tables []string) error {
	if cl == nil || len(tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(tables))
	seen := map[string]bool{}
	for _, t := range tables {
		n := normalize(t)
		if seen[n] {
			continue
		}
		seen[n] = true
		names = append(names, "'"+n+"'")
	}
	if len(names) == 0 {
		return nil
	}
	q := fmt.Sprintf(
		"SELECT name FROM system.tables WHERE database = 'system' AND name IN (%s)",
		strings.Join(names, ", "))
	rows, err := cl.Query(ctx, q)
	if err != nil {
		return err
	}
	found := map[string]bool{}
	for _, row := range rows {
		found[asString(row["name"])] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range seenNames(seen) {
		if found[t] {
			c.markLocked(t, StateOK)
		} else {
			c.markLocked(t, StateMissing)
		}
	}
	return nil
}

func seenNames(seen map[string]bool) []string {
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}

// MarkOnErr перекладывает класс ошибки исполнения в состояние таблицы.
func (c *Cache) MarkOnErr(name string, cls chclient.ErrClass) {
	n := normalize(name)
	switch cls {
	case chclient.ErrTableMissing:
		c.setState(n, StateMissing)
	case chclient.ErrNoAccess:
		c.setState(n, StateDenied)
	}
}

func (c *Cache) setState(name, st string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markLocked(name, st)
}

func (c *Cache) markLocked(name, st string) {
	if c.m[name] == st {
		return
	}
	c.m[name] = st
	c.lastChange = time.Now()
}

// State возвращает состояние таблицы (unknown, если не выяснено).
func (c *Cache) State(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.m[normalize(name)]; ok {
		return st
	}
	return StateUnknown
}

// RefreshDenied перепроверяет все не-ok таблицы микрозапросами
// (SELECT 1 ... LIMIT 1: микросекунды; план — behavior.access_recheck_interval).
func (c *Cache) RefreshDenied(ctx context.Context, cl *chclient.Client) int {
	if cl == nil {
		return 0
	}
	c.mu.Lock()
	pending := make([]string, 0, len(c.m))
	for n, st := range c.m {
		if st != StateOK {
			pending = append(pending, n)
		}
	}
	c.mu.Unlock()

	fixed := 0
	for _, name := range pending {
		q := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", name)
		_, err := cl.Query(ctx, q)
		switch {
		case err == nil:
			c.setState(name, StateOK)
			fixed++
		default:
			if ce, ok := chclient.AsError(err); ok {
				c.MarkOnErr(name, ce.Class)
			}
		}
	}
	return fixed
}

// DeniedCount — число таблиц в состоянии denied (для self-метрики).
func (c *Cache) DeniedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, st := range c.m {
		if st == StateDenied {
			n++
		}
	}
	return n
}

// DeniedTables — перечень закрытых таблиц.
func (c *Cache) DeniedTables() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []string{}
	for n, st := range c.m {
		if st == StateDenied {
			out = append(out, n)
		}
	}
	return out
}

// LastChangeHours — часы с последнего изменения доступов.
func (c *Cache) LastChangeHours() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastChange.IsZero() {
		return 0
	}
	return time.Since(c.lastChange).Hours()
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
