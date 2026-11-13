// Package buffer: персистентная очередь отправки на диске (write-ahead).
// Запись сохраняется в файл ДО отправки, поэтому рестарт агента ничего
// не теряет. Каталог — storage.buffer_dir, лимит — storage.buffer_max_bytes
// (формат agent.yaml разд. 9).
//
// Соответствие модели очереди прототипа (agett_go.md):
//
//	Enqueue          → Append   (задача попадает на диск до отправки)
//	GetPending       → Batch    (готовые к отправке с учётом backoff)
//	UpdateStatus     → Defer    (счётчик попыток + срок следующей)
//	IncrementRetries → Attempts в Item
//	CleanupDone      → Commit   (успех или необратимый отказ)
//	RecoverPending   → New      (индексация каталога при старте)
//	Stats            → Pending
package buffer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Item — задача очереди: путь приёма, записи, счётчик попыток и срок
// следующей попытки. Backoff живёт в файле, поэтому переживает рестарт.
type Item struct {
	Path      string            `json:"path"`
	Records   []json.RawMessage `json:"records"`
	CreatedAt string            `json:"created_at"`
	Attempts  int               `json:"attempts"`
	NextAfter string            `json:"next_after,omitempty"` // RFC3339; пусто — готово к отправке
}

// B — очередь: файлы buf-00000001.json по возрастанию номера (FIFO).
type B struct {
	mu       sync.Mutex
	dir      string
	next     int
	bytes    int64
	maxBytes int64
	legacy   int
}

// New создаёт каталог очереди и индексирует накопленное (аналог
// RecoverPending прототипа: невыполненные задачи готовы к досылу).
// Записи устаревшего формата (env-снапшоты прежнего буфера) удаляются
// и учитываются в LegacyCount.
func New(dir string, maxBytes int64) (*B, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("buffer: каталог: %w", err)
	}
	b := &B{dir: dir, maxBytes: maxBytes}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("buffer: чтение: %w", err)
	}
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasPrefix(n, "buf-") || !strings.HasSuffix(n, ".json") {
			continue
		}
		full := filepath.Join(dir, n)
		raw, rerr := os.ReadFile(full)
		if rerr != nil {
			continue
		}
		var legacy struct {
			Env json.RawMessage `json:"env"`
		}
		var it Item
		if ue := json.Unmarshal(raw, &legacy); ue == nil && len(legacy.Env) > 0 {
			_ = os.Remove(full)
			b.legacy++
			continue
		}
		if uerr := json.Unmarshal(raw, &it); uerr != nil || it.Path == "" {
			_ = os.Remove(full)
			b.legacy++
			continue
		}
		var num int
		if _, perr := fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(n, "buf-"), ".json"), "%d", &num); perr == nil && num >= b.next {
			b.next = num + 1
		}
		if st, serr := os.Stat(full); serr == nil {
			b.bytes += st.Size()
		}
	}
	return b, nil
}

// LegacyCount — сколько записей прежнего формата удалено при старте.
func (b *B) LegacyCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.legacy
}

// Append (Enqueue прототипа) — постановка записей в конец очереди.
// Пишет на диск до отправки; при превышении лимита вытесняет старейшие.
func (b *B) Append(path string, records []map[string]any) error {
	if path == "" || len(records) == 0 {
		return nil
	}
	item := Item{Path: path, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	for _, r := range records {
		raw, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("buffer: сериализация записи: %w", err)
		}
		item.Records = append(item.Records, raw)
	}
	raw, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("buffer: сериализация задачи: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for b.maxBytes > 0 && b.bytes+int64(len(raw)) > b.maxBytes {
		oldest := b.oldestLocked()
		if oldest == "" {
			break // пусто и всё равно над лимитом — пишем под лимит не хуже
		}
		b.removeLocked(oldest)
	}
	name := fmt.Sprintf("buf-%08d.json", b.next)
	b.next++
	tmp := filepath.Join(b.dir, name+".tmp")
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("buffer: запись: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(b.dir, name)); err != nil {
		return fmt.Errorf("buffer: замена: %w", err)
	}
	b.bytes += int64(len(raw))
	return nil
}

// Batch (GetPending прототипа) — готовые к отправке задачи: FIFO,
// группа = один путь приёма (из группы собирается один конверт),
// задачи в backoff пропускаются. Битые файлы очереди уничтожаются.
func (b *B) Batch(maxRecords int, maxBytes int64, now time.Time) ([]Item, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	names := b.listLocked()
	var items []Item
	var chosen []string
	recCount := 0
	used := int64(0)
	for _, n := range names {
		full := filepath.Join(b.dir, n)
		raw, rerr := os.ReadFile(full)
		if rerr != nil {
			continue
		}
		var it Item
		if uerr := json.Unmarshal(raw, &it); uerr != nil || it.Path == "" || len(it.Records) == 0 {
			b.removeLocked(n) // битая задача — не блокирует очередь
			continue
		}
		if it.NextAfter != "" {
			if due, perr := time.Parse(time.RFC3339, it.NextAfter); perr == nil && due.After(now) {
				continue // ещё в backoff
			}
		}
		if len(items) > 0 && it.Path != items[0].Path {
			break // группа собирается по одному пути приёма
		}
		if maxRecords > 0 && recCount+len(it.Records) > maxRecords {
			break
		}
		if maxBytes > 0 && len(items) > 0 && used+int64(len(raw)) > maxBytes {
			break
		}
		items = append(items, it)
		chosen = append(chosen, n)
		recCount += len(it.Records)
		used += int64(len(raw))
	}
	return items, chosen
}

// Defer (UpdateStatus прототипа) — счётчик попыток и срок следующей
// попытки для отложенных после временного отказа задач.
func (b *B) Defer(names []string, nextAfter time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	stamp := nextAfter.UTC().Format(time.RFC3339)
	for _, n := range names {
		full := filepath.Join(b.dir, n)
		raw, err := os.ReadFile(full)
		if err != nil {
			continue
		}
		var it Item
		if uerr := json.Unmarshal(raw, &it); uerr != nil || it.Path == "" {
			continue
		}
		it.Attempts++
		it.NextAfter = stamp
		out, merr := json.Marshal(it)
		if merr != nil {
			continue
		}
		tmp := full + ".tmp"
		if werr := os.WriteFile(tmp, out, 0o600); werr != nil {
			continue
		}
		_ = os.Rename(tmp, full)
	}
	return nil
}

// Commit (CleanupDone прототипа) — задачи ушли из очереди: успех или
// необратимый отказ (HTTP 4xx).
func (b *B) Commit(names []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var failed []string
	for _, n := range names {
		full := filepath.Join(b.dir, n)
		st, serr := os.Stat(full)
		if serr != nil {
			continue // уже нет — норма
		}
		if err := os.Remove(full); err != nil {
			failed = append(failed, err.Error())
			continue
		}
		b.bytes -= st.Size()
		if b.bytes < 0 {
			b.bytes = 0
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("buffer: удаление: %s", strings.Join(failed, "; "))
	}
	return nil
}

// Pending (Stats прототипа) — число задач в очереди и объём на диске.
func (b *B) Pending() (n int, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n = len(b.listLocked())
	return n, b.bytes
}

func (b *B) listLocked() []string {
	entries, _ := os.ReadDir(b.dir)
	names := []string{}
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && strings.HasPrefix(n, "buf-") && strings.HasSuffix(n, ".json") {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names
}

func (b *B) oldestLocked() string {
	names := b.listLocked()
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func (b *B) removeLocked(name string) {
	full := filepath.Join(b.dir, name)
	if st, err := os.Stat(full); err == nil {
		b.bytes -= st.Size()
		if b.bytes < 0 {
			b.bytes = 0
		}
	}
	_ = os.Remove(full)
}
