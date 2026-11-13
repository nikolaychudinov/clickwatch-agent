package buffer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newBuf(t *testing.T, maxBytes int64) (*B, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := New(dir, maxBytes)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b, dir
}

func TestAppendBatchCommit(t *testing.T) {
	b, _ := newBuf(t, 1<<20)
	path := "/v2/ingest/c1/ch.parts.too_many_parts"
	if err := b.Append(path, []map[string]any{{"uid": "ch.parts.too_many_parts", "status": "warning"}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if n, _ := b.Pending(); n != 1 {
		t.Fatalf("pending = %d, ожидается 1", n)
	}
	items, names := b.Batch(50, 1<<20, time.Now())
	if len(items) != 1 || len(names) != 1 {
		t.Fatalf("batch = %d задач (names %d), ожидается 1", len(items), len(names))
	}
	if items[0].Path != path {
		t.Fatalf("path = %q", items[0].Path)
	}
	var rec map[string]any
	if err := json.Unmarshal(items[0].Records[0], &rec); err != nil {
		t.Fatalf("разбор записи: %v", err)
	}
	if rec["uid"] != "ch.parts.too_many_parts" {
		t.Fatalf("запись потеряла содержимое: %v", rec)
	}
	if err := b.Commit(names); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if n, _ := b.Pending(); n != 0 {
		t.Fatalf("после Commit pending = %d", n)
	}
}

// Задача, отложенная после временного отказа, не выбирается до истечения
// backoff; счётчик попыток сохраняется в файле.
func TestDeferAndBackoff(t *testing.T) {
	b, _ := newBuf(t, 1<<20)
	path := "/v2/ingest/c1/ch.state.lost_parts"
	_ = b.Append(path, []map[string]any{{"uid": "ch.state.lost_parts"}})

	items, names := b.Batch(50, 1<<20, time.Now())
	if len(items) != 1 {
		t.Fatal("задача не выбрана")
	}
	_ = b.Defer(names, time.Now().Add(time.Hour))

	if n, _ := b.Pending(); n != 1 {
		t.Fatal("отложенная задача должна оставаться в очереди")
	}
	if got, _ := b.Batch(50, 1<<20, time.Now()); len(got) != 0 {
		t.Fatal("backoff не отработал: задача выбрана раньше срока")
	}
	items, _ = b.Batch(50, 1<<20, time.Now().Add(2*time.Hour))
	if len(items) != 1 || items[0].Attempts != 1 {
		t.Fatalf("после истечения backoff: задач %d, attempts %d", len(items), items[0].Attempts)
	}
}

// Группа отправки собирается по одному пути приёма (FIFO с остановкой на
// первом чужом пути).
func TestBatchGroupsByPath(t *testing.T) {
	b, _ := newBuf(t, 1<<20)
	_ = b.Append("/p/A", []map[string]any{{"n": 1}})
	_ = b.Append("/p/A", []map[string]any{{"n": 2}})
	_ = b.Append("/p/B", []map[string]any{{"n": 3}})
	items, names := b.Batch(50, 1<<20, time.Now())
	if len(items) != 2 {
		t.Fatalf("в группе %d задач, ожидается 2 по пути /p/A", len(items))
	}
	for _, it := range items {
		if it.Path != "/p/A" {
			t.Fatalf("чужой путь в группе: %q", it.Path)
		}
	}
	if len(names) != 2 {
		t.Fatalf("names = %d", len(names))
	}
}

// Лимит байт: старейшие задачи вытесняются первыми.
func TestEvictionUnderLimit(t *testing.T) {
	b, _ := newBuf(t, 300) // ~1-2 задачи
	for i := 0; i < 4; i++ {
		if err := b.Append("/v2/agent/events", []map[string]any{{"i": i, "payload": "xxxx-yyyy"}}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	n, bytes := b.Pending()
	if n == 4 {
		t.Fatal("лимит байт не сработал")
	}
	if bytes <= 0 || n <= 0 {
		t.Fatalf("очередь не должна опустеть целиком при живом append: n=%d bytes=%d", n, bytes)
	}
}

// Записи прежнего формата (env-снапшоты) удаляются при старте и считаются.
func TestLegacyFilesRemoved(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "buf-00000001.json"),
		[]byte(`{"path":"/v2/ingest/x","env":{"agent":{}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	b, err := New(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if b.LegacyCount() != 1 {
		t.Fatalf("legacy = %d, ожидается 1", b.LegacyCount())
	}
	if n, _ := b.Pending(); n != 0 {
		t.Fatalf("устаревшая запись не должна входить в очередь: %d", n)
	}
}

// RecoverPending: очередь создаётся заново тем же каталогом — задачи живы.
func TestRecoverPending(t *testing.T) {
	dir := t.TempDir()
	b1, err := New(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_ = b1.Append("/v2/agent/events", []map[string]any{{"event": "ch_unreachable"}})
	_ = b1.Append("/v2/ingest/c1/ch.q", []map[string]any{{"uid": "ch.q"}})

	b2, err := New(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := b2.Pending(); n != 2 {
		t.Fatalf("после рестарта в очереди %d задач, ожидается 2", n)
	}
	items, _ := b2.Batch(50, 1<<20, time.Now())
	if len(items) != 1 || items[0].Path != "/v2/agent/events" {
		t.Fatalf("после рестарта группа = %v", items)
	}
}

// Битые файлы очереди не блокируют отправку.
func TestCorruptedEntriesSkipped(t *testing.T) {
	b, dir := newBuf(t, 1<<20)
	if err := os.WriteFile(filepath.Join(dir, "buf-00000001.json"), []byte("{broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = b.Append("/p/X", []map[string]any{{"ok": true}})
	items, _ := b.Batch(50, 1<<20, time.Now())
	if len(items) != 1 || items[0].Path != "/p/X" {
		t.Fatalf("битый файл не пропущен: %v", items)
	}
}
