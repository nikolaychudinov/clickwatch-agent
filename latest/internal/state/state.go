// Package state: state.json — agent_uuid, client_id, agent_seq, применённый
// набор правил и последние статусы правил (для edge-detection, документация по использованию разд. 14).
package state

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Ruleset — применённый набор правил.
type Ruleset struct {
	DatasetVersion int    `json:"dataset_version"`
	DatasetHash    string `json:"dataset_hash"`
	ReleasedAt     string `json:"released_at"`
	Source         string `json:"source"`
}

// Data — содержимое state.json.
type Data struct {
	AgentUUID    string            `json:"agent_uuid"`
	ClientID     string            `json:"client_id"`
	AgentSeq     uint64            `json:"agent_seq"`
	Ruleset      Ruleset           `json:"ruleset"`
	LastStatuses map[string]string `json:"last_statuses"`
}

// Store — потокобезопасный владелец файла состояния.
type Store struct {
	mu   sync.Mutex
	path string
	d    Data
}

// Load читает state.json; отсутствующий файл создаёт новый данность UUID.
func Load(path string) (*Store, error) {
	s := &Store{path: path, d: Data{LastStatuses: map[string]string{}}}
	raw, err := os.ReadFile(path)
	if err == nil {
		if jerr := json.Unmarshal(raw, &s.d); jerr != nil {
			s.d = Data{AgentUUID: NewUUID(), LastStatuses: map[string]string{}}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("state: %w", err)
	}
	if s.d.AgentUUID == "" {
		s.d.AgentUUID = NewUUID()
		if err := s.Save(); err != nil {
			return nil, err
		}
	}
	if s.d.LastStatuses == nil {
		s.d.LastStatuses = map[string]string{}
	}
	return s, nil
}

// Get копирует текущее состояние.
func (s *Store) Get() Data {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.d
	cp.LastStatuses = make(map[string]string, len(s.d.LastStatuses))
	for k, v := range s.d.LastStatuses {
		cp.LastStatuses[k] = v
	}
	return cp
}

// Mutate применяет функцию к состоянию и сохраняет файл.
func (s *Store) Mutate(fn func(*Data)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.d)
	_ = s.save()
}

// Save принудительная запись файла (без блокировки — вызывает Mutate).
func (s *Store) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.save()
}

func (s *Store) save() error {
	if s.path == "" {
		return nil
	}
	if dir := filepath.Dir(s.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("state: каталог: %w", err)
		}
	}
	raw, err := json.MarshalIndent(s.d, "", "  ")
	if err != nil {
		return fmt.Errorf("state: сериализация: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("state: запись: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("state: замена: %w", err)
	}
	return nil
}

// NextSeq атомарно увеличивает и возвращает номер пакета (дежурное требование протокола).
func (s *Store) NextSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.d.AgentSeq++
	_ = s.save()
	return s.d.AgentSeq
}

// NewUUID — UUIDv4 из crypto/rand (без внешних зависимостей).
func NewUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
