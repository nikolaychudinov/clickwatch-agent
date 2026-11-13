package rules

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// Parse разбирает содержимое слоя правил в структуру.
// Неизвестные поля игнорируются (совместимость с будущими версиями грамматики).
func Parse(data []byte) (*Ruleset, error) {
	var rs Ruleset
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&rs); err != nil {
		return nil, fmt.Errorf("rules: разбор: %w", err)
	}
	if rs.SchemaVersion == 0 && len(rs.Rules) == 0 && len(rs.SelfRules) == 0 {
		return nil, fmt.Errorf("rules: файл пуст или не является набором правил")
	}
	rs.ResolveAll()
	return &rs, nil
}

// CanonicalHash — канонический sha256 от содержательных блоков набора
// (детерминированная сериализация полей в порядке объявления структур).
// Пайплайн сборки обязан вычислять хеш тем же способом.
func CanonicalHash(rs *Ruleset) string {
	payload := struct {
		SchemaVersion   int         `json:"schema_version"`
		DatasetUID      string      `json:"dataset_uid"`
		DatasetVersion  int         `json:"dataset_version"`
		ReleasedAt      string      `json:"released_at"`
		Channel         string      `json:"channel"`
		MinAgentVersion string      `json:"min_agent_version"`
		Defaults        Defaults    `json:"defaults"`
		Rules           []*Rule     `json:"rules"`
		SelfRules       []*SelfRule `json:"self_rules"`
	}{
		SchemaVersion:   rs.SchemaVersion,
		DatasetUID:      rs.DatasetUID,
		DatasetVersion:  rs.DatasetVersion,
		ReleasedAt:      rs.ReleasedAt,
		Channel:         rs.Channel,
		MinAgentVersion: rs.MinAgentVersion,
		Defaults:        rs.Defaults,
		Rules:           rs.Rules,
		SelfRules:       rs.SelfRules,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// RecoverHash пересчитывает хеш в заглушке или пустом поле (встроенный слой).
func (rs *Ruleset) RecoverHash() {
	if rs.DatasetHash == "" || rs.DatasetHash == DatasetHashPlaceholder {
		rs.DatasetHash = CanonicalHash(rs)
	}
}

// VerifyHash сверяет заявленный хеш слоя с каноническим.
// Для embedded допустима заглушка — тогда хеш пересчитывается на месте.
func (rs *Ruleset) VerifyHash(allowPlaceholder bool) error {
	if allowPlaceholder && (rs.DatasetHash == "" || rs.DatasetHash == DatasetHashPlaceholder) {
		rs.RecoverHash()
		return nil
	}
	got := CanonicalHash(rs)
	if got != rs.DatasetHash {
		return fmt.Errorf("rules: хеш слоя %q не совпал с каноническим %q", rs.DatasetHash, got)
	}
	return nil
}
