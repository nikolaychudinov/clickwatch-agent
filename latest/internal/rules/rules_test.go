package rules

import (
	"strings"
	"testing"
)

func TestEmbeddedParseAndValidate(t *testing.T) {
	rs, err := Parse(EmbeddedRaw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(rs.Rules) != 48 {
		t.Fatalf("rules = %d, ожидается 48", len(rs.Rules))
	}
	if len(rs.SelfRules) != 11 {
		t.Fatalf("self_rules = %d, ожидается 11", len(rs.SelfRules))
	}
	if rs.DatasetVersion != 0 {
		t.Fatalf("dataset_version = %d, ожидается 0 (embedded слой)", rs.DatasetVersion)
	}
	// hash: как в production (main), заглушка заменяется каноническим значением
	rs.RecoverHash()
	if rs.DatasetHash == "" || strings.Contains(rs.DatasetHash, "computed_at_build") {
		t.Fatalf("dataset_hash не пересчитан: %q", rs.DatasetHash)
	}
	if !strings.HasPrefix(rs.DatasetHash, "sha256:") {
		t.Fatalf("формат dataset_hash: %q", rs.DatasetHash)
	}

	if errs := Validate(rs, true); len(errs) > 0 {
		t.Fatalf("Validate(strict): %d ошибок: %v", len(errs), errs[:min(len(errs), 5)])
	}

	// последовательность id 1..48
	seen := map[int]bool{}
	for _, r := range rs.Rules {
		seen[r.ID] = true
	}
	for id := 1; id <= 48; id++ {
		if !seen[id] {
			t.Fatalf("нет правила с id %d", id)
		}
	}

	// эффективные поля
	for _, r := range rs.Rules {
		if r.EffInterval <= 0 {
			t.Fatalf("правило %s: нулевой интервал", r.UID)
		}
		if r.EffTimeout <= 0 || r.EffTimeout >= r.EffInterval {
			t.Fatalf("правило %s: таймаут %s вне (0, интервал %s)", r.UID, r.EffTimeout, r.EffInterval)
		}
	}
	// kafka_consumers (id 40) объявлено выключенным до проверки dba
	found := false
	for _, r := range rs.Rules {
		if r.UID == "ch.components.kafka_consumers" {
			found = true
			if r.EffEnabled {
				t.Fatal("kafka_consumers должен быть disabled по умолчанию")
			}
		}
	}
	if !found {
		t.Fatal("нет правила ch.components.kafka_consumers")
	}
}

func TestValidateCatches(t *testing.T) {
	rs, err := Parse(EmbeddedRaw)
	if err != nil {
		t.Fatal(err)
	}
	// дубликат uid
	rs.Rules[1].UID = rs.Rules[0].UID
	errs := Validate(rs, false)
	if len(errs) == 0 {
		t.Fatal("валидация не поймала дубликат uid")
	}
}

func TestVersionGateHelper(t *testing.T) {
	// правила с since 22.3 и 25.0 присутствуют в встроенном наборе
	rs, err := Parse(EmbeddedRaw)
	if err != nil {
		t.Fatal(err)
	}
	var has223, has250 bool
	for _, r := range rs.Rules {
		if r.Since == "22.3" {
			has223 = true
		}
		if r.Since == "25.0" {
			has250 = true
		}
	}
	if !has223 || !has250 {
		t.Fatalf("ожидались гейты 22.3 и 25.0: %v / %v", has223, has250)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
