package rules

import (
	"os"
	"testing"
)

// TestSaasExampleParses — слой SaaS (образец) должен парситься и проходить
// нестрогую валидацию (strict только для встроенного набора).
func TestSaasExampleParses(t *testing.T) {
	raw, err := os.ReadFile("../../../rules_saas.example.yaml")
	if err != nil {
		t.Skipf("файл недоступен из теста: %v", err)
	}
	rs, perr := Parse(raw)
	if perr != nil {
		t.Fatalf("Parse: %v", perr)
	}
	if len(rs.Rules) != 1 {
		t.Fatalf("rules = %d, ожидается 1", len(rs.Rules))
	}
	r := rs.Rules[0]
	if r.UID != "ch.queries.raw_log" || r.ID != 49 {
		t.Fatalf("не то правило: id=%d uid=%q", r.ID, r.UID)
	}
	if r.EffEnabled {
		t.Fatalf("пример слоя должен быть enabled: false по умолчанию")
	}
	if errs := Validate(rs, false); len(errs) > 0 {
		t.Fatalf("Validate: %d ошибок: %v", len(errs), errs[:min(len(errs), 5)])
	}
}
