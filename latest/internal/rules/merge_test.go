package rules_test

import (
	"testing"
	"time"

	"chagent/internal/chversion"
	"chagent/internal/duration"
	"chagent/internal/plan"
	"chagent/internal/rules"
	"chagent/internal/status"
)

func mkRule(uid string, since string, enabled bool) *rules.Rule {
	eb := enabled
	iv := duration.Duration{Duration: 5 * time.Minute}
	to := duration.Duration{Duration: 15 * time.Second}
	return &rules.Rule{
		UID:              uid,
		ID:               1,
		Group:            "components",
		Priority:         "P2",
		AppliesTo:        []string{"replicated"},
		Scope:            "node",
		Tables:           []string{"system.parts"},
		Since:            since,
		Interval:         &iv,
		ExecutionTimeout: &to,
		Query:            "SELECT count() AS c FROM system.parts",
		QuerySettings:    "max_threads=2",
		UnitMap:          map[string]string{"c": "count"},
		Thresholds:       []rules.Threshold{{Metric: "c", Op: "gte", Value: 1, Severity: "warning"}},
		Enabled:          &eb,
		ThresholdVersion: 1,
		ThresholdsSource: "builtin",
	}
}

func newCatalog(rs *rules.Ruleset) *rules.Catalog {
	meta := rules.Meta{DatasetVersion: rs.DatasetVersion, Source: "embedded"}
	return &rules.Catalog{Rules: rs.Rules, Meta: meta}
}

func TestBuildCatalogOverlayAndOrder(t *testing.T) {
	emb := &rules.Ruleset{
		SchemaVersion: 1, DatasetUID: "t", DatasetVersion: 0,
		Rules: []*rules.Rule{mkRule("ch.a.one", "22.3", true), mkRule("ch.a.two", "", true)},
	}
	// слой SaaS целиком подменяет набор: оба правила катятся из него
	saas := &rules.Ruleset{
		SchemaVersion: 1, DatasetUID: "t", DatasetVersion: 5,
		Rules: []*rules.Rule{mkRule("ch.a.one", "22.3", true), mkRule("ch.a.two", "", true)},
	}
	e := false
	ivl := duration.Duration{Duration: 1 * time.Hour}
	ov := &rules.Overlay{Rules: []rules.OverlayRule{
		{UID: "ch.a.one", Enabled: &e, Interval: &ivl},
		{UID: "ch.a.nothere", Enabled: &e}, // несуществующий uid игнорируется
	}}

	emb.ResolveAll()
	saas.ResolveAll()
	cat, err := rules.BuildCatalog(emb, saas, ov)
	if err != nil {
		t.Fatal(err)
	}
	if cat.Meta.DatasetVersion != 5 {
		t.Fatalf("версия слоя-победителя = %d", cat.Meta.DatasetVersion)
	}
	if len(cat.Rules) != 2 {
		t.Fatalf("после overlay правил %d", len(cat.Rules))
	}
	for _, r := range cat.Rules {
		switch r.UID {
		case "ch.a.one":
			if r.EffEnabled {
				t.Fatal("overlay enabled=false не применился")
			}
			if r.EffInterval != ivl.Duration {
				t.Fatal("overlay interval не применился")
			}
			if r.ThresholdsSource != "local" {
				t.Fatalf("thresholds_source = %q, ожидался local", r.ThresholdsSource)
			}
		case "ch.a.two":
			if !r.EffEnabled {
				t.Fatal("чужой overlay не должен менять другие правила")
			}
		}
	}
}

// слой SaaS без правила = правило удалено из плана (никакого resurrection из embedded).
func TestSaaSLayerSupersedes(t *testing.T) {
	emb := &rules.Ruleset{
		SchemaVersion: 1, DatasetUID: "t", DatasetVersion: 0,
		Rules: []*rules.Rule{mkRule("ch.a.one", "", true), mkRule("ch.a.two", "", true)},
	}
	saas := &rules.Ruleset{
		SchemaVersion: 1, DatasetUID: "t", DatasetVersion: 7,
		Rules: []*rules.Rule{mkRule("ch.a.one", "", true)},
	}
	emb.ResolveAll()
	saas.ResolveAll()
	cat, err := rules.BuildCatalog(emb, saas, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Rules) != 1 || cat.Rules[0].UID != "ch.a.one" {
		t.Fatalf("ожидалось остаётся 1 правило слоя SaaS: %d", len(cat.Rules))
	}
}

func TestMergeLock(t *testing.T) {
	r := mkRule("ch.b.one", "", true)
	r.ThresholdsLock = true
	r.Thresholds = []rules.Threshold{{Metric: "c", Op: "gte", Value: 5, Severity: "critical"}}
	emb := &rules.Ruleset{SchemaVersion: 1, DatasetUID: "t", Rules: []*rules.Rule{r}}
	emb.ResolveAll()

	blocked := []rules.Threshold{{Metric: "c", Op: "gte", Value: 3, Severity: "warning"}}
	ov := &rules.Overlay{Rules: []rules.OverlayRule{
		{UID: "ch.b.one", Thresholds: &blocked},
	}}
	cat, err := rules.BuildCatalog(emb, nil, ov)
	if err != nil {
		t.Fatal(err)
	}
	if cat.Rules[0].Thresholds[0].Value != 5 || cat.Rules[0].Thresholds[0].Severity != "critical" {
		t.Fatal("thresholds_lock: пороги не должны были перекрыться")
	}
}

func TestPlanVersionGates(t *testing.T) {
	emb := &rules.Ruleset{SchemaVersion: 1, DatasetUID: "t", Rules: []*rules.Rule{
		mkRule("ch.old.rule", "22.3", true), // 21.8 → skip
		mkRule("ch.new.rule", "25.0", true), // 21.8 → skip
		mkRule("ch.mid.rule", "", true),     // живое
	}}
	emb.ResolveAll()

	cat := newCatalog(emb)
	env := plan.Env{
		Version:    chversion.V{Major: 21, Minor: 8},
		MinVersion: chversion.V{Major: 21, Minor: 8},
		Mode:       "replicated",
	}
	p := plan.Build(cat, env)

	if len(p.Runnables) != 1 || p.Runnables[0].UID != "ch.mid.rule" {
		t.Fatalf("исполняемые: %d (%v)", len(p.Runnables), p.Runnables)
	}
	if p.Static["ch.old.rule"] != status.SkippedUnsupported ||
		p.Static["ch.new.rule"] != status.SkippedUnsupported {
		t.Fatalf("пропуски версий: %v", p.Static)
	}
}

func TestDiffAgainst(t *testing.T) {
	old := &rules.Ruleset{SchemaVersion: 1, DatasetUID: "t", Rules: []*rules.Rule{
		mkRule("ch.d.a", "", true),
		mkRule("ch.d.b", "", true),
	}}
	old.Rules[0].ThresholdVersion = 1
	newr := &rules.Ruleset{SchemaVersion: 1, DatasetUID: "t", Rules: []*rules.Rule{
		mkRule("ch.d.a", "", true), // ревизия порогов выросла
		mkRule("ch.d.c", "", true), // новый
	}}
	newr.Rules[0].ThresholdVersion = 2
	for _, rs := range []*rules.Ruleset{old, newr} {
		rs.ResolveAll()
	}
	diff := newCatalog(newr).DiffAgainst(newCatalog(old))
	if len(diff.Added) != 1 || diff.Added[0] != "ch.d.c" {
		t.Fatalf("added = %v", diff.Added)
	}
	if len(diff.Removed) != 1 || diff.Removed[0] != "ch.d.b" {
		t.Fatalf("removed = %v", diff.Removed)
	}
	if len(diff.ThresholdsChanged) != 1 || diff.ThresholdsChanged[0] != "ch.d.a" {
		t.Fatalf("thresholds_changed = %v", diff.ThresholdsChanged)
	}
}
