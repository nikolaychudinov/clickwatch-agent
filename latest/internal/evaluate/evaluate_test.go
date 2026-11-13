package evaluate

import (
	"testing"

	"chagent/internal/rules"
	"chagent/internal/status"
)

func mkThresholds() []rules.Threshold {
	return []rules.Threshold{
		{Metric: "cnt", Op: "gte", Value: 10, Severity: "warning"},
		{Metric: "cnt", Op: "gte", Value: 100, Severity: "critical"},
		{Metric: "ratio", Op: "gte", Value: 0.5, Severity: "warning"},
	}
}

func TestMaxSeverity(t *testing.T) {
	row := map[string]any{"cnt": "150"} // 64-битное число приходит строкой
	if got := EvalStatus(mkThresholds(), row); got != status.Critical {
		t.Fatalf("status = %q, ожидался critical", got)
	}
	row2 := map[string]any{"cnt": 12.0}
	if got := EvalStatus(mkThresholds(), row2); got != status.Warning {
		t.Fatalf("status = %q, ожидался warning", got)
	}
	row3 := map[string]any{"cnt": 1.0}
	if got := EvalStatus(mkThresholds(), row3); got != status.None {
		t.Fatalf("status = %q, ожидался none", got)
	}
}

func TestNullSkipsThreshold(t *testing.T) {
	row := map[string]any{"cnt": nil, "ratio": 0.9}
	if got := EvalStatus(mkThresholds(), row); got != status.Warning {
		t.Fatalf("NULL-метрика не должна рождать алерт; got %q", got)
	}
	row2 := map[string]any{"ratio": 0.1}
	if got := EvalStatus(mkThresholds(), row2); got != status.None {
		t.Fatalf("got %q, ожидался none", got)
	}
	empty := map[string]any{}
	if got := EvalStatus(mkThresholds(), empty); got != status.None {
		t.Fatalf("пустая строка результата: %q", got)
	}
}

func TestOps(t *testing.T) {
	cases := []struct {
		op    string
		val   float64
		ref   float64
		want  bool
	}{
		{"gt", 5, 5, false},
		{"gte", 5, 5, true},
		{"lt", 4, 5, true},
		{"lte", 5, 5, true},
		{"eq", 5, 5, true},
		{"neq", 4, 5, true},
	}
	for _, c := range cases {
		th := rules.Threshold{Metric: "m", Op: c.op, Value: c.ref, Severity: "warning"}
		if got := Triggers(th, c.val); got != c.want {
			t.Fatalf("op %s(%v, ref %v) = %v", c.op, c.val, c.ref, got)
		}
	}
}
