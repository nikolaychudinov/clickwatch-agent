// Package evaluate: оценка порогов правила по строке результата.
// Ключевая семантика ФТ-16.2: отсутствующая или NULL-метрика не даёт алерта —
// порог пропускается, а не трактуется как ноль.
package evaluate

import (
	"encoding/json"
	"strconv"
	"strings"

	"chagent/internal/rules"
	"chagent/internal/status"
)

// CoerceFloat приводит значение колонки JSONEachRow к числу.
// ClickHouse по умолчанию сериализует 64-битные целые строками — обрабатываем оба вида.
func CoerceFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case float64:
		return t, true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}

// EvalStatus вычисляет статус правила: максимум сработавшей severity.
// Порядок порогов в списке не влияет.
func EvalStatus(thresholds []rules.Threshold, row map[string]any) string {
	cur := status.None
	for _, th := range thresholds {
		raw, ok := row[th.Metric]
		if !ok {
			continue // метрики нет — порог не оценивается
		}
		v, ok := CoerceFloat(raw)
		if !ok {
			continue // NULL или нечисловое значение — то же самое
		}
		if Triggers(th, v) && status.SeverityRank(th.Severity) > status.SeverityRank(cur) {
			cur = th.Severity
		}
	}
	return cur
}

// Triggers — срабатывание одного порога по операции.
func Triggers(th rules.Threshold, v float64) bool {
	switch th.Op {
	case "gt":
		return v > th.Value
	case "gte":
		return v >= th.Value
	case "lt":
		return v < th.Value
	case "lte":
		return v <= th.Value
	case "eq":
		return v == th.Value
	case "neq":
		return v != th.Value
	}
	return false
}
