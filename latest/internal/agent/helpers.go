package agent

import (
	"fmt"

	"chagent/internal/rules"
)

// parseAndValidateLayer — приём скачанного слоя SaaS (документация по использованию разд. 12):
// parse → валидация (нежёсткий режим) → сверка канонического хеша.
// Валиден целиком или отброшен целиком — частичная валидация не применяется.
func parseAndValidateLayer(body []byte) (*rules.Ruleset, error) {
	layer, err := rules.Parse(body)
	if err != nil {
		return nil, err
	}
	if errs := rules.Validate(layer, false); len(errs) > 0 {
		limit := errs
		if len(limit) > 5 {
			limit = limit[:5]
		}
		return nil, fmt.Errorf("валидация слоя: %v (всего %d)", limit, len(errs))
	}
	if err := layer.VerifyHash(false); err != nil {
		return nil, err
	}
	return layer, nil
}
