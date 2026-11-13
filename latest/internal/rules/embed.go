// Встроенный слой правил (слой 1 из 3, описание формата правил (в README репозитория) разд. 1).
package rules

import _ "embed"

// EmbeddedRaw — содержимое rules_default.yaml, компилируемое в бинарник.
//
//go:embed rules_default.yaml
var EmbeddedRaw []byte
