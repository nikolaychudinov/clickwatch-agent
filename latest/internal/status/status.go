// Package status: перечень статусов результата правила и их рангов.
// Источник: протокол обмена с сервером, разд. 4.1; жизненный цикл правил, разд. 10.
package status

const (
	None                = "none"
	Warning             = "warning"
	Critical            = "critical"
	Disabled            = "disabled"
	SkippedUnsupported  = "skipped_unsupported"
	SkippedTableMissing = "skipped_table_missing"
	SkippedNoAccess     = "skipped_no_access"
	ErrorQuery          = "error_query"
	Timeout             = "timeout"
	ConfigInvalid       = "config_invalid"
)

// SeverityRank — ранг severity для максимизации при оценке порогов:
// critical старше warning старше none. Порядок порогов в списке не важен.
func SeverityRank(s string) int {
	switch s {
	case Critical:
		return 3
	case Warning:
		return 2
	}
	return 1
}

// IsStatic — статусы переходов: они отправляются в SaaS только при смене
// (edge-detection), а не на каждом исполнении (см. правила Спецификация разд. 10).
func IsStatic(s string) bool {
	switch s {
	case Disabled, SkippedUnsupported, SkippedTableMissing, SkippedNoAccess:
		return true
	}
	return false
}

// LegalOps — разрешённые операции порогов (грамматика rules.yaml).
var LegalOps = map[string]bool{
	"gt": true, "gte": true, "lt": true, "lte": true, "eq": true, "neq": true,
}

// LegalSeverities — разрешённые severity порогов.
var LegalSeverities = map[string]bool{
	"none": true, "warning": true, "critical": true,
}
