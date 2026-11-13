// Package chversion: сравнение версий ClickHouse для гейтов since/until.
// Точность — (мажор, минор); патч-разряды игнорируются:
// rule.since "22.3" применим к "22.3.1.1" и не применим к "22.2.3.5".
package chversion

import (
	"fmt"
	"strconv"
	"strings"
)

// V — пара мажор.минор.
type V struct {
	Major, Minor int
}

// Parse разбирает "25.3.3.42", "22.3", "21.8.1".
func Parse(s string) (V, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return V{}, fmt.Errorf("version: %q: ожидалось major.minor", s)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return V{}, fmt.Errorf("version: %q: %w", s, err)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return V{}, fmt.Errorf("version: %q: %w", s, err)
	}
	if major < 0 || minor < 0 {
		return V{}, fmt.Errorf("version: %q: отрицательные разряды", s)
	}
	return V{Major: major, Minor: minor}, nil
}

func (v V) String() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

// Cmp возвращает -1 / 0 / 1.
func Cmp(a, b V) int {
	if a.Major != b.Major {
		if a.Major > b.Major {
			return 1
		}
		return -1
	}
	if a.Minor != b.Minor {
		if a.Minor > b.Minor {
			return 1
		}
		return -1
	}
	return 0
}

// Gte: version >= since (семантика since включающая).
func Gte(version, since V) bool { return Cmp(version, since) >= 0 }

// Lte: version <= until (семантика until включающая).
func Lte(version, until V) bool { return Cmp(version, until) <= 0 }
