// Package duration: скаляр времени (time.Duration) для YAML-файлов агента.
// Грамматика файлов ("5m", "24h", "15s") совместима с time.ParseDuration.
package duration

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration оборачивает time.Duration: в YAML это всегда строка.
type Duration struct {
	time.Duration
}

// UnmarshalYAML — yaml.v3 узловый интерфейс.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	s := strings.TrimSpace(node.Value)
	if s == "" {
		return fmt.Errorf("duration: пустое значение")
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration: %q: %w", s, err)
	}
	d.Duration = v
	return nil
}

// MarshalYAML — обратная сериализация в строку.
func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// MarshalJSON — в JSON тоже строка ("5m").
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}
