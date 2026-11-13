// Package config: загрузка и дефолты agent.yaml.
// Источник: формат agent.yaml (блоки identity / node / clickhouse / send / sync /
// status / behavior / storage / limits / labels).
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"chagent/internal/duration"
)

// Config — корень agent.yaml.
type Config struct {
	SchemaVersion int    `yaml:"schema_version"`
	Identity      Identity `yaml:"identity"`
	Node          Node     `yaml:"node"`
	Clickhouse    Clickhouse `yaml:"clickhouse"`
	Send          Send     `yaml:"send"`
	Sync          Sync     `yaml:"sync"`
	Status        Status   `yaml:"status"`
	Behavior      Behavior `yaml:"behavior"`
	Storage       Storage  `yaml:"storage"`
	Limits        Limits   `yaml:"limits"`
	Labels        map[string]string `yaml:"labels"`
}

// Identity — кто агент.
type Identity struct {
	AgentUUID          string `yaml:"agent_uuid"`
	AgentVersion       string `yaml:"agent_version"`
	TenantID           string `yaml:"tenant_id"`
	APIKeyRef          string `yaml:"api_key_ref"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// Node — статический инвентарь ноды.
type Node struct {
	Hostname    string   `yaml:"hostname"`
	FQDN        string   `yaml:"fqdn"`
	PrimaryIP   string   `yaml:"primary_ip"`
	IPAddresses []string `yaml:"ip_addresses"`
	OSName      string   `yaml:"os_name"`
	OSVersion   string   `yaml:"os_version"`
	Kernel      string   `yaml:"kernel"`
	CPUCores    int      `yaml:"cpu_cores"`
	RAMBytes    int64    `yaml:"ram_bytes"`
	DisksCount  int      `yaml:"disks_count"`
	Timezone    string   `yaml:"timezone"`
}

// Clickhouse — инвентарь конфигурации ClickHouse.
type Clickhouse struct {
	Host              string            `yaml:"host"`
	Version           string            `yaml:"version"`
	Distribution      string            `yaml:"distribution"`
	InstallType       string            `yaml:"install_type"`
	ClusterNames      []string          `yaml:"cluster_names"`
	IsLocalInCluster  *bool             `yaml:"is_local_in_cluster"`
	ShardNum          int               `yaml:"shard_num"`
	ReplicaNum        int               `yaml:"replica_num"`
	ReplicationMode   string            `yaml:"replication_mode"`
	ClusterTopology   string            `yaml:"cluster_topology"`
	TCPPort           int               `yaml:"tcp_port"`
	HTTPPort          int               `yaml:"http_port"`
	User              string            `yaml:"user"`
	PasswordEnv       string            `yaml:"password_env"`
	ConnectionTimeout duration.Duration `yaml:"connection_timeout"`
	ServerTime        string            `yaml:"server_time"`
	ServerTimezone    string            `yaml:"server_timezone"`
	CapturedAt        string            `yaml:"captured_at"`
}

// Send — куда и как отправлять результаты.
type Send struct {
	EndpointBase       string            `yaml:"endpoint_base"`
	IngestPathTemplate string            `yaml:"ingest_path_template"`
	EventsEndpoint     string            `yaml:"events_endpoint"`
	RegisterEndpoint   string            `yaml:"register_endpoint"`
	BatchMaxResults    int               `yaml:"batch_max_results"`
	BatchMaxBytes      int               `yaml:"batch_max_bytes"`
	FlushInterval      duration.Duration `yaml:"flush_interval"`
	HTTPTimeout        duration.Duration `yaml:"http_timeout"`
	TLSCA              string            `yaml:"tls_ca"`
}

// Sync — обновление правил.
type Sync struct {
	RulesEndpoint     string            `yaml:"rules_endpoint"`
	Channel           string            `yaml:"channel"`
	PollInterval      duration.Duration `yaml:"poll_interval"`
	CacheTTLFailure   duration.Duration `yaml:"cache_ttl_on_failure"`
	MaxDatasetAge     duration.Duration `yaml:"max_dataset_age"`
}

// Status — статус клиента.
type Status struct {
	StatusEndpoint string            `yaml:"status_endpoint"`
	PollInterval   duration.Duration `yaml:"poll_interval"`
	CacheState     duration.Duration `yaml:"cache_state"`
}

// Behavior — недоступность CH и возврат доступов.
type Behavior struct {
	CHProbeQuery           string            `yaml:"ch_probe_query"`
	CHProbeInterval        duration.Duration `yaml:"ch_probe_interval"`
	BackoffInitial         duration.Duration `yaml:"ch_backoff_initial"`
	BackoffMultiplier      float64           `yaml:"ch_backoff_multiplier"`
	BackoffMax             duration.Duration `yaml:"ch_backoff_max"`
	UnreachableAlertRepeat duration.Duration `yaml:"unreachable_alert_repeat"`
	AccessRecheckInterval  duration.Duration `yaml:"access_recheck_interval"`
	AccessAlertMode        string            `yaml:"access_alert_mode"`
}

// Storage — файлы состояния.
type Storage struct {
	StateFile      string `yaml:"state_file"`
	RulesDir       string `yaml:"rules_dir"`
	BufferDir      string `yaml:"buffer_dir"`
	BufferMaxBytes int64  `yaml:"buffer_max_bytes"`
	LogLevel       string `yaml:"log_level"`
	LogFile        string `yaml:"log_file"`
}

// Limits — самопределение нагрузки на ClickHouse.
type Limits struct {
	MaxConcurrentQueries int    `yaml:"max_concurrent_queries"`
	MaxThreadsPerQuery   int    `yaml:"max_threads_per_query"`
	DailyQueryBudget     int    `yaml:"daily_query_budget"`
	MinCHVersion         string `yaml:"min_ch_version"`
}

// Load читает и разбирает agent.yaml, применяет дефолты и спокойные проверки.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Send.IngestPathTemplate == "" {
		c.Send.IngestPathTemplate = "/v2/ingest/{{client_id}}/{{rule_uid}}"
	}
	if c.Send.EventsEndpoint == "" {
		c.Send.EventsEndpoint = "/v2/agent/events"
	}
	if c.Send.RegisterEndpoint == "" {
		c.Send.RegisterEndpoint = "/v2/clients/register"
	}
	if c.Send.BatchMaxResults <= 0 {
		c.Send.BatchMaxResults = 50
	}
	if c.Send.BatchMaxBytes <= 0 {
		c.Send.BatchMaxBytes = 1048576
	}
	if c.Send.FlushInterval.Duration == 0 {
		c.Send.FlushInterval = duration.Duration{Duration: 30_000_000_000} // 30s
	}
	if c.Send.HTTPTimeout.Duration == 0 {
		c.Send.HTTPTimeout = duration.Duration{Duration: 10_000_000_000} // 10s
	}
	if c.Sync.RulesEndpoint == "" {
		c.Sync.RulesEndpoint = "/v2/rules/latest"
	}
	if c.Sync.Channel == "" {
		c.Sync.Channel = "stable"
	}
	if c.Sync.PollInterval.Duration == 0 {
		c.Sync.PollInterval = duration.Duration{Duration: 900_000_000_000} // 15m
	}
	if c.Sync.CacheTTLFailure.Duration == 0 {
		c.Sync.CacheTTLFailure = duration.Duration{Duration: 300_000_000_000} // 5m
	}
	if c.Sync.MaxDatasetAge.Duration == 0 {
		c.Sync.MaxDatasetAge = duration.Duration{Duration: 600 * 3_600_000_000_000} // 600h
	}
	if c.Status.StatusEndpoint == "" {
		c.Status.StatusEndpoint = "/v2/clients/{{client_id}}/status"
	}
	if c.Status.PollInterval.Duration == 0 {
		c.Status.PollInterval = duration.Duration{Duration: 900_000_000_000} // 15m
	}
	if c.Status.CacheState.Duration == 0 {
		c.Status.CacheState = duration.Duration{Duration: 24 * 3_600_000_000_000} // 24h
	}
	if c.Behavior.CHProbeQuery == "" {
		c.Behavior.CHProbeQuery = "SELECT 1"
	}
	if c.Behavior.CHProbeInterval.Duration == 0 {
		c.Behavior.CHProbeInterval = duration.Duration{Duration: 10_000_000_000} // 10s
	}
	if c.Behavior.BackoffInitial.Duration == 0 {
		c.Behavior.BackoffInitial = duration.Duration{Duration: 10_000_000_000}
	}
	if c.Behavior.BackoffMultiplier <= 1 {
		c.Behavior.BackoffMultiplier = 2.0
	}
	if c.Behavior.BackoffMax.Duration == 0 {
		c.Behavior.BackoffMax = duration.Duration{Duration: 15 * 60_000_000_000} // 15m
	}
	if c.Behavior.UnreachableAlertRepeat.Duration == 0 {
		c.Behavior.UnreachableAlertRepeat = duration.Duration{Duration: 3_600_000_000_000} // 1h
	}
	if c.Behavior.AccessRecheckInterval.Duration == 0 {
		c.Behavior.AccessRecheckInterval = duration.Duration{Duration: 1_800_000_000_000} // 30m
	}
	if c.Behavior.AccessAlertMode == "" {
		c.Behavior.AccessAlertMode = "on_change"
	}
	if c.Storage.BufferMaxBytes <= 0 {
		c.Storage.BufferMaxBytes = 512 * 1024 * 1024 // 512 MiB
	}
	if c.Storage.LogLevel == "" {
		c.Storage.LogLevel = "info"
	}
	if c.Limits.MaxConcurrentQueries <= 0 {
		c.Limits.MaxConcurrentQueries = 2
	}
	if c.Limits.MaxThreadsPerQuery <= 0 {
		c.Limits.MaxThreadsPerQuery = 2
	}
	if c.Limits.DailyQueryBudget <= 0 {
		c.Limits.DailyQueryBudget = 2000
	}
	if c.Limits.MinCHVersion == "" {
		c.Limits.MinCHVersion = "21.8"
	}
	if c.Clickhouse.User == "" {
		c.Clickhouse.User = "default"
	}
}

func (c *Config) validate() error {
	if c.SchemaVersion != 1 {
		return fmt.Errorf("config: schema_version = %d, ожидается 1", c.SchemaVersion)
	}
	if c.Send.EndpointBase == "" {
		return fmt.Errorf("config: send.endpoint_base пуст")
	}
	if c.Clickhouse.HTTPPort <= 0 {
		return fmt.Errorf("config: clickhouse.http_port не задан")
	}
	if strings.Contains(c.Send.EndpointBase, " ") {
		return fmt.Errorf("config: send.endpoint_base содержит пробел")
	}
	return nil
}

// APIKey разворачивает api_key_ref: "env:NAME" или "file:///путь".
func (i Identity) APIKey() string {
	ref := strings.TrimSpace(i.APIKeyRef)
	switch {
	case ref == "":
		return ""
	case strings.HasPrefix(ref, "env:"):
		return os.Getenv(strings.TrimPrefix(ref, "env:"))
	case strings.HasPrefix(ref, "file://"):
		p := strings.TrimPrefix(ref, "file://")
		b, err := os.ReadFile(p)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	return ref
}

// ClientFile — per-client provisioning-файл ноды (config.yaml (per-client provisioning)).
// Один файл на ноду, заполняется администратором: client_id и client_salt выдаются
// при создании клиента в справочнике clients коллектора; clickhouse — реквизиты
// подключения агента к ноде ClickHouse (перекрывают agent.yaml, если заполнены).
type ClientFile struct {
	ClientID   string     `yaml:"client_id"`
	ClientSalt string     `yaml:"client_salt"` // соль подписи; секрет — держать 0600
	Clickhouse Clickhouse `yaml:"clickhouse"`
}

// Loaded сообщает: задан ли per-client provisioning целиком.
func (c *ClientFile) Loaded() bool {
	return c != nil && c.ClientID != "" && c.ClientSalt != ""
}

// LoadClientFile читает config.yaml; файл отсутствует → (nil, nil) — per-client
// provisioning не используется, агент живёт по agent.yaml (register-флоу).
func LoadClientFile(path string) (*ClientFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("client config: %w", err)
	}
	var c ClientFile
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("client config: %w", err)
	}
	c.ClientID = strings.TrimSpace(c.ClientID)
	c.ClientSalt = strings.TrimSpace(c.ClientSalt)
	if (c.ClientID == "") != (c.ClientSalt == "") {
		return nil, fmt.Errorf("client config: client_id и client_salt задаются только вместе")
	}
	return &c, nil
}

// ApplyClientFile перекрывает агентские реквизиты ClickHouse per-client значениями
// из config.yaml (только реально заполненные поля). Выбор соли — в agent.New.
func ApplyClientFile(cfg *Config, cli *ClientFile) {
	if cli == nil {
		return
	}
	ch := cli.Clickhouse
	if ch.Host != "" {
		cfg.Clickhouse.Host = ch.Host
	}
	if ch.User != "" {
		cfg.Clickhouse.User = ch.User
	}
	if ch.PasswordEnv != "" {
		cfg.Clickhouse.PasswordEnv = ch.PasswordEnv
	}
	if ch.HTTPPort != 0 {
		cfg.Clickhouse.HTTPPort = ch.HTTPPort
	}
	if ch.TCPPort != 0 {
		cfg.Clickhouse.TCPPort = ch.TCPPort
	}
	if ch.ConnectionTimeout.Duration > 0 {
		cfg.Clickhouse.ConnectionTimeout = ch.ConnectionTimeout
	}
}

// CHPassword возвращает пароль учётной записи CH из переменной окружения
// (имя задаётся clickhouse.password_env). Пусто, если не задан.
func (c Config) CHPassword() string {
	if c.Clickhouse.PasswordEnv == "" {
		return ""
	}
	return os.Getenv(c.Clickhouse.PasswordEnv)
}
