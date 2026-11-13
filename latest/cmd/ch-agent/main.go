// ch-agent — go-агент мониторинга ClickHouse.
//
// Запуск:
//
//	ch-agent -config /etc/ch-agent/agent.yaml
//
// Служебные режимы:
//
//	ch-agent -check -config agent.yaml   # разбор конфига/правил/плана без CH и SaaS
//	ch-agent -version                    # версия сборки
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"

	"chagent/internal/agent"
	"chagent/internal/chversion"
	"chagent/internal/config"
	"chagent/internal/plan"
	"chagent/internal/rules"
)

// AgentVersion — версия сборки; может перекрываться ldflags:
//
//	go build -ldflags "-X main.AgentVersion=1.3.0" ./cmd/ch-agent
var AgentVersion = "1.2.0"

const defaultRulesDir = "/etc/ch-agent"

func buildLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func main() {
	cfgPath := flag.String("config", "", "путь к agent.yaml")
	clientPath := flag.String("client-config", "",
		"путь к per-client config.yaml (по умолчанию config.yaml рядом с agent.yaml)")
	check := flag.Bool("check", false, "проверка конфигурации и плана правил; без сети")
	showVersion := flag.Bool("version", false, "показать версию и выйти")
	flag.Parse()

	if *showVersion {
		fmt.Println("ch-agent", AgentVersion)
		return
	}
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "не указан путь к конфигурации: ch-agent -config /etc/ch-agent/agent.yaml")
		os.Exit(2)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	// per-client provisioning: config.yaml (client_id + client_salt + реквизиты CH)
	if *clientPath == "" {
		*clientPath = filepath.Join(filepath.Dir(*cfgPath), "config.yaml")
	}
	cli, err := config.LoadClientFile(*clientPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
	config.ApplyClientFile(cfg, cli)

	// фактическая версия сборки всегда перечитывается (формат agent.yaml, разд. 2)
	cfg.Identity.AgentVersion = AgentVersion
	log := buildLogger(cfg.Storage.LogLevel)

	// слой 1: встроенный набор (компилирован в бинарник)
	emb, err := rules.Parse(rules.EmbeddedRaw)
	if err != nil {
		fmt.Fprintln(os.Stderr, "embedded rules: "+err.Error())
		os.Exit(1)
	}
	if errs := rules.Validate(emb, true); len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "embedded rules: %d ошибок: %v\n", len(errs), errs)
		os.Exit(1)
	}
	emb.RecoverHash()

	// слой 3: локальный override; битый слой игнорируется целиком (документация по использованию разд. 4)
	overlay, err := rules.LoadOverlay(overlayPath(cfg))
	if err != nil {
		fmt.Fprintln(os.Stderr, "слой 3 невалиден и будет проигнорирован: "+err.Error())
		overlay = nil
	}

	if *check {
		if err := runCheck(cfg, emb, overlay, cli); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		return
	}

	ag, err := agent.New(*cfg, log, emb, overlay, cfg.Storage.StateFile, cli)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if rerr := ag.Run(ctx); rerr != nil && ctx.Err() == nil {
		log.Error("агент остановлен ошибкой", "err", rerr.Error())
		os.Exit(1)
	}
	log.Info("агент остановлен")
}

// overlayPath — слой 3 всегда лежит как rules.yaml в storage.rules_dir
// (кэш скачанного из SaaS слоя — соседний файл rules_saas.yaml).
func overlayPath(cfg *config.Config) string {
	dir := cfg.Storage.RulesDir
	if dir == "" {
		dir = defaultRulesDir
	}
	return dir + string(os.PathSeparator) + "rules.yaml"
}

// runCheck — офлайн-проверка: конфиг+встроенные правила+план; сеть не используется.
func runCheck(cfg *config.Config, emb *rules.Ruleset, overlay *rules.Overlay, cli *config.ClientFile) error {
	cat, err := rules.BuildCatalog(emb, nil, overlay)
	if err != nil {
		return err
	}
	mode := cfg.Clickhouse.ReplicationMode
	if mode == "" {
		mode = "standalone"
	}
	minV, err2 := chversion.Parse(cfg.Limits.MinCHVersion)
	if err2 != nil {
		return fmt.Errorf("limits.min_ch_version: %w", err2)
	}
	ver, err2 := chversion.Parse(cfg.Clickhouse.Version)
	if err2 != nil {
		return fmt.Errorf("clickhouse.version: %w", err2)
	}
	env := plan.Env{Version: ver, MinVersion: minV, Mode: mode}
	p := plan.Build(cat, env)

	fmt.Printf("config: ok (saas=%s, channel=%s)\n", cfg.Send.EndpointBase, cfg.Sync.Channel)
	if cli.Loaded() {
		fmt.Printf("client: pre-provisioned client_id=%s (per-client salt из config.yaml)\n", cli.ClientID)
	} else {
		fmt.Println("client: config.yaml не задан — обычный register-флоу")
	}
	fmt.Printf("rules: dataset_version=%d hash=%s rules=%d self=%d released=%s\n",
		cat.Meta.DatasetVersion, cat.Meta.DatasetHash, len(cat.Rules), len(cat.SelfRules), cat.Meta.ReleasedAt)
	fmt.Printf("plan: версия ClickHouse заявлена %s, режим %s → исполняемых %d, статических %d\n",
		p.Version.String(), p.Mode, len(p.Runnables), len(p.Static))
	fmt.Printf("forecast: ~%d исполнений/сутки при бюджете %d\n", p.ForecastPerDay, cfg.Limits.DailyQueryBudget)

	byStatus := map[string]int{}
	for _, st := range p.Static {
		byStatus[st]++
	}
	keys := make([]string, 0, len(byStatus))
	for k := range byStatus {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, st := range keys {
		fmt.Printf("  статусы %s: %d\n", st, byStatus[st])
	}
	fmt.Println(" исполняемые правила:")
	for _, r := range p.Runnables {
		fmt.Printf("   %-3d %-42s %s\n", r.ID, r.UID, r.EffInterval)
	}
	type stPair struct {
		uid, st string
	}
	pairs := make([]stPair, 0, len(p.Static))
	for uid, st := range p.Static {
		pairs = append(pairs, stPair{uid, st})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].uid < pairs[j].uid })
	fmt.Println(" статические:")
	for _, pr := range pairs {
		fmt.Printf("       %-42s %s\n", pr.uid, pr.st)
	}
	if p.ForecastPerDay > cfg.Limits.DailyQueryBudget {
		fmt.Println()
		fmt.Println("ВНИМАНИЕ: прогноз превышает limits.daily_query_budget — скорректируйте бюджет или интервалы.")
	}
	return nil
}
