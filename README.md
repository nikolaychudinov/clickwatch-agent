# ch-agent — мониторинговый агент ClickHouse на Go

Один статический бинарник, который встаёт на ноду ClickHouse, исполняет встроенный
набор правил из 48 точек контроля (копия `rules_default.yaml`, компилируется в
бинарник через `go:embed`), отправляет self-метрики агента (11 штук) и результаты
правил в SaaS. Кластер не зависит от версии ClickHouse: неприменимое
пропускается статическими статусами `skipped_unsupported` / `skipped_table_missing`,
а не ломает сбор.

Спецификации, которым следует реализация: `rules_default_usage_spec.md`
(жизненный цикл правил), `agent_yaml_spec.md` (конфигурация),
`protocol_agent_saas_spec.md` (протокол), `rules_yaml_spec.md` (грамматика правил).

## Возможности

- встроенный слой правил: 48 правил мониторинга + 11 self-метрик, канонический
  `dataset_hash` пересчитывается при старте (заглушка `sha256:computed_at_build`
  в исходнике недопустима к отправке);
- три слоя правил: embedded → слой SaaS (`rules_saas.yaml`) → локальный
  override (`rules.yaml`); побеждает слой с максимальным `dataset_version`;
- локальный override патчит только `thresholds` / `enabled` / `interval` по `uid`,
  поле `thresholds_lock` защищает пороги от перекрытия, а `thresholds_source`
  становится `local` у перекрытых правил;
- планировщик: по тикеру на правило с сохранением фазы, пул исполнителей
  `limits.max_concurrent_queries`, случайная фаза для новых правил, дневной
  бюджет исполнений по UTC-суткам без накопления долга при простое;
- персистентная очередь отправки (write-ahead): инжест-записи и события
  пишутся на диск до отправки; рестарт агента ничего не теряет — очередь
  восстанавливается при старте и досылается; повторные попытки с экспоненциальным
  backoff (2 с → 15 м, без потолка попыток), вытеснение старейших задач по
  `storage.buffer_max_bytes`, статистика очереди в логе на каждый цикл
  флаша (`send.flush_interval`);
- самопределение нагрузки: `SETTINGS max_threads` на каждом запросе клампится
  `limits.max_threads_per_query`, ClickHouse опрашивается только на чтение
  системных таблиц;
- self-метрики работают и при недоступном ClickHouse (`ch.agent.ch.unreachable_seconds`,
  `ch.agent.rules.*`, `ch.agent.access.*` и другие — полный реестр в
  `rules_default.yaml`, блок `self_rules`);
- статусы-переходы (`disabled`, `skipped_*`) отправляются в SaaS по смене
  (edge-detection), ошибка исполнения — по факту.

## Сборка

Нужен Go 1.22+. Единственная зависимость — `gopkg.in/yaml.v3`.

```sh
cd ch-agent
go build ./cmd/ch-agent
# версия сборки может перекрываться:
go build -ldflags "-X main.AgentVersion=1.3.0" ./cmd/ch-agent
```

## Быстрый старт

```sh
export CH_AGENT_PASSWORD="..."       # пароль учётной записи clickhouse.user
export CH_AGENT_API_KEY="..."        # только БЕЗ config.yaml: fleet-соль bootstrap (api_key_ref)

cp client.example.yaml config.yaml  # per-client provisioning ноды: client_id/client_salt
                                    # (из справочника clients коллектора) + реквизиты CH;
                                    # дефолтный путь — рядом с agent.yaml, иначе -client-config <путь>

./ch-agent -check -config agent.example.yaml            # офлайн: конфиг+правила+план
./ch-agent -config /etc/ch-agent/agent.yaml             # основной режим
./ch-agent -version
```

При наличии `config.yaml` агент подписывает ВСЕ запросы (register включительно)
заголовками `X-Client-Id` + `X-Client-Hash = HMAC-SHA256(client_salt, client_id)`:
client_id перекрывает `state.json`, client_salt — `identity.api_key_ref`
(fleet-соль остаётся fallback'ом только для агентов без config.yaml — bootstrap
register-флоу). Реквизиты `clickhouse:` из config.yaml перекрывают agent.yaml
(только заполненные поля). Без config.yaml агент идёт по обычному register-флоу.

Офлайн-режим `-check` не обращается ни к ClickHouse, ни к SaaS: он разбирает
конфиг, валидирует встроенный слой, печатает итог планирования для заявленной
версии (поле `clickhouse.version`) и строку статуса per-client provisioning
(`client: pre-provisioned client_id=…` либо «config.yaml не задан»).

Пример вывода (`agent.example.yaml` — заявлен 25.3, режим replicated):

```text
config: ok (saas=https://saas.company/monitoring, channel=stable)
client: config.yaml не задан — обычный register-флоу
rules: dataset_version=0 hash=sha256:e4e1ecbb8f371dbb584cacfcbe1c65fc048969d037570b0cc20d7d8e208916b3 rules=48 self=11 released=2026-10-29
plan: версия ClickHouse заявлена 25.3, режим replicated → исполняемых 47, статических 1
forecast: ~6726 исполнений/сутки при бюджете 20000
  статусы disabled: 1
 исполняемые правила:
    1   ch.mutations.active_summary                5m0s
    2   ch.mutations.failed                        5m0s
    ...
   47   ch.parts.duplicates                        1h0m0s
   48   ch.state.settings_values                   1h0m0s
 статические:
       ch.components.kafka_consumers              disabled
```

Прогноз ~6726 исполнений в сутки — это сумма `86400 / interval` рабочего набора.
Дефолтный бюджет спецификации (2000) будет отвергать хвост набора: либо
поднимайте `limits.daily_query_budget` (20000 в примере), либо режьте частоту.

## Структура

| Пакет | Ответственность |
| --- | --- |
| `cmd/ch-agent` | флаги `-config` / `-client-config` / `-check` / `-version`, сборка слоёв |
| `internal/agent` | оркестратор: публикация каталога, циклы probe / регистрация / статус / sync / доступы |
| `internal/rules` | слой 1 (`go:embed rules_default.yaml`), парсинг, валидация, канонический хеш, слияние 3 слоёв, diff |
| `internal/plan` | фильтр применимости: `since` / `until`, `applies_to`, `min_ch_version`, `enabled` |
| `internal/scheduler` | тикеры с сохранением фазы, джиттер, пул воркеров, дневной бюджет |
| `internal/runner` | исполнение правила: SETTINGS-кламп, retry по классам ошибок, sample-лимиты |
| `internal/evaluate` | оценка порогов; NULL-метрика пропускает порог, а не трактует нулём |
| `internal/chclient` | ClickHouse по HTTP-интерфейсу (`default_format=JSONEachRow`), таксономия ошибок |
| `internal/access` | кэш доступности системных таблиц (`ok` / `missing` / `denied`) и переоценка |
| `internal/saasclient` | register / rules fetch / per-rule инжест / events / статус клиента, конверт, пакетирование |
| `internal/buffer` | персистентная очередь отправки на диске (WAL): Append/Batch/Defer/Commit, восстановление при старте |
| `internal/state` | `state.json`: agent_uuid, client_id, agent_seq, applied ruleset, edge-кэш |
| `internal/selfloop` | цикл self-метрик по блоку `self_rules` |
| `internal/config`, `internal/duration`, `internal/chversion`, `internal/status` | конфиг, скаляр времени, сравнение версий, алфавиты статусов |

## Поведение при сбоях (кратко)

- ClickHouse недоступен: очередь задач не наполняется, `ch_unreachable` отправляется
  по смене и повтором `behavior.unreachable_alert_repeat`; после восстановления —
  `ch_recovered` с длительностью простоя;
- таблица отсутствует в сборке — `skipped_table_missing`, доступ закрыт —
  `skipped_no_access`; оба слова кэшируются и переоцениваются циклом
  `behavior.access_recheck_interval` микрозапросом `SELECT 1 ... LIMIT 1`;
- SaaS недоступен (сеть/5xx) — записи остаются в персистентной очереди,
  попытки повторяются с экспоненциальным backoff; HTTP 4xx — записи удаляются
  из очереди с ошибкой (повторы не исправят ошибку в данных); рестарт агента
  очередь сохраняет целиком;
- спящий режим (`suspended` / `revoked` / `trial_expired`): сбор правил и сверка
  приостанавливаются, self-метрика `ch.agent.status = 1`, события
  `dormant_entered` / `dormant_exited`;
- слой SaaS с ошибками валидации или несовпавшим хешем отбрасывается целиком,
  продолжает работать предыдущий каталог; событие `config_invalid` уходит в SaaS.

## Осознанные отклонения от спецификации

1. **ClickHouse по HTTP, а не нативный драйвер.** Мониторинговые запросы —
   read-only агрегаты по системным таблицам; HTTP-интерфейс покрывает их без
   тяжёлых зависимостей и одинаково работает на 21.8…25.x. Точка замены —
   тип `chclient.Client` (пара `Query`/`Probe`): нативная реализация
   подставляется без изменения остального кода.
2. **Конверт содержит поле `records`** (массив записей) — имя ключа уточнено
   по протоколу; остальная обвязка конверта соответствует разделу 2 протокола.
3. **Локальный `rules.yaml` — только правка.** Локальный слой не создаёт
   правил и не воскрешает удалённые из слоя SaaS; запись с неизвестным uid
   в оверлее молча игнорируется.
4. **Прогноз-бюджет**: агент не меняет сам ни пороги, ни частоты; при
   превышении прогнозом дневного бюджета он печатает предупреждение в
   `-check` и пишет его же в лог при публикации каталога.
5. **Хранилище скачанного слоя**: новые версии `rules_saas.yaml` ротируются
   в `.bak-1`…`.bak-5` (Usage spec разд. 12) — реализовано.
6. **Персистентная очередь — на файлах, без bbolt.** Прототип агента задач
   для гарантии «ничего не теряется на рестарте» предлагал очередь в bbolt;
   здесь тот же набор гарантий — задачи переживают рестарт (RecoverPending
   при старте), счётчик попыток, статистика, вытеснение по лимиту — при
   нулевых внешних зависимостях. Каждая задача — plain JSON-файл в
   `storage.buffer_dir`, при разборе инцидентов вскрывается редактором.
7. **Конверт собирается в момент отправки**, а не при постановке в очередь:
   после отлёжки в очереди `agent_seq` остаётся монотонным, `host_time` —
   актуальным.

## Тесты

```sh
go test ./...
```

Покрытие: парсинг и строгая валидация встроенных 46+11 (сквозные id 1..46,
нормируемые интервалы, заглушка хеша), оверлей и `thresholds_lock`, замещение
слоёв по `dataset_version`, гейты `since` / `until` в планировании, diff набора,
сравнение версий, оценка порогов (максимум severity, NULL-пропуск);
персистентная очередь: постановка, backoff-откладывание, группировка по пути
приёма, вытеснение по лимиту, восстановление после рестарта, сценарии
отправки (успех / временный отказ / HTTP 4xx) на тестовом сервере.
