# Установка агента ClickWatch на Linux-сервер с ClickHouse

*Техническая инструкция для репозитория исходников: от получения кода из Git
до работающего демона `ch-agent` с персональными ключами клиента.*

---

## 0. Коротко о процессе

| Шаг | Что делается | Кто выполняет |
|---|---|---|
| 1 | Получить исходники из Git | администратор клиента |
| 2 | Собрать исполняемый файл `ch-agent` | администратор клиента |
| 3 | Создать учётную запись ClickHouse «только чтение» | администратор клиента (SQL) |
| 4 | Разложить конфигурацию `agent.yaml` | администратор клиента |
| 5 | Записать персональные ключи `client_id` + `client_salt` в `config.yaml` | администратор клиента (ключи выдаёт ClickWatch) |
| 6 | Прогнать офлайн-проверку `-check` | администратор клиента |
| 7 | Установить и запустить сервис systemd | администратор клиента |
| 8 | Убедиться в доставке данных в личный кабинет | администратор клиента |

Заведение пары `client_id` / `client_salt` в справочнике клиентов выполняется
на стороне ClickWatch при подключении; клиенту передаются только значения ключей.

---

## 1. Требования

- **ОС:** Linux x86_64 или arm64 (Debian/Ubuntu/RHEL-семейство; достаточно
  любой системы, работающей под systemd; запускать можно и без systemd).
- **Для сборки:** Go **1.22 или новее** (`go version`). Единственная внешняя
  зависимость модуля — `gopkg.in/yaml.v3` (её подтягивает `go build` сам).
*Сборку можно выполнить на любой машине с Go и перенести готовый бинарник
на сервер — на самом сервере Go не обязателен.*
- **ClickHouse:** 21.8 или новее. Пригодны и standalone, и replicated-сборки;
  неприменимые к версии проверки агент пропускает сам.
- **Сеть:**
  - доступ агент → ClickHouse по HTTP-порту (по умолчанию **8123**, локально);
  - исходящий HTTPS (443) от агента к серверу ClickWatch
    (адрес задаётся полем `send.endpoint_base`, например `https://agent.clickwatch.ru/monitoring`);
  - **входящих соединений агент не требует.**
- **Ресурсы:** ~50–100 МБ ОЗУ; до ~500 МБ диска для локального буфера отправки.

---

## 2. Получение исходников из Git

```sh
git clone <URL-репозитория> ch-agent
cd ch-agent
git checkout <тег-релиза>        # или ветка по рекомендации поддержки
```

В каталоге репозитория не должно быть исполняемых артефактов и локальных
конфигураций с секретами (см. раздел 11 «Безопасность», подраздел
«Ваши секреты — мимо Git»).

---

## 3. Сборка

```sh
cd ch-agent
go build -o /usr/local/bin/ch-agent ./cmd/ch-agent
```

Версию сборки удобно зафиксировать через ldflags:

```sh
go build -ldflags "-X main.AgentVersion=1.2.0" -o /usr/local/bin/ch-agent ./cmd/ch-agent
/usr/local/bin/ch-agent -version
```

Сборка без сети возможна, если модули уже в кэше (`go mod download`
выполняется один раз на машине сборки, у которой есть доступ к сети модулей).

---

## 4. Учётная запись ClickHouse (только чтение)

Агент выполняет только read-only запросы к **системным** таблицам
(`system.parts`, `system.mutations`, `system.replicas`, `system.query_log`,
`system.events`, `system.disks`, `system.asynchronous_metrics` и другие
из каталога правил). Строки таблиц пользователя он не читает.

SQL для ClickHouse (под администратором):

```sql
-- Профиль ограничений: защита даже в случае порчи конфига агента
CREATE SETTINGS PROFILE chwatch_profile
  SETTINGS max_execution_time = 5, max_threads = 2, readonly = 1;

-- Пользователь агента
CREATE USER chwatch_ro IDENTIFIED BY 'СЛОЖНЫЙ_ПАРОЛЬ'
  SETTINGS PROFILE 'chwatch_profile';

-- Права: чтение системных таблиц
GRANT SELECT ON system.* TO chwatch_ro;
```

Если версия ClickHouse не принимает `SETTINGS PROFILE` в `CREATE USER`,
выполните отдельно:

```sql
ALTER USER chwatch_ro SETTINGS PROFILE 'chwatch_profile';
```

Проверка:

```sh
clickhouse-client --user chwatch_ro --ask-password --query "SELECT 1"
clickhouse-client --user chwatch_ro --ask-password --query "SELECT count() FROM system.parts"
```

Агент общается с ClickHouse по **HTTP-интерфейсу** (порт 8123) — убедитесь,
что этот порт доступен агенту с той машины, где он запущен, и что пользователь
`chwatch_ro` не ограничен по источникам, если подключение не локальное.

---

## 5. Каталоги, системный пользователь и файлы

```sh
# системный пользователь без входа в систему
sudo useradd -r -d /var/lib/ch-agent -s /usr/sbin/nologin chwatch-agent

# каталоги
sudo mkdir -p /etc/ch-agent /var/lib/ch-agent/buffer
sudo chown -R chwatch-agent:chwatch-agent /etc/ch-agent /var/lib/ch-agent
sudo chmod 700 /etc/ch-agent /var/lib/ch-agent
```

Назначение:

| Путь | Что там лежит | Кто пишет |
|---|---|---|
| `/etc/ch-agent/agent.yaml` | главный конфиг агента | root (редактирование), агент читает |
| `/etc/ch-agent/config.yaml` | персональные ключи клиента | root (редактирование), агент читает |
| `/etc/ch-agent/rules.yaml` | (опционально) локальное перекрытие порогов/интервалов | root |
| `/etc/ch-agent/rules_saas.yaml` | скачанный с сервера актуальный набор правил (и ротация `.bak-1`…`.bak-5`) | агент |
| `/var/lib/ch-agent/state.json` | внутреннее состояние (UUID агента, счётчики) | агент |
| `/var/lib/ch-agent/buffer/` | персистентная очередь отправки (файлы-задачи) | агент |

Важно про каталог правил: агент ДОЛЖЕН уметь писать в
`storage.rules_dir` (по умолчанию `/etc/ch-agent/`) — именно поэтому
владельцем каталога назначается пользователь сервиса.

---

## 6. Главный конфиг `agent.yaml`

Скопируйте пример и заполните:

```sh
sudo cp ch-agent/agent.example.yaml /etc/ch-agent/agent.yaml
sudo nano /etc/ch-agent/agent.yaml
```

Обязательные для заполнения поля (остальное можно оставить как в примере):

```yaml
schema_version: 1

send:
  endpoint_base: "https://agent.clickwatch.ru/monitoring"   # выдаёт ClickWatch

clickhouse:
  host: "127.0.0.1"        # хост вашей ноды ClickHouse
  http_port: 8123
  tcp_port: 9000
  user: "chwatch_ro"
  password_env: "CH_AGENT_PASSWORD"   # имя переменной окружения с паролем
```

Краткая карта остальных блоков:

| Блок | Назначение | Дефолт |
|---|---|---|
| `identity` | версия, идентификация; `api_key_ref` НЕ нужен, если заполнен `config.yaml` (ключ клиента из `config.yaml` старше) | — |
| `node` | статический инвентарь ноды (hostname, IP, ОС) — можно заполнить по желанию | заполняется из примера |
| `send` | `endpoint_base`, таймауты, размер партии (50 результатов / 1 МБ) | flush 30s, timeout 10s |
| `sync` | канал и период обновления правил (`stable`, 15m) | 15m |
| `status` | период опроса статуса подписки | 15m |
| `behavior` | поведения при недоступности CH (backoff 10с→15м) и переопрос доступов | из примера |
| `storage` | пути к state/buffer, уровень лога | как в примере |
| `limits` | `max_concurrent_queries` (2), `max_threads_per_query` (2), `daily_query_budget` (обычно 20000 для полного набора), `min_ch_version` (21.8) | из примера |
| `labels` | ваши подписи (env, dc, service) — попадают в кабинет | по желанию |

Секрет-файл с паролем ClickHouse (имя переменной должно совпадать
с `clickhouse.password_env`):

```sh
sudo nano /etc/ch-agent/secrets.env
```

Содержимое (права 0600):

```sh
CH_AGENT_PASSWORD='СЛОЖНЫЙ_ПАРОЛЬ'
```

---

## 7. Персональные ключи клиента (`config.yaml`)

При подключении ClickWatch выдаёт вам два значения: **client_id** и
**client_salt**. Они подписывают все запросы агента к серверу
(`X-Client-Id` и `X-Client-Hash = HMAC-SHA256(client_salt, client_id)`).
Это единственные ваши «секреты» — храните так же, как пароли.

```sh
sudo cp ch-agent/client.example.yaml /etc/ch-agent/config.yaml
sudo nano /etc/ch-agent/config.yaml
```

Заполните:

```yaml
client_id: "ВЫДАННЫЙ-ID-КЛИЕНТА"
client_salt: "ВЫДАННЫЙ-КЛЮЧ-ПОДПИСИ"

# Реквизиты ClickHouse: заполняются те же, что в agent.yaml, и ПЕРЕКРЫВАЮТ
# их (удобно для кластера — один агентный образ, разные config.yaml на нодах)
clickhouse:
  host: "127.0.0.1"
  http_port: 8123
  tcp_port: 9000
  user: "chwatch_ro"
  password_env: "CH_AGENT_PASSWORD"
```

Настройте права:

```sh
sudo chown root:chwatch-agent /etc/ch-agent/config.yaml
sudo chmod 640 /etc/ch-agent/config.yaml
sudo chown root:root /etc/ch-agent/secrets.env
sudo chmod 600 /etc/ch-agent/secrets.env
sudo chown root:chwatch-agent /etc/ch-agent/agent.yaml
sudo chmod 640 /etc/ch-agent/agent.yaml
```

---

## 8. Офлайн-проверка перед запуском

```sh
sudo /usr/local/bin/ch-agent -check \
  -config /etc/ch-agent/agent.yaml \
  -client-config /etc/ch-agent/config.yaml
```

`-check` не обращается ни к ClickHouse, ни к серверу ClickWatch: он разбирает
конфиг, валидирует встроенный каталог правил, печатает план исполнения для
заявленной версии ClickHouse и строку состояния клиента. Ожидаем примерно:

```text
config: ok (saas=https://agent.clickwatch.ru/monitoring, channel=stable)
client: pre-provisioned client_id=<ВЫДАННЫЙ-ID>
rules: dataset_version=... rules=48 self=11 ...
plan: версия ClickHouse заявлена 25.3, режим replicated → исполняемых 47, статических 1
forecast: ~6726 исполнений/сутки при бюджете 20000
```

Если «forecast … при бюджете» показывает превышение — поднимите
`limits.daily_query_budget` (в примере 20000) или снизьте частоты правил.

---

## 9. Сервис systemd

```sh
sudo nano /etc/systemd/system/ch-agent.service
```

```ini
[Unit]
Description=ClickWatch monitoring agent (ClickHouse)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=chwatch-agent
Group=chwatch-agent
EnvironmentFile=/etc/ch-agent/secrets.env
ExecStart=/usr/local/bin/ch-agent \
  -config /etc/ch-agent/agent.yaml \
  -client-config /etc/ch-agent/config.yaml
Restart=on-failure
RestartSec=10

NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/etc/ch-agent /var/lib/ch-agent

[Install]
WantedBy=multi-user.target
```

Запуск и контроль:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now ch-agent
journalctl -u ch-agent -f
```

Что увидеть в логе после старта: строку о публикации каталога правил,
процессы флаша буфера каждые `send.flush_interval` (30 с), отсутствие
ошибок доступа к ClickHouse.

---

## 10. Проверка доставки

1. В личном кабинете ClickWatch должен появиться статус клиента «активен»
   и первые данные по правилам (первая волна — в пределах минуты после
   первого флаша).
2. При недоступности сервера ClickWatch записи копятся в
   `/var/lib/ch-agent/buffer/` и досылаются автоматически после
   восстановления связи (рестарт агента очередь сохраняет).
3. При недоступности самого ClickHouse агент уведомляет отдельным
   событием и продолжает посылять служебные показатели.

Больше деталей диагностики — в логе (`storage.log_level: debug` в
`agent.yaml`) и в выводе `-check`.

---

## 11. Безопасность

- **Минимальные права CH:** пользователь `chwatch_ro` — только `SELECT` на
  `system.*`, профиль с `readonly=1`, лимит потоков и таймаута.
- **Секреты вне конфигов:** пароль ClickHouse — через переменную окружения;
  `client_salt` — в `config.yaml` с правами 640 (root:chwatch-agent).
- **Подпись запросов:** каждый запрос к серверу подписывается
  HMAC-SHA256(client_salt, client_id) — подделать личность клиента нельзя.
- **TLS:** проверка сертификатов включена (`insecure_skip_verify: false` в
  примере не менять); для собственного корневого сертификата — `send.tls_ca`.
- **Только исходящие соединения:** порт 8123 ClickHouse должен быть доступен
  агенту, наружу агент не слушает ни одного порта.

### Ваши секреты — мимо Git

Если рабочая копия репозитория (или собственный форк) живёт рядом с конфигурацией
`/etc/ch-agent`, проследите, чтобы в Git не попало:

| Файл/путь | Почему |
|---|---|
| `config.yaml` с вашими `client_id` / `client_salt` | персональные ключи подписи |
| рабочий `agent.yaml` с адресами вашей инфраструктуры | внутренние детали |
| `/var/lib/ch-agent/state.json` и содержимое `buffer/` | рабочее состояние агента |
| скачанный слой правил `rules_saas.yaml`, локальный `rules.yaml`, `*.bak-*` | обновляются автоматически |
| собранные исполняемые файлы (`ch-agent`, `ch-agent.exe`, `*.tar.gz`) | артефакты сборки |

Рекомендуемый фрагмент `.gitignore` для корня рабочей копии:

```gitignore
config.yaml
agent.yaml
rules.yaml
rules_saas.yaml
rules_saas.yaml.bak-*
state.json
buffer/
ch-agent
ch-agent.exe
*.tar.gz
```

В репозитории при этом остаются: исходный код, `go.mod`/`go.sum`,
файлы-примеры (`agent.example.yaml`, `client.example.yaml` — без секретов),
встроенный каталог правил `internal/rules/rules_default.yaml` и эта документация.

---

## 12. Обновление и удаление

**Обновление агента:**

```sh
cd ch-agent && git pull
go build -o /usr/local/bin/ch-agent ./cmd/ch-agent
sudo systemctl restart ch-agent
```

Персистентная очередь гарантирует: во время обновления ничего не теряется.

**Обновление правил** — не требуется: набор скачивается агентом сам
(канал `stable`), ротация хранится рядом.

**Удаление:**

```sh
sudo systemctl disable --now ch-agent
sudo rm /usr/local/bin/ch-agent /etc/systemd/system/ch-agent.service
sudo rm -rf /etc/ch-agent /var/lib/ch-agent
sudo userdel chwatch-agent
```

И в ClickHouse (по желанию):

```sql
DROP USER chwatch_ro;
DROP SETTINGS PROFILE chwatch_profile;
```

---

## 13. Типовые проблемы

| Симптом | Что проверить |
|---|---|
| `config: ... пуст endpoint_base` | не заполнен `send.endpoint_base` в `agent.yaml` |
| `-check`: `client: config.yaml не задан` | не передан `-client-config` или файл не заполнен (нужны ОБА поля: id и salt) |
| в логе повторяется «ClickHouse недоступен»/backoff | пароль учётной записи, порт 8123, хост в `clickhouse:` |
| «skipped_no_access» по правилам | пользователь CH без доступа к системным таблицам → проверьте `GRANT SELECT ON system.*` |
| записи копятся в `buffer/`, кабинет пуст | нет исходящего HTTPS до `send.endpoint_base` (файрвол/прокси) |
| «forecast … превышает бюджет» | поднимите `limits.daily_query_budget` или снизьте частоты правил |

---

*Контакт для вопросов по установке — поддержка ClickWatch.*
