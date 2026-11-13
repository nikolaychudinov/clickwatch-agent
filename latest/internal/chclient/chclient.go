// Package chclient: клиент ClickHouse по HTTP-интерфейсу (порт clickhouse.http_port).
//
// Осознанное отклонение от черновой идеи «нативный драйвер ch-go» (ФТ): мониторинговые
// запросы — read-only агрегаты по системным таблицам; HTTP-интерфейс покрывает их
// без внешних зависимостей и одинаково работает на 21.8…25.x. Границей для замены
// служит тип Client: его Query/RPOBE-методы легко подменить нативной реализацией.
package chclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Row — строка результата: имя колонки → значение (string/float64/bool/nil).
type Row map[string]any

// ErrClass — таксономия ошибок исполнения (документация по использованию разд. 8).
type ErrClass int

const (
	ErrNet ErrClass = iota + 1 // сетевой/транспортный отказ — допускает retry
	ErrTimeout                 // превышен execution_timeout
	ErrTableMissing            // таблицы нет в этой сборке — skipped_table_missing
	ErrNoAccess                // нет grants — skipped_no_access
	ErrSQL                     // прочие ошибки SQL — error_query
)

// Error — классифицированная ошибка; Msg не длиннее 200 символов.
type Error struct {
	Class ErrClass
	Code  int
	Msg   string
}

func (e *Error) Error() string {
	return fmt.Sprintf("clickhouse: класс %d, код %d: %s", int(e.Class), e.Code, e.Msg)
}

// AsError приводит произвольную ошибку к *Error (неизвестная — ErrSQL не нужна:
// разворачивание происходит на стороне вызовов через errors.As; тут — обходной путь).
func AsError(err error) (*Error, bool) {
	e, ok := err.(*Error)
	return e, ok
}

// Client — лёгкий HTTP-клиент CH. Адрес по умолчанию 127.0.0.1:
// агент ставится на саму ноду ClickHouse (см. формат agent.yaml разд. 4).
type Client struct {
	host     string
	port     int
	user     string
	password string
	hc       *http.Client
}

// New создаёт клиент; dialTimeout — лимит на установку соединения.
func New(host string, port int, user, password string, dialTimeout time.Duration) *Client {
	if strings.TrimSpace(host) == "" {
		host = "127.0.0.1"
	}
	if dialTimeout <= 0 {
		dialTimeout = 2 * time.Second
	}
	return &Client{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		hc: &http.Client{
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: dialTimeout}).DialContext,
				MaxIdleConns:        4,
				MaxIdleConnsPerHost: 4,
			},
		},
	}
}

var codeRe = regexp.MustCompile(`Code:\s*(\d+)`)

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

func classify(httpStatus int, body string) *Error {
	code := 0
	if m := codeRe.FindStringSubmatch(body); m != nil {
		code, _ = strconv.Atoi(m[1])
	}
	msg := truncate(strings.TrimSpace(body), 200)
	low := strings.ToLower(body)
	switch {
	case strings.Contains(low, "unknown_table"),
		strings.Contains(low, "unknown_database"),
		strings.Contains(low, "no_such_database"),
		strings.Contains(low, "no_such_table"),
		code == 60, code == 81:
		return &Error{Class: ErrTableMissing, Code: code, Msg: msg}
	case strings.Contains(low, "access_denied"),
		strings.Contains(low, "not_enough_privileges"),
		strings.Contains(low, "authentication failed"),
		httpStatus == 403,
		code == 510, code == 511, code == 516:
		return &Error{Class: ErrNoAccess, Code: code, Msg: msg}
	}
	return &Error{Class: ErrSQL, Code: code, Msg: msg}
}

// Query исполняет запрос и возвращает строки в формате JSONEachRow.
func (c *Client) Query(ctx context.Context, sql string) ([]Row, error) {
	q := url.Values{}
	q.Set("user", c.user)
	if c.password != "" {
		q.Set("password", c.password)
	}
	q.Set("default_format", "JSONEachRow")
	raw := fmt.Sprintf("http://%s:%d/?%s", c.host, c.port, q.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, raw, strings.NewReader(sql))
	if err != nil {
		return nil, &Error{Class: ErrSQL, Msg: truncate(err.Error(), 200)}
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := c.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, &Error{Class: ErrTimeout, Msg: truncate(ctx.Err().Error(), 200)}
		}
		return nil, &Error{Class: ErrNet, Msg: truncate(err.Error(), 200)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, &Error{Class: ErrNet, Msg: truncate(err.Error(), 200)}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, classify(resp.StatusCode, string(body))
	}

	var rows []Row
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var r Row
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, &Error{Class: ErrSQL, Msg: truncate("формат ответа: "+err.Error(), 200)}
		}
		rows = append(rows, r)
	}
	return rows, nil
}

// Probe — версия, время сервера и расхождение часов в одном дешёвом запросе.
type Probe struct {
	Version string
	DBNow   string
	TZ      string
	DBUnix  int64
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprint(v)
	}
}

// ProbeInfo — данные успешного probe-запроса.
func (c *Client) Probe(ctx context.Context) (*Probe, error) {
	rows, err := c.Query(ctx,
		"SELECT version() AS ver, now() AS srv_now, timezone() AS tz, toUnixTimestamp(now()) AS srv_unix")
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, &Error{Class: ErrSQL, Msg: "пустой ответ probe-запроса"}
	}
	p := &Probe{
		Version: asString(rows[0]["ver"]),
		DBNow:   asString(rows[0]["srv_now"]),
		TZ:      asString(rows[0]["tz"]),
	}
	switch t := rows[0]["srv_unix"].(type) {
	case string:
		p.DBUnix, _ = strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	case float64:
		p.DBUnix = int64(t)
	}
	return p, nil
}
