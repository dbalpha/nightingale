package tencentcls

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ccfos/nightingale/v6/datasource"
	clskit "github.com/ccfos/nightingale/v6/dskit/tencentcls"
	"github.com/ccfos/nightingale/v6/models"

	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/common/model"
)

const TencentCLSType = "tencent-cls"

type TencentCLS struct {
	clskit.TencentCLS `json:",inline" mapstructure:",squash"`
}

type Query struct {
	Ref      string          `json:"ref" mapstructure:"ref"`
	Query    string          `json:"query" mapstructure:"query"`
	TopicID  string          `json:"topic_id" mapstructure:"topic_id"`
	Topics   []string        `json:"topics" mapstructure:"topics"`
	Keys     datasource.Keys `json:"keys" mapstructure:"keys"`
	Start    int64           `json:"start" mapstructure:"start"`
	End      int64           `json:"end" mapstructure:"end"`
	From     int64           `json:"from" mapstructure:"from"`
	To       int64           `json:"to" mapstructure:"to"`
	Time     int64           `json:"time" mapstructure:"time"`
	Interval int64           `json:"interval" mapstructure:"interval"`
	Offset   int64           `json:"offset" mapstructure:"offset"`
	Limit    int             `json:"limit" mapstructure:"limit"`
	Sort     string          `json:"sort" mapstructure:"sort"`
}

func init() {
	datasource.RegisterDatasource(TencentCLSType, new(TencentCLS))
}

func (c *TencentCLS) Init(settings map[string]interface{}) (datasource.Datasource, error) {
	newest := new(TencentCLS)
	if err := mapstructure.Decode(settings, newest); err != nil {
		return nil, err
	}
	return newest, nil
}

func (c *TencentCLS) InitClient() error {
	return c.InitHTTPClient()
}

func (c *TencentCLS) Validate(ctx context.Context) error {
	if strings.TrimSpace(c.SecretID) == "" {
		return fmt.Errorf("tencent-cls.secret_id is required")
	}
	if strings.TrimSpace(c.SecretKey) == "" {
		return fmt.Errorf("tencent-cls.secret_key is required")
	}
	if strings.TrimSpace(c.Region) == "" {
		return fmt.Errorf("tencent-cls.region is required")
	}
	if c.Timeout == 0 {
		c.Timeout = clskit.DefaultTimeout
	}
	if c.MaxQueryRows == 0 {
		c.MaxQueryRows = clskit.DefaultMaxQueryRows
	}
	if c.Endpoint == "" {
		c.Endpoint = clskit.DefaultEndpoint
	}
	return nil
}

func (c *TencentCLS) Equal(other datasource.Datasource) bool {
	o, ok := other.(*TencentCLS)
	if !ok {
		return false
	}
	return c.SecretID == o.SecretID &&
		c.SecretKey == o.SecretKey &&
		c.Region == o.Region &&
		c.Endpoint == o.Endpoint &&
		c.TopicID == o.TopicID &&
		c.Timeout == o.Timeout &&
		c.MaxQueryRows == o.MaxQueryRows &&
		c.ClusterName == o.ClusterName
}

func (c *TencentCLS) QueryData(ctx context.Context, queryParam interface{}) ([]models.DataResp, error) {
	param, err := decodeQuery(queryParam)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(param.Query) == "" {
		return nil, fmt.Errorf("query is required")
	}
	if strings.TrimSpace(param.Keys.ValueKey) == "" {
		return nil, fmt.Errorf("valueKey is required")
	}

	req, err := c.buildSearchLogRequest(ctx, param, true)
	if err != nil {
		return nil, err
	}

	resp, err := c.SearchLog(ctx, req)
	if err != nil {
		return nil, err
	}

	rows, err := parseAnalysisRecords(resp.AnalysisRecords)
	if err != nil {
		return nil, err
	}
	return convertRowsToDataResp(rows, param.Keys, param.Ref, param.Query, req.To), nil
}

func (c *TencentCLS) QueryLog(ctx context.Context, queryParam interface{}) ([]interface{}, int64, error) {
	param, err := decodeQuery(queryParam)
	if err != nil {
		return nil, 0, err
	}
	if strings.TrimSpace(param.Query) == "" {
		param.Query = "*"
	}

	req, err := c.buildSearchLogRequest(ctx, param, true)
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.SearchLog(ctx, req)
	if err != nil {
		return nil, 0, err
	}

	items := convertLogs(resp.Results)
	if len(items) == 0 && len(resp.AnalysisRecords) > 0 {
		rows, err := parseAnalysisRecords(resp.AnalysisRecords)
		if err != nil {
			return nil, 0, err
		}
		items = make([]interface{}, 0, len(rows))
		for i := range rows {
			items = append(items, rows[i])
		}
	}

	total := resp.LogCount
	if total == 0 {
		total = int64(len(items))
	}
	return items, total, nil
}

func (c *TencentCLS) QueryMapData(ctx context.Context, query interface{}) ([]map[string]string, error) {
	param, err := decodeQuery(query)
	if err != nil {
		return nil, err
	}
	if param.Limit <= 0 {
		param.Limit = 1
	}
	if param.Start > 30 {
		param.Start -= 30
	}
	if param.From > 30 {
		param.From -= 30
	}

	items, _, err := c.QueryLog(ctx, param)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]string, 0, len(items))
	for _, item := range items {
		row, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		strRow := make(map[string]string, len(row))
		for k, v := range row {
			strRow[k] = fmt.Sprintf("%v", v)
		}
		result = append(result, strRow)
		if param.Limit <= 1 {
			break
		}
	}
	return result, nil
}

func (c *TencentCLS) MakeLogQuery(ctx context.Context, query interface{}, eventTags []string, start, end int64) (interface{}, error) {
	param, err := decodeQuery(query)
	if err != nil {
		return nil, err
	}
	param.Query = appendEventTags(param.Query, eventTags)
	param.Start = start
	param.End = end
	if param.Limit <= 0 {
		param.Limit = 100
	}
	return param, nil
}

func (c *TencentCLS) MakeTSQuery(ctx context.Context, query interface{}, eventTags []string, start, end int64) (interface{}, error) {
	param, err := decodeQuery(query)
	if err != nil {
		return nil, err
	}
	param.Query = appendEventTags(param.Query, eventTags)
	param.Start = start
	param.End = end
	return param, nil
}

func (c *TencentCLS) buildSearchLogRequest(ctx context.Context, param *Query, useNewAnalysis bool) (*clskit.SearchLogRequest, error) {
	from, to := c.resolveTimeRange(ctx, param)
	req := &clskit.SearchLogRequest{
		From:           toMilliseconds(from),
		To:             toMilliseconds(to),
		QueryString:    param.Query,
		QuerySyntax:    1,
		Sort:           param.Sort,
		Limit:          c.resolveLimit(param.Limit),
		UseNewAnalysis: useNewAnalysis,
	}

	topicID := strings.TrimSpace(param.TopicID)
	topics := normalizeTopics(param.Topics)
	if topicID == "" && len(topics) == 0 {
		topicID = strings.TrimSpace(c.TopicID)
	}
	if topicID == "" && len(topics) == 1 {
		topicID = topics[0]
		topics = nil
	}

	if topicID != "" {
		req.TopicID = topicID
	} else if len(topics) > 0 {
		req.Topics = make([]clskit.MultiTopicSearchInformation, 0, len(topics))
		for _, topic := range topics {
			req.Topics = append(req.Topics, clskit.MultiTopicSearchInformation{TopicID: topic})
		}
	} else {
		return nil, fmt.Errorf("topic_id or topics is required")
	}

	if req.From <= 0 || req.To <= 0 {
		return nil, fmt.Errorf("from and to are required")
	}
	if req.From >= req.To {
		return nil, fmt.Errorf("from must be less than to")
	}

	return req, nil
}

func (c *TencentCLS) resolveTimeRange(ctx context.Context, param *Query) (int64, int64) {
	from := firstNonZero(param.Start, param.From)
	to := firstNonZero(param.End, param.To, param.Time)
	if from > 0 && to > 0 {
		return from, to
	}

	delay := int64(0)
	if v, ok := contextDelay(ctx); ok {
		delay = v
	}

	now := time.Now().Unix() - delay
	if to == 0 {
		to = now - param.Offset
	}
	if from == 0 {
		interval := param.Interval
		if interval <= 0 {
			interval = 60
		}
		from = to - interval
	}
	return from, to
}

func (c *TencentCLS) resolveLimit(limit int) int {
	if limit > 0 {
		if limit > 1000 {
			return 1000
		}
		return limit
	}
	if c.MaxQueryRows > 0 {
		if c.MaxQueryRows > 1000 {
			return 1000
		}
		return c.MaxQueryRows
	}
	return clskit.DefaultMaxQueryRows
}

func decodeQuery(query interface{}) (*Query, error) {
	param := new(Query)
	if query == nil {
		return param, nil
	}
	if queryStr, ok := query.(string); ok {
		param.Query = queryStr
		return param, nil
	}
	if err := mapstructure.Decode(query, param); err != nil {
		return nil, fmt.Errorf("decode query param failed: %w", err)
	}
	return param, nil
}

func parseAnalysisRecords(records []string) ([]map[string]interface{}, error) {
	rows := make([]map[string]interface{}, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record) == "" {
			continue
		}
		row := make(map[string]interface{})
		decoder := json.NewDecoder(bytes.NewBufferString(record))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("decode analysis record failed: %w, record=%s", err, record)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func convertRowsToDataResp(rows []map[string]interface{}, keys datasource.Keys, ref, query string, defaultEndMs int64) []models.DataResp {
	valueKeys := strings.Fields(keys.ValueKey)
	labelKeys := strings.Fields(keys.LabelKey)
	timeKey := keys.TimeKey
	if timeKey == "" {
		timeKey = firstExistingKey(rows, "__time__", "time", "_time")
	}

	defaultTS := float64(time.Now().Unix())
	if defaultEndMs > 0 {
		defaultTS = float64(fromMilliseconds(defaultEndMs))
	}

	seriesMap := make(map[string]*models.DataResp)
	for _, row := range rows {
		labels := make(model.Metric)
		for _, labelKey := range labelKeys {
			if v, exists := row[labelKey]; exists && v != nil {
				labels[model.LabelName(labelKey)] = model.LabelValue(fmt.Sprintf("%v", v))
			}
		}

		ts := defaultTS
		if timeKey != "" {
			if t, err := parseUnixSeconds(row[timeKey], keys.TimeFormat); err == nil {
				ts = t
			}
		}

		for _, valueKey := range valueKeys {
			value, err := parseFloat(row[valueKey])
			if err != nil || math.IsNaN(value) {
				continue
			}

			metric := make(model.Metric, len(labels)+1)
			for k, v := range labels {
				metric[k] = v
			}
			metric[model.MetricNameLabel] = model.LabelValue(valueKey)

			key := metric.String()
			if _, exists := seriesMap[key]; !exists {
				seriesMap[key] = &models.DataResp{
					Ref:    ref,
					Metric: metric,
					Query:  query,
					Values: [][]float64{},
				}
			}
			seriesMap[key].Values = append(seriesMap[key].Values, []float64{ts, value})
		}
	}

	result := make([]models.DataResp, 0, len(seriesMap))
	for _, item := range seriesMap {
		sort.Slice(item.Values, func(i, j int) bool {
			return item.Values[i][0] < item.Values[j][0]
		})
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Metric.String() < result[j].Metric.String()
	})
	return result
}

func convertLogs(logs []clskit.LogInfo) []interface{} {
	result := make([]interface{}, 0, len(logs))
	for _, log := range logs {
		row := make(map[string]interface{})
		if strings.TrimSpace(log.LogJSON) != "" {
			decoder := json.NewDecoder(strings.NewReader(log.LogJSON))
			decoder.UseNumber()
			_ = decoder.Decode(&row)
		}
		if len(row) == 0 && log.RawLog != "" {
			row["raw_log"] = log.RawLog
		}
		row["_time"] = log.Time
		row["_topic_id"] = log.TopicID
		row["_topic_name"] = log.TopicName
		row["_source"] = log.Source
		row["_filename"] = log.FileName
		row["_hostname"] = log.HostName
		row["_pkg_id"] = log.PkgID
		row["_pkg_log_id"] = log.PkgLogID
		row["_index_status"] = log.IndexStatus
		result = append(result, row)
	}
	return result
}

func appendEventTags(query string, eventTags []string) string {
	filters := make([]string, 0, len(eventTags))
	for _, tag := range eventTags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			continue
		}
		filters = append(filters, fmt.Sprintf("%s:%s", parts[0], strconv.Quote(parts[1])))
	}
	if len(filters) == 0 {
		return query
	}

	tagFilter := strings.Join(filters, " AND ")
	if strings.TrimSpace(query) == "" || strings.TrimSpace(query) == "*" {
		return tagFilter
	}

	if idx := strings.Index(query, "|"); idx >= 0 {
		filterPart := strings.TrimSpace(query[:idx])
		sqlPart := query[idx:]
		if filterPart == "" || filterPart == "*" {
			return tagFilter + " " + sqlPart
		}
		return filterPart + " AND " + tagFilter + " " + sqlPart
	}

	return query + " AND " + tagFilter
}

func normalizeTopics(topics []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(topics))
	for _, topic := range topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}
		if _, exists := seen[topic]; exists {
			continue
		}
		seen[topic] = struct{}{}
		result = append(result, topic)
	}
	return result
}

func firstExistingKey(rows []map[string]interface{}, candidates ...string) string {
	for _, candidate := range candidates {
		for _, row := range rows {
			if _, exists := row[candidate]; exists {
				return candidate
			}
		}
	}
	return ""
}

func firstNonZero(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func toMilliseconds(ts int64) int64 {
	if ts > 0 && ts < 1_000_000_000_000 {
		return ts * 1000
	}
	return ts
}

func fromMilliseconds(ts int64) int64 {
	if ts >= 1_000_000_000_000 {
		return ts / 1000
	}
	return ts
}

func parseFloat(v interface{}) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("value is nil")
	}

	switch val := v.(type) {
	case json.Number:
		return val.Float64()
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int8:
		return float64(val), nil
	case int16:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case uint:
		return float64(val), nil
	case uint8:
		return float64(val), nil
	case uint16:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		return parseFloat(rv.Elem().Interface())
	}
	return 0, fmt.Errorf("cannot convert %T to float64", v)
}

func parseUnixSeconds(v interface{}, format string) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("time value is nil")
	}

	switch val := v.(type) {
	case json.Number:
		i, err := val.Int64()
		if err == nil {
			return float64(fromMilliseconds(i)), nil
		}
		f, err := val.Float64()
		if err != nil {
			return 0, err
		}
		return normalizeTimestampFloat(f), nil
	case int64:
		return float64(fromMilliseconds(val)), nil
	case int:
		return float64(fromMilliseconds(int64(val))), nil
	case float64:
		return normalizeTimestampFloat(val), nil
	case string:
		return parseTimeString(val, format)
	default:
		return 0, fmt.Errorf("unsupported time type %T", v)
	}
}

func normalizeTimestampFloat(v float64) float64 {
	if v >= 1_000_000_000_000 {
		return math.Floor(v / 1000)
	}
	return v
}

func parseTimeString(s, format string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("time string is empty")
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return float64(fromMilliseconds(i)), nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return normalizeTimestampFloat(f), nil
	}

	if format != "" {
		t, err := time.Parse(format, s)
		if err == nil {
			return float64(t.Unix()), nil
		}
	}

	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		time.DateTime,
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05",
	}
	for _, layout := range formats {
		t, err := time.Parse(layout, s)
		if err == nil {
			return float64(t.Unix()), nil
		}
	}
	return 0, fmt.Errorf("cannot parse time %q", s)
}

func contextDelay(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	switch v := ctx.Value("delay").(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}
