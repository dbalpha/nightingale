package tencentcls

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
)

func TestQueryDataConvertsAnalysisRecords(t *testing.T) {
	var gotBody map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"Response": map[string]interface{}{
				"RequestId": "rid-1",
				"AnalysisRecords": []string{
					`{"status":"403","count":2,"time":1700000000}`,
					`{"status":"404","count":"3","time":1700000060000}`,
				},
			},
		})
	}))
	defer server.Close()

	ds := testDatasource(server.URL)
	data, err := ds.QueryData(context.Background(), map[string]interface{}{
		"ref":      "A",
		"query":    `status:403 OR status:404 | SELECT status,count(*) AS count GROUP BY status`,
		"topic_id": "topic-a",
		"start":    int64(1700000000),
		"end":      int64(1700000060),
		"keys": map[string]interface{}{
			"valueKey": "count",
			"labelKey": "status",
		},
	})
	if err != nil {
		t.Fatalf("QueryData error: %v", err)
	}
	if gotBody["TopicId"] != "topic-a" {
		t.Fatalf("unexpected topic id: %#v", gotBody)
	}
	if gotBody["From"] != float64(1700000000000) || gotBody["To"] != float64(1700000060000) {
		t.Fatalf("unexpected millisecond range: %#v", gotBody)
	}
	if len(data) != 2 {
		t.Fatalf("unexpected data length: %d %#v", len(data), data)
	}

	seen := map[string]float64{}
	for _, item := range data {
		status := string(item.Metric[model.LabelName("status")])
		if item.Ref != "A" || item.Metric[model.MetricNameLabel] != model.LabelValue("count") {
			t.Fatalf("unexpected metric: %#v", item)
		}
		if len(item.Values) != 1 {
			t.Fatalf("unexpected values: %#v", item.Values)
		}
		seen[status] = item.Values[0][1]
	}
	if seen["403"] != 2 || seen["404"] != 3 {
		t.Fatalf("unexpected values: %#v", seen)
	}
}

func TestQueryLogParsesRawAndAnalysisRows(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"Response": map[string]interface{}{
				"RequestId": "rid-2",
				"Results": []map[string]interface{}{
					{
						"Time":      1700000000000,
						"TopicId":   "topic-a",
						"TopicName": "app",
						"LogJson":   `{"level":"error","message":"failed"}`,
					},
				},
			},
		})
	}))
	defer server.Close()

	ds := testDatasource(server.URL)
	items, total, err := ds.QueryLog(context.Background(), map[string]interface{}{
		"query":    "level:error",
		"topic_id": "topic-a",
		"start":    int64(1700000000),
		"end":      int64(1700000060),
	})
	if err != nil {
		t.Fatalf("QueryLog error: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("unexpected log result total=%d items=%#v", total, items)
	}
	row := items[0].(map[string]interface{})
	if row["level"] != "error" || row["_topic_id"] != "topic-a" {
		t.Fatalf("unexpected row: %#v", row)
	}
}

func TestQueryDataValidation(t *testing.T) {
	ds := testDatasource("http://127.0.0.1:1")
	_, err := ds.QueryData(context.Background(), map[string]interface{}{
		"query":    `* | SELECT count(*) AS count`,
		"topic_id": "topic-a",
		"start":    int64(1700000000),
		"end":      int64(1700000060),
	})
	if err == nil || !strings.Contains(err.Error(), "valueKey is required") {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = ds.QueryData(context.Background(), map[string]interface{}{
		"query": `* | SELECT count(*) AS count`,
		"keys":  map[string]interface{}{"valueKey": "count"},
		"start": int64(1700000000),
		"end":   int64(1700000060),
	})
	if err == nil || !strings.Contains(err.Error(), "topic_id or topics is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildSearchLogRequestUsesIntervalAndOffset(t *testing.T) {
	ds := testDatasource("http://127.0.0.1:1")
	ds.TopicID = "default-topic"
	req, err := ds.buildSearchLogRequest(context.Background(), &Query{
		Query:    "* | SELECT count(*) AS count",
		Topics:   []string{"topic-a", "topic-b"},
		Interval: 300,
		Offset:   60,
		Limit:    10,
	}, true)
	if err != nil {
		t.Fatalf("buildSearchLogRequest error: %v", err)
	}
	if req.To <= req.From {
		t.Fatalf("unexpected time range: from=%d to=%d", req.From, req.To)
	}
	if req.To-req.From != 300000 {
		t.Fatalf("unexpected interval: from=%d to=%d", req.From, req.To)
	}
	if req.TopicID != "" || len(req.Topics) != 2 {
		t.Fatalf("query topics should override datasource default topic: %#v", req)
	}
}

func TestValidateRequiresCredentials(t *testing.T) {
	ds := &TencentCLS{}
	err := ds.Validate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "secret_id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testDatasource(endpoint string) *TencentCLS {
	ds := &TencentCLS{}
	plug, err := ds.Init(map[string]interface{}{
		"tencent-cls.secret_id":      "sid",
		"tencent-cls.secret_key":     "skey",
		"tencent-cls.region":         "ap-guangzhou",
		"tencent-cls.endpoint":       endpoint,
		"tencent-cls.timeout":        int64(1000),
		"tencent-cls.max_query_rows": 500,
	})
	if err != nil {
		panic(err)
	}
	ret := plug.(*TencentCLS)
	if err := ret.Validate(context.Background()); err != nil {
		panic(err)
	}
	if err := ret.InitClient(); err != nil {
		panic(err)
	}
	return ret
}
