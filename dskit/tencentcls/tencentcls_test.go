package tencentcls

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSearchLogBuildsSignedRequest(t *testing.T) {
	var gotBody map[string]interface{}
	var gotHeader http.Header
	var gotHost string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Clone()
		gotHost = r.Host
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"Response": map[string]interface{}{
				"RequestId":       "rid-1",
				"AnalysisRecords": []string{`{"count":1}`},
			},
		})
	}))
	defer server.Close()

	client := &TencentCLS{
		SecretID:  "sid",
		SecretKey: "skey",
		Region:    "ap-guangzhou",
		Endpoint:  server.URL,
		Timeout:   1000,
	}

	resp, err := client.SearchLog(context.Background(), &SearchLogRequest{
		From:           1700000000000,
		To:             1700000060000,
		QueryString:    `error | SELECT count(*) AS count`,
		QuerySyntax:    1,
		TopicID:        "topic-a",
		Limit:          10,
		UseNewAnalysis: true,
	})
	if err != nil {
		t.Fatalf("SearchLog error: %v", err)
	}
	if resp.RequestID != "rid-1" {
		t.Fatalf("unexpected request id: %s", resp.RequestID)
	}

	if gotHost == "" || !strings.Contains(gotHeader.Get("Authorization"), "TC3-HMAC-SHA256 Credential=sid/") {
		t.Fatalf("authorization header was not set correctly: host=%q auth=%q", gotHost, gotHeader.Get("Authorization"))
	}
	if gotHeader.Get("X-TC-Action") != searchLog {
		t.Fatalf("unexpected action header: %q", gotHeader.Get("X-TC-Action"))
	}
	if gotHeader.Get("X-TC-Version") != apiVersion {
		t.Fatalf("unexpected version header: %q", gotHeader.Get("X-TC-Version"))
	}
	if gotHeader.Get("X-TC-Region") != "ap-guangzhou" {
		t.Fatalf("unexpected region header: %q", gotHeader.Get("X-TC-Region"))
	}
	if gotBody["TopicId"] != "topic-a" {
		t.Fatalf("unexpected topic id: %#v", gotBody["TopicId"])
	}
	if gotBody["From"] != float64(1700000000000) || gotBody["To"] != float64(1700000060000) {
		t.Fatalf("unexpected time range: %#v", gotBody)
	}
	if gotBody["UseNewAnalysis"] != true || gotBody["QuerySyntax"] != float64(1) {
		t.Fatalf("unexpected analysis request body: %#v", gotBody)
	}
}

func TestSearchLogUsesTopicsForMultiTopicRequest(t *testing.T) {
	var gotBody map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"Response": map[string]interface{}{"RequestId": "rid-2"},
		})
	}))
	defer server.Close()

	client := &TencentCLS{
		SecretID:  "sid",
		SecretKey: "skey",
		Region:    "ap-guangzhou",
		Endpoint:  server.URL,
	}

	_, err := client.SearchLog(context.Background(), &SearchLogRequest{
		From:        1700000000000,
		To:          1700000060000,
		QueryString: "*",
		Topics: []MultiTopicSearchInformation{
			{TopicID: "topic-a"},
			{TopicID: "topic-b"},
		},
	})
	if err != nil {
		t.Fatalf("SearchLog error: %v", err)
	}
	if _, exists := gotBody["TopicId"]; exists {
		t.Fatalf("TopicId must not be sent with Topics: %#v", gotBody)
	}
	topics, ok := gotBody["Topics"].([]interface{})
	if !ok || len(topics) != 2 {
		t.Fatalf("unexpected Topics payload: %#v", gotBody["Topics"])
	}
}

func TestSearchLogPropagatesTencentError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"Response": map[string]interface{}{
				"Error": map[string]string{
					"Code":    "FailedOperation.QueryError",
					"Message": "syntax error",
				},
				"RequestId": "rid-error",
			},
		})
	}))
	defer server.Close()

	client := &TencentCLS{
		SecretID:  "sid",
		SecretKey: "skey",
		Region:    "ap-guangzhou",
		Endpoint:  server.URL,
	}

	_, err := client.SearchLog(context.Background(), &SearchLogRequest{
		From:      1700000000000,
		To:        1700000060000,
		TopicID:   "topic-a",
		Limit:     10,
		HighLight: true,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "FailedOperation.QueryError") || !strings.Contains(err.Error(), "rid-error") {
		t.Fatalf("unexpected error: %v", err)
	}
}
