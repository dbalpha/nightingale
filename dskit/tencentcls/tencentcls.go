package tencentcls

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultEndpoint     = "cls.tencentcloudapi.com"
	DefaultTimeout      = int64(10000)
	DefaultMaxQueryRows = 1000

	service     = "cls"
	apiVersion  = "2020-10-16"
	searchLog   = "SearchLog"
	contentType = "application/json; charset=utf-8"
)

type TencentCLS struct {
	SecretID     string       `json:"tencent-cls.secret_id" mapstructure:"tencent-cls.secret_id"`
	SecretKey    string       `json:"tencent-cls.secret_key" mapstructure:"tencent-cls.secret_key"`
	Region       string       `json:"tencent-cls.region" mapstructure:"tencent-cls.region"`
	Endpoint     string       `json:"tencent-cls.endpoint" mapstructure:"tencent-cls.endpoint"`
	TopicID      string       `json:"tencent-cls.topic_id" mapstructure:"tencent-cls.topic_id"`
	Timeout      int64        `json:"tencent-cls.timeout" mapstructure:"tencent-cls.timeout"` // millis
	ClusterName  string       `json:"tencent-cls.cluster_name" mapstructure:"tencent-cls.cluster_name"`
	MaxQueryRows int          `json:"tencent-cls.max_query_rows" mapstructure:"tencent-cls.max_query_rows"`
	HTTPClient   *http.Client `json:"-" mapstructure:"-"`
}

type MultiTopicSearchInformation struct {
	TopicID string `json:"TopicId"`
	Context string `json:"Context,omitempty"`
}

type SearchLogRequest struct {
	From           int64                         `json:"From"`
	To             int64                         `json:"To"`
	QueryString    string                        `json:"QueryString,omitempty"`
	QuerySyntax    int                           `json:"QuerySyntax,omitempty"`
	TopicID        string                        `json:"TopicId,omitempty"`
	Topics         []MultiTopicSearchInformation `json:"Topics,omitempty"`
	Sort           string                        `json:"Sort,omitempty"`
	Limit          int                           `json:"Limit,omitempty"`
	Offset         int                           `json:"Offset,omitempty"`
	Context        string                        `json:"Context,omitempty"`
	SamplingRate   *float64                      `json:"SamplingRate,omitempty"`
	UseNewAnalysis bool                          `json:"UseNewAnalysis,omitempty"`
	HighLight      bool                          `json:"HighLight,omitempty"`
}

type SearchLogResponse struct {
	Context         string           `json:"Context"`
	ListOver        bool             `json:"ListOver"`
	Analysis        bool             `json:"Analysis"`
	Results         []LogInfo        `json:"Results"`
	ColNames        []string         `json:"ColNames"`
	AnalysisResults []LogItems       `json:"AnalysisResults"`
	AnalysisRecords []string         `json:"AnalysisRecords"`
	Columns         []Column         `json:"Columns"`
	SamplingRate    float64          `json:"SamplingRate"`
	LogCount        int64            `json:"LogCount"`
	Topics          json.RawMessage  `json:"Topics"`
	RequestID       string           `json:"RequestId"`
	Error           *TencentAPIError `json:"Error"`
}

type LogInfo struct {
	Time        int64    `json:"Time"`
	TopicID     string   `json:"TopicId"`
	TopicName   string   `json:"TopicName"`
	Source      string   `json:"Source"`
	FileName    string   `json:"FileName"`
	HostName    string   `json:"HostName"`
	PkgID       string   `json:"PkgId"`
	PkgLogID    string   `json:"PkgLogId"`
	HighLights  []string `json:"HighLights"`
	LogJSON     string   `json:"LogJson"`
	RawLog      string   `json:"RawLog"`
	IndexStatus string   `json:"IndexStatus"`
}

type LogItems struct {
	Data []LogItem `json:"Data"`
}

type LogItem struct {
	Key   string      `json:"Key"`
	Value interface{} `json:"Value"`
}

type Column struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
}

type TencentAPIError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

func (e *TencentAPIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type apiResponse struct {
	Response SearchLogResponse `json:"Response"`
}

func (c *TencentCLS) InitHTTPClient() error {
	timeout := time.Duration(c.Timeout) * time.Millisecond
	if timeout == 0 {
		timeout = time.Duration(DefaultTimeout) * time.Millisecond
	}

	c.HTTPClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout: timeout,
			}).DialContext,
			ResponseHeaderTimeout: timeout,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
		},
		Timeout: timeout,
	}

	return nil
}

func (c *TencentCLS) SearchLog(ctx context.Context, reqParam *SearchLogRequest) (*SearchLogResponse, error) {
	if c.HTTPClient == nil {
		if err := c.InitHTTPClient(); err != nil {
			return nil, err
		}
	}

	if reqParam == nil {
		return nil, fmt.Errorf("search log request is nil")
	}

	body, err := json.Marshal(reqParam)
	if err != nil {
		return nil, fmt.Errorf("marshal search log request failed: %w", err)
	}

	endpoint, host, err := c.endpoint()
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create search log request failed: %w", err)
	}

	timestamp := time.Now().Unix()
	httpReq.Host = host
	httpReq.Header.Set("Content-Type", contentType)
	httpReq.Header.Set("Host", host)
	httpReq.Header.Set("X-TC-Action", searchLog)
	httpReq.Header.Set("X-TC-Version", apiVersion)
	httpReq.Header.Set("X-TC-Timestamp", fmt.Sprintf("%d", timestamp))
	httpReq.Header.Set("X-TC-Region", c.Region)
	httpReq.Header.Set("Authorization", c.signature(string(body), host, searchLog, timestamp))

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("search log request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read search log response failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("search log failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var wrapped apiResponse
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	decoder.UseNumber()
	if err := decoder.Decode(&wrapped); err != nil {
		return nil, fmt.Errorf("decode search log response failed: %w, body=%s", err, string(respBody))
	}

	if wrapped.Response.Error != nil {
		if wrapped.Response.RequestID != "" {
			return nil, fmt.Errorf("%s, request_id=%s", wrapped.Response.Error.Error(), wrapped.Response.RequestID)
		}
		return nil, wrapped.Response.Error
	}

	return &wrapped.Response, nil
}

func (c *TencentCLS) endpoint() (string, string, error) {
	raw := strings.TrimSpace(c.Endpoint)
	if raw == "" {
		raw = DefaultEndpoint
	}

	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("invalid tencent cls endpoint: %w", err)
	}
	if parsed.Host == "" {
		return "", "", fmt.Errorf("invalid tencent cls endpoint: host is empty")
	}

	parsed.Path = "/"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), parsed.Host, nil
}

func (c *TencentCLS) signature(payload, host, action string, timestamp int64) string {
	canonicalHeaders := fmt.Sprintf("content-type:%s\nhost:%s\nx-tc-action:%s\n", contentType, strings.ToLower(host), strings.ToLower(action))
	hashedPayload := sha256Hex(payload)
	canonicalRequest := strings.Join([]string{
		http.MethodPost,
		"/",
		"",
		canonicalHeaders,
		"content-type;host;x-tc-action",
		hashedPayload,
	}, "\n")

	date := time.Unix(timestamp, 0).UTC().Format("2006-01-02")
	credentialScope := fmt.Sprintf("%s/%s/tc3_request", date, service)
	stringToSign := strings.Join([]string{
		"TC3-HMAC-SHA256",
		fmt.Sprintf("%d", timestamp),
		credentialScope,
		sha256Hex(canonicalRequest),
	}, "\n")

	secretDate := hmacSHA256([]byte("TC3"+c.SecretKey), date)
	secretService := hmacSHA256(secretDate, service)
	secretSigning := hmacSHA256(secretService, "tc3_request")
	signature := hex.EncodeToString(hmacSHA256(secretSigning, stringToSign))

	return fmt.Sprintf("TC3-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.SecretID, credentialScope, "content-type;host;x-tc-action", signature)
}

func sha256Hex(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func hmacSHA256(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}
