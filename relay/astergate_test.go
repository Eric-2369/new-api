package relay

import (
	"context"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	perfmetrics "github.com/QuantumNous/new-api/pkg/perf_metrics"
	"github.com/QuantumNous/new-api/relay/channel/astergate"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsterGateRawForwarding(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	request := "{ \"model\":\"claude/test[1m]\", \"messages\":[{\"role\":\"user\",\"content\":\"hi\"}], \"unknown\": {\"zero\":0}, \"thinking\":\"future-protocol\" }"
	for _, tc := range []struct {
		name, contentType, body string
		status                  int
		wantError               bool
	}{
		{"json", "application/json", `{"content":[],"unknown":false,"usage":{"input_tokens":10,"output_tokens":2}}`, 200, false},
		{"sse", "text/event-stream", ": keepalive\r\nevent: extra\r\ndata: {\"future\":true}\r\n\r\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", 200, false},
		{"error", "application/json", `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`, 429, true},
		{"stream_error", "text/event-stream", "event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"busy\"}}\n\n", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/prefix/v1/messages?beta=true", r.URL.RequestURI())
				assert.Equal(t, "Bearer upstream-key", r.Header.Get("Authorization"))
				for _, name := range []string{"X-Api-Key", "Cookie", "X-Forwarded-For", "X-Astergate-Peer-Id", "New-Api-User", "X-Remove"} {
					assert.Empty(t, r.Header.Get(name), name)
				}
				assert.Equal(t, "claude-code/test", r.Header.Get("User-Agent"))
				assert.Equal(t, "cli-bg", r.Header.Get("X-App"))
				assert.Equal(t, "test-beta", r.Header.Get("Anthropic-Beta"))
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.Equal(t, request, string(body))
				w.Header().Set("Content-Type", tc.contentType)
				w.Header().Set("Retry-After", "15")
				w.Header().Set("Set-Cookie", "upstream-secret")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", strings.NewReader(request))
			c.Request.Header = http.Header{"Content-Type": {"application/json"}, "Authorization": {"Bearer local-key"}, "X-Api-Key": {"local-key"}, "Cookie": {"session=local"}, "X-Forwarded-For": {"spoof"}, "X-Astergate-Peer-Id": {"spoof"}, "New-Api-User": {"1"}, "Connection": {"X-Remove"}, "X-Remove": {"secret"}, "User-Agent": {"claude-code/test"}, "X-App": {"cli-bg"}, "Anthropic-Beta": {"test-beta"}}
			t.Cleanup(func() { common.CleanupBodyStorage(c) })
			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelBaseUrl: server.URL + "/prefix/v1", ApiKey: "upstream-key"}}
			apiErr := forwardAsterGate(c, info, "messages", false)
			if tc.wantError {
				require.NotNil(t, apiErr)
			} else {
				require.Nil(t, apiErr)
			}
			assert.Equal(t, tc.status, recorder.Code)
			assert.Equal(t, tc.body, recorder.Body.String())
			assert.Empty(t, recorder.Header().Get("Set-Cookie"))
			assert.Equal(t, "15", recorder.Header().Get("Retry-After"))
		})
	}
}

func TestAsterGateUsageObservation(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	observer := &astergate.Observer{Stream: true}
	// Split writes inside JSON and SSE framing; deltas are cumulative, not additive.
	for _, part := range []string{
		"event: message_start\r\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":20,\"cache_creation_input_tokens\":8,\"cache_creation\":{\"ephemeral_5m_input_tokens\":3,\"ephemeral_1h_input_tokens\":5}}}}\r",
		"\n\r\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":4}}\n\ndata: {\"type\":\"message_delta\",\n",
		"data: \"usage\":{\"output_tokens\":6,\"server_tool_use\":{\"web_search_requests\":2}}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
	} {
		observer.Write([]byte(part))
	}
	usage := observer.Usage()
	require.NotNil(t, usage)
	assert.Equal(t, 10, usage.PromptTokens)
	assert.Equal(t, 6, usage.CompletionTokens)
	assert.Equal(t, 20, usage.PromptTokensDetails.CachedTokens)
	assert.Equal(t, 8, usage.PromptTokensDetails.CachedCreationTokens)
	assert.Equal(t, 3, usage.ClaudeCacheCreation5mTokens)
	assert.Equal(t, 5, usage.ClaudeCacheCreation1hTokens)
	require.NotNil(t, usage.BillingUsage)
	assert.Equal(t, "anthropic", usage.BillingUsage.Semantic)
	assert.Equal(t, 2, observer.ToolCounts["web_search"])
	assert.True(t, observer.Done)
	for _, value := range []string{"-1", "1.5", "2147483648", "18446744073709551615", `"10"`} {
		t.Run(value, func(t *testing.T) {
			invalid := &astergate.Observer{}
			invalid.Write([]byte(`{"usage":{"input_tokens":` + value + `}}`))
			assert.Nil(t, invalid.Usage())
		})
	}
	empty := &astergate.Observer{}
	empty.Write([]byte(`{"usage":{"billing_usage":{"source":"oai_chat"}}}`))
	assert.Nil(t, empty.Usage())
}

func TestAsterGateRedirectAndCancellation(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect must not be followed") }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	resp, err := astergate.Request(context.Background(), server.Client(), server.URL, "messages", "", "key", nil, strings.NewReader("{}"))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusTemporaryRedirect, resp.StatusCode)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = astergate.Request(ctx, server.Client(), server.URL, "messages", "", "key", nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAsterGateReport(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"bound", `{"protocol":"astergate-usage","protocol_version":1,"scope":"bound_account","status":"available","five_hour":{"used_percent":25.5,"window_seconds":18000},"weekly":null,"secret":"not exposed"}`, true},
		{"unavailable", `{"protocol":"astergate-usage","protocol_version":1,"scope":"bound_account","status":"unavailable","reason":"key_not_bound_to_single_account","five_hour":null,"weekly":null}`, true},
		{"unauthorized", `{"protocol":"astergate-usage","protocol_version":1,"scope":"bound_account","status":"unavailable","reason":"account_not_authorized"}`, true},
		{"wrong_protocol", `{"protocol":"other","protocol_version":1}`, false},
		{"invalid_percent", `{"protocol":"astergate-usage","protocol_version":1,"scope":"bound_account","status":"available","five_hour":{"used_percent":101,"window_seconds":18000}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/v1/usage", r.URL.Path)
				assert.Equal(t, "1", r.Header.Get("X-Astergate-Usage-Protocol"))
				assert.Equal(t, "Bearer key", r.Header.Get("Authorization"))
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			report, err := astergate.FetchReport(context.Background(), server.Client(), server.URL, "key")
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.name == "unauthorized" {
				assert.Equal(t, "account_not_authorized", report.Reason)
			}
			encoded, err := common.Marshal(report)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), "secret")
			if tc.name == "unavailable" {
				assert.Nil(t, report.FiveHour)
				assert.Nil(t, report.Weekly)
			}
		})
	}
}

func TestAsterGateValidationPreservesOpaqueFields(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	require.NoError(t, i18n.Init())
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"future_option", `{"model":"claude/test","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"thinking":"future-protocol"}`, true},
		{"overflow", `{"model":"claude/test","messages":[{"role":"user","content":"hi"}],"max_tokens":18446744073709551615}`, false},
		{"negative", `{"model":"claude/test","messages":[{"role":"user","content":"hi"}],"max_tokens":-1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", strings.NewReader(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")
			common.SetContextKey(c, constant.ContextKeyChannelType, constant.ChannelTypeAsterGate)
			t.Cleanup(func() { common.CleanupBodyStorage(c) })
			_, err := helper.GetAndValidateRequest(c, types.RelayFormatClaude)
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			storage, err := common.GetBodyStorage(c)
			require.NoError(t, err)
			raw, err := io.ReadAll(common.NewReplayableBodyReader(storage))
			require.NoError(t, err)
			assert.Equal(t, tc.body, string(raw))
			common.SetContextKey(c, constant.ContextKeyChannelType, constant.ChannelTypeAnthropic)
			_, err = helper.GetAndValidateRequest(c, types.RelayFormatClaude)
			assert.Error(t, err, "ordinary Claude channels retain their typed validation")
		})
	}
}

func TestAsterGateOversizedEventDoesNotHideLaterUsage(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	observer := &astergate.Observer{Stream: true}
	observer.Write([]byte("data: " + strings.Repeat("x", 8<<20) + "\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":7}}\n\n"))
	usage := observer.Usage()
	require.NotNil(t, usage)
	assert.Equal(t, 7, usage.CompletionTokens)
}

func TestAsterGateUsageSaturationIsAuditable(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	observer := &astergate.Observer{}
	observer.Write([]byte(`{"usage":{"input_tokens":2147483647,"output_tokens":2}}`))
	usage := observer.Usage()
	require.NotNil(t, usage)
	assert.Equal(t, 2147483647, usage.TotalTokens)
	require.NotNil(t, observer.QuotaClamp)
}

func TestAsterGateInterruptedBillingAndFirstContent(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	o := &astergate.Observer{Stream: true, CountTokens: func(text string) int { return len(strings.Fields(text)) }}
	o.Write([]byte(": ping\n\ndata: {\"type\":\"ping\"}\n\n"))
	assert.False(t, o.HasContent)
	assert.Nil(t, o.BillingUsage(100))
	o.Write([]byte(`data: {"type":"message_start","message":{"usage":{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":20}}}` + "\n\n"))
	assert.False(t, o.HasContent)
	o.Write([]byte(`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"one two three"}}` + "\n\n"))
	assert.True(t, o.HasContent)
	usage := o.BillingUsage(100)
	require.NotNil(t, usage)
	assert.Equal(t, 3, usage.CompletionTokens)
	require.NotNil(t, usage.BillingUsage)
	assert.Equal(t, 3, usage.BillingUsage.ClaudeUsage.OutputTokens)
	assert.True(t, usage.BillingUsage.Estimated)
	assert.Equal(t, 10, usage.PromptTokens)
	assert.Equal(t, 20, usage.PromptTokensDetails.CachedTokens)
	assert.Equal(t, usage, o.BillingUsage(100), "finalization must not double count")
	o.Write([]byte(`data: {"type":"message_delta","usage":{"output_tokens":2}}` + "\n\ndata: {\"type\":\"message_stop\"}\n\n"))
	assert.Equal(t, 2, o.BillingUsage(100).CompletionTokens, "completed upstream usage is authoritative")
}

func TestAsterGateEarlyFailureDoesNotSettle(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	body := ": ping\n\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\"}}\n\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"claude-test"}`))
	t.Cleanup(func() { common.CleanupBodyStorage(c) })
	info := relaycommon.GenRelayInfoClaude(c, &dto.ClaudeRequest{Model: "claude-test"})
	info.ChannelMeta = &relaycommon.ChannelMeta{ChannelBaseUrl: server.URL, ApiKey: "key"}
	info.SetEstimatePromptTokens(100)
	// No database or billing session: reaching settlement would fail this fixture.
	require.NotNil(t, forwardAsterGate(c, info, "messages", true))
	assert.False(t, info.HasSendResponse(), "heartbeats and errors are not first content")
}

func TestAsterGateHTTPBaseURL(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, base := range []string{"http://160.191.8.131:8080", "http://160.191.8.131:8080/v1", "http://160.191.8.131:8080/v1/"} {
		for _, path := range []string{"messages", "messages/count_tokens", "usage"} {
			endpoint, err := astergate.Endpoint(base, path)
			require.NoError(t, err)
			assert.Equal(t, "http://160.191.8.131:8080/v1/"+path, endpoint)
		}
	}
}

func TestAsterGateUpstreamStreamContracts(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, tc := range []struct {
		name, body string
		success    bool
	}{
		{"terminal_at_eof", "event: message_stop\ndata: {\"type\":\"message_stop\"}\n", true},
		{"terminal_by_event_name", "event: message_stop\ndata: {}\n\n", true},
		{"business_rejection", "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"prompt too long\"}}\n\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"claude-test"}`))
			t.Cleanup(func() { common.CleanupBodyStorage(c) })
			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelBaseUrl: server.URL, ApiKey: "key"}}
			apiErr := forwardAsterGate(c, info, "messages", false)
			if tc.success {
				assert.Nil(t, apiErr, "upstream recognizes this as a successful terminal")
			} else {
				require.NotNil(t, apiErr)
				assert.Equal(t, perfmetrics.OutcomeIgnored, perfmetrics.ClassifyRelayOutcome(c.Request.Context(), info, apiErr))
			}
		})
	}
}

func TestAsterGateEOFFinalization(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, cleanEOF := range []bool{false, true} {
		observer := &astergate.Observer{Stream: true}
		observer.Write([]byte("event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":10}}}\n\nevent: message_delta\ndata: {\"usage\":{\"output_tokens\":7}}"))
		observer.Finalize(cleanEOF)
		observer.Finalize(cleanEOF)
		usage := observer.Usage()
		require.NotNil(t, usage)
		assert.Equal(t, 10, usage.PromptTokens)
		expected := 0
		if cleanEOF {
			expected = 7
		}
		assert.Equal(t, expected, usage.CompletionTokens)
	}
	for _, tc := range []struct {
		kind   string
		status int
	}{
		{"invalid_request_error", 400}, {"rate_limit_error", 429}, {"authentication_error", 401}, {"permission_error", 403}, {"overloaded_error", 529}, {"unknown_secret", 502},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			observer := &astergate.Observer{Stream: true}
			observer.Write([]byte("event: error\ndata: {\"error\":{\"type\":\"" + tc.kind + "\",\"message\":\"secret\"}}\n\n"))
			require.True(t, observer.Failed)
			assert.Equal(t, tc.status, observer.ErrorStatus)
			assert.NotContains(t, observer.ErrorType, "secret")
		})
	}
}

func TestAsterGateUnaryBusinessError(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, tc := range []struct {
		status  int
		kind    string
		outcome perfmetrics.Outcome
	}{
		{400, "invalid_request_error", perfmetrics.OutcomeIgnored},
		{401, "authentication_error", perfmetrics.OutcomeFailure},
		{429, "rate_limit_error", perfmetrics.OutcomeFailure},
		{500, "api_error", perfmetrics.OutcomeFailure},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			body := `{"type":"error","error":{"type":"` + tc.kind + `","message":"upstream detail"}}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"claude-test","stream":false}`))
			t.Cleanup(func() { common.CleanupBodyStorage(c) })
			info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelBaseUrl: server.URL, ApiKey: "key"}}
			apiErr := forwardAsterGate(c, info, "messages", false)
			require.NotNil(t, apiErr)
			assert.Equal(t, tc.status, apiErr.StatusCode)
			assert.Equal(t, body, recorder.Body.String())
			assert.Equal(t, tc.outcome, perfmetrics.ClassifyRelayOutcome(c.Request.Context(), info, apiErr))
		})
	}
}

func TestAsterGateLargeUnaryUsage(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	body := `{"type":"message","content":[{"type":"text","text":"` + strings.Repeat("x", 5<<20) + `"}],"usage":{"input_tokens":10,"output_tokens":2000,"cache_read_input_tokens":100}}`
	o := &astergate.Observer{}
	for len(body) > 0 {
		n := min(len(body), 32*1024)
		o.Write([]byte(body[:n]))
		body = body[n:]
	}
	require.NoError(t, o.Err())
	usage := o.BillingUsage(50)
	require.NotNil(t, usage, "valid response within upstream's limit must retain its usage")
	assert.Equal(t, 2000, usage.CompletionTokens)
	assert.Equal(t, 100, usage.PromptTokensDetails.CachedTokens)
}

func TestAsterGateUnaryObservationLimitIsExplicit(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	observer := &astergate.Observer{}
	observer.Write(make([]byte, (64<<20)+1))
	require.ErrorContains(t, observer.Err(), "64 MiB")
	observer.Write([]byte(`{"usage":{"output_tokens":1}}`))
	require.Error(t, observer.Err(), "later bytes must not hide an exceeded limit")
}

func TestAsterGateThinkingPerformance(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, tc := range []struct {
		name, thinking string
		opaque         bool
		wantThinking   int
		wantMS         int64
		wantTokens     int64
	}{
		{"hidden_thinking", "3278", false, 3278, 38157, 3513},
		{"opaque_without_breakdown", "0", true, 0, 38157, 3513},
		{"ordinary_stream", "0", false, 0, 38157, 3513},
		{"invalid_optional_metadata", "-1", false, 0, 38157, 3513},
		{"overflow_optional_metadata", "2147483648", false, 0, 38157, 3513},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &astergate.Observer{Stream: true}
			o.Write([]byte(`data: {"type":"message_start","message":{"usage":{"input_tokens":2,"cache_read_input_tokens":172246,"cache_creation_input_tokens":3330}}}` + "\n\n"))
			if tc.opaque {
				o.Write([]byte(`data: {"type":"content_block_delta","delta":{"type":"signature_delta","signature":"opaque"}}` + "\n\n"))
			}
			o.Write([]byte(`data: {"type":"message_delta","usage":{"output_tokens":3513,"output_tokens_details":{"thinking_tokens":` + tc.thinking + `}}}` + "\n\n" + `data: {"type":"message_stop"}` + "\n\n"))
			u := o.BillingUsage(100)
			require.NotNil(t, u)
			assert.Equal(t, 2, u.PromptTokens)
			assert.Equal(t, 3513, u.CompletionTokens)
			assert.Equal(t, 172246, u.PromptTokensDetails.CachedTokens)
			assert.Equal(t, 3330, u.PromptTokensDetails.CachedCreationTokens)
			assert.Equal(t, tc.wantThinking, o.ThinkingTokens)
			start := time.Unix(1700000000, 0)
			info := &relaycommon.RelayInfo{StartTime: start, FirstResponseTime: start.Add(2000 * time.Millisecond), IsStream: true}
			info.SetAsterGatePerformance(start.Add(40157*time.Millisecond), u.CompletionTokens, o.ThinkingTokens)
			require.NotNil(t, info.AsterGatePerformance)
			assert.Equal(t, tc.wantMS, info.AsterGatePerformance.GenerationMS)
			assert.Equal(t, tc.wantTokens, info.AsterGatePerformance.OutputTokens)
			assert.Equal(t, "generation", info.AsterGatePerformance.Basis)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			other := model.NewLogOther()
			service.AppendClientTransportLogInfo(c, info, other)
			extensions := other.Snapshot()["fork_extensions"].(map[string]any)
			assert.Equal(t, "http", extensions["client_transport"])
			assert.Equal(t, info.AsterGatePerformance, extensions["astergate_performance"])
		})
	}
}

func TestAsterGateEmptyBlockStartsGenerationWithoutInventingUsage(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	for _, kind := range []string{"thinking", "text", "tool_use", "redacted_thinking"} {
		t.Run(kind, func(t *testing.T) {
			o := &astergate.Observer{Stream: true}
			o.Write([]byte(`data: {"type":"ping"}` + "\n\n" + `data: {"type":"message_start","message":{}}` + "\n\n"))
			assert.False(t, o.GenerationStarted)
			o.Write([]byte(`event: content_block_start` + "\n" + `data: {"content_block":{"type":"` + kind + `","thinking":"","text":""}}` + "\n\n"))
			assert.True(t, o.GenerationStarted)
			if kind != "tool_use" {
				assert.Nil(t, o.BillingUsage(100), "an empty block must not create estimated billable usage")
			}
		})
	}
	start := time.Unix(1700000000, 0)
	for _, stream := range []bool{false, true} {
		info := &relaycommon.RelayInfo{StartTime: start, IsStream: stream}
		info.SetAsterGatePerformance(start.Add(time.Second), 123, 50)
		require.NotNil(t, info.AsterGatePerformance)
		assert.Zero(t, info.AsterGatePerformance.GenerationMS, "unknown generation start must not become request throughput")
		assert.EqualValues(t, 123, info.AsterGatePerformance.OutputTokens)
	}
}

type asterGateTimingWriter struct {
	*httptest.ResponseRecorder
	onWrite func()
}

func (w *asterGateTimingWriter) Write(p []byte) (int, error) {
	w.onWrite()
	return w.ResponseRecorder.Write(p)
}

func TestAsterGateThinkingStartIsTimedBeforeSignature(t *testing.T) {
	t.Setenv("ENABLE_ASTERGATE", "true")
	start := `data: {"type":"content_block_start","content_block":{"type":"thinking","thinking":"","signature":""}}` + "\n\n"
	end := `data: {"type":"content_block_delta","delta":{"type":"signature_delta","signature":"opaque"}}` + "\n\n" + `data: {"type":"message_stop"}` + "\n\n"
	received := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, start)
		w.(http.Flusher).Flush()
		select {
		case <-received:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, end)
	}))
	defer server.Close()
	var info *relaycommon.RelayInfo
	var first time.Time
	writer := &asterGateTimingWriter{ResponseRecorder: httptest.NewRecorder(), onWrite: func() {
		if first.IsZero() {
			assert.True(t, info.HasSendResponse(), "empty thinking block must set the start before its signature arrives")
			first = info.FirstResponseTime
			close(received)
		}
	}}
	c, _ := gin.CreateTestContext(writer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.Request = httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"claude-test"}`)).WithContext(ctx)
	t.Cleanup(func() { common.CleanupBodyStorage(c) })
	info = relaycommon.GenRelayInfoClaude(c, &dto.ClaudeRequest{Model: "claude-test"})
	info.ChannelMeta = &relaycommon.ChannelMeta{ChannelBaseUrl: server.URL, ApiKey: "key"}
	require.Nil(t, forwardAsterGate(c, info, "messages", false))
	assert.Equal(t, first, info.FirstResponseTime)
	assert.Equal(t, start+end, writer.Body.String())
}
