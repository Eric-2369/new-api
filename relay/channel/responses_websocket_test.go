package channel_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relay/channel"
	"github.com/QuantumNous/new-api/relay/channel/codex"
	"github.com/QuantumNous/new-api/relay/channel/openai"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoResponsesWebSocketRequestSupportsOpenAIAndCodex(t *testing.T) {
	tests := []struct {
		name          string
		channelType   int
		apiKey        string
		adaptor       channel.Adaptor
		wantPath      string
		wantAuth      string
		wantAccountID string
	}{
		{
			name:        "openai",
			channelType: constant.ChannelTypeOpenAI,
			apiKey:      "openai-key",
			adaptor:     &openai.Adaptor{},
			wantPath:    "/v1/responses",
			wantAuth:    "Bearer openai-key",
		},
		{
			name:          "codex",
			channelType:   constant.ChannelTypeCodex,
			apiKey:        `{"access_token":"codex-token","account_id":"account-1"}`,
			adaptor:       &codex.Adaptor{},
			wantPath:      "/backend-api/codex/responses",
			wantAuth:      "Bearer codex-token",
			wantAccountID: "account-1",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			upgrader := websocket.Upgrader{EnableCompression: true, CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.Clone(r.Context())
				conn, err := upgrader.Upgrade(w, r, nil)
				if err == nil {
					_ = conn.Close()
				}
			}))
			defer server.Close()

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
			info := &relaycommon.RelayInfo{
				RelayMode:      relayconstant.RelayModeResponses,
				RequestURLPath: "/v1/responses",
				IsStream:       true,
				ChannelMeta: &relaycommon.ChannelMeta{
					ChannelType:    testCase.channelType,
					ChannelBaseUrl: server.URL,
					ApiKey:         testCase.apiKey,
				},
			}
			testCase.adaptor.Init(info)

			conn, response, err := channel.DoResponsesWebSocketRequest(testCase.adaptor, ctx, info)
			require.NoError(t, err)
			require.NotNil(t, conn)
			require.NotNil(t, response)
			assert.Equal(t, http.StatusSwitchingProtocols, response.StatusCode)
			assert.Contains(t, response.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")
			_ = conn.Close()

			request := <-requests
			assert.Equal(t, testCase.wantPath, request.URL.Path)
			assert.Equal(t, testCase.wantAuth, request.Header.Get("Authorization"))
			assert.Equal(t, testCase.wantAccountID, request.Header.Get("ChatGPT-Account-ID"))
			assert.Equal(t, "responses_websockets=2026-02-06", request.Header.Get("OpenAI-Beta"))
			if testCase.channelType == constant.ChannelTypeCodex {
				assert.Equal(t, "codex_cli_rs", request.Header.Get("Originator"))
			}
		})
	}
}

func TestDoResponsesWebSocketRequestHonorsChannelTLSAndHeaderOverride(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "custom-ws-beta", r.Header.Get("OpenAI-Beta"))
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer server.Close()
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	info := &relaycommon.RelayInfo{RelayMode: relayconstant.RelayModeResponses, RequestURLPath: "/v1/responses", ChannelMeta: &relaycommon.ChannelMeta{ChannelType: constant.ChannelTypeOpenAI, ChannelBaseUrl: server.URL, ApiKey: "test-key"}}
	info.ChannelMeta.HeadersOverride = map[string]any{"OpenAI-Beta": "custom-ws-beta"}
	adaptor := &openai.Adaptor{}
	adaptor.Init(info)
	_, _, err := channel.DoResponsesWebSocketRequest(adaptor, ctx, info)
	require.Error(t, err, "an untrusted certificate must be rejected by default")
	info.ChannelSetting.TLSInsecureSkipVerify = true
	conn, _, err := channel.DoResponsesWebSocketRequest(adaptor, ctx, info)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}
