package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay"
	"github.com/QuantumNous/new-api/relay/channel/astergate"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// CountAsterGateTokens is registered behind the ordinary token, model-limit and
// channel-selection middleware. Other channel types keep their disabled route.
func CountAsterGateTokens(c *gin.Context) {
	if common.GetContextKeyInt(c, constant.ContextKeyChannelType) != constant.ChannelTypeAsterGate {
		c.Status(http.StatusNotFound)
		return
	}
	request, err := helper.GetAndValidateAsterGateRequest(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"type": "error", "error": gin.H{"type": "invalid_request_error", "message": i18n.T(c, i18n.MsgAsterGateInvalidRequest)}})
		return
	}
	info := relaycommon.GenRelayInfoClaude(c, request)
	if apiErr := relay.AsterGateCountTokens(c, info); apiErr != nil && !c.Writer.Written() {
		c.JSON(apiErr.StatusCode, gin.H{"type": "error", "error": apiErr.ToClaudeError()})
	}
}

// GetAsterGateChannelUsage uses the same ChannelRead authorization as Codex.
// Protocol v1 deliberately requests the single-bound-account contract, supported
// by both current v2 servers and older servers. No upstream raw payload is exposed.
func GetAsterGateChannelUsage(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		common.ApiErrorT(c, "Invalid parameters")
		return
	}
	channel, err := model.GetChannelById(id, true)
	if err != nil || channel == nil || channel.Type != constant.ChannelTypeAsterGate || channel.ChannelInfo.IsMultiKey {
		common.ApiErrorT(c, "Invalid parameters")
		return
	}
	settings := channel.GetSetting()
	client, err := service.GetHttpClientWithProxySettings(settings.Proxy, settings)
	if err != nil {
		common.ApiErrorT(c, "Failed to fetch usage")
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()
	report, err := astergate.FetchReport(ctx, client, channel.GetBaseURL(), channel.Key)
	if err != nil {
		common.ApiErrorT(c, "Failed to fetch usage")
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"success": true, "data": report})
}

// Unsupported transformations are rejected rather than silently ignored.
func validateAsterGateChannel(channel *model.Channel) error {
	if !constant.AsterGateEnabled() {
		return common.NewMessage("Invalid AsterGate channel configuration")
	}
	if _, err := astergate.Endpoint(channel.GetBaseURL(), "messages"); err != nil {
		return common.NewMessage("Invalid AsterGate channel configuration")
	}
	settings := channel.GetSetting()
	if settings.TLSInsecureSkipVerify || settings.SystemPrompt != "" {
		return common.NewMessage("Invalid AsterGate channel configuration")
	}
	for _, value := range []*string{channel.ModelMapping, channel.ParamOverride, channel.HeaderOverride} {
		if value != nil && strings.TrimSpace(*value) != "" && strings.TrimSpace(*value) != "{}" {
			return common.NewMessage("Invalid AsterGate channel configuration")
		}
	}
	return nil
}

var errAsterGateProbeRestricted = common.NewMessage("AsterGate requires Claude Code; this channel test is skipped. Verify with Claude Code.")

// Keep the AsterGate probe out of the generic converter/adaptor test pipeline.
func testAsterGateChannel(ctx context.Context, channel *model.Channel, testModel string, stream bool) testResult {
	if !constant.AsterGateEnabled() {
		return testResult{localErr: errAsterGateProbeRestricted}
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	payload, err := common.Marshal(map[string]any{"model": testModel, "max_tokens": 16, "stream": stream, "messages": []map[string]string{{"role": "user", "content": "Hi"}}})
	if err != nil {
		return testResult{context: c, localErr: err, newAPIError: types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())}
	}
	settings := channel.GetSetting()
	client, err := service.GetHttpClientWithProxySettings(settings.Proxy, settings)
	if err != nil {
		return testResult{context: c, localErr: err, newAPIError: types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())}
	}
	key, _, keyErr := channel.GetNextEnabledKey()
	if keyErr != nil {
		return testResult{context: c, localErr: keyErr, newAPIError: keyErr}
	}
	probeCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	resp, err := astergate.Request(probeCtx, client, channel.GetBaseURL(), "messages", "", key, http.Header{"Content-Type": {"application/json"}, "Anthropic-Version": {"2023-06-01"}}, bytes.NewReader(payload))
	if err != nil {
		probeErr := errors.New("AsterGate upstream request failed")
		return testResult{context: c, localErr: probeErr, newAPIError: types.NewError(probeErr, types.ErrorCodeDoRequestFailed, types.ErrOptionWithSkipRetry())}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err == nil && len(body) > 4<<20 {
		err = errors.New("AsterGate probe response too large")
	}
	if err == nil && resp.StatusCode == http.StatusForbidden &&
		(resp.Header.Get("X-AsterGate-Error-Code") == "claude_code_only" || gjson.GetBytes(body, "error.code").String() == "claude_code_only") {
		return testResult{context: c, localErr: errAsterGateProbeRestricted}
	}
	if err == nil && resp.StatusCode != http.StatusOK {
		err = errors.New("AsterGate upstream rejected probe")
	}
	if err == nil {
		valid := false
		if stream {
			observer := &astergate.Observer{Stream: true}
			observer.Write(body)
			observer.Finalize(true)
			valid = strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") && observer.Started && observer.Done && !observer.Failed
		} else if gjson.ValidBytes(body) {
			message := gjson.ParseBytes(body)
			valid = message.Get("type").String() == "message" && message.Get("content").IsArray() && message.Get("usage").IsObject() && !message.Get("error").Exists()
		}
		if !valid {
			err = errors.New("invalid AsterGate probe response")
		}
	}
	if err != nil {
		return testResult{context: c, localErr: err, newAPIError: types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())}
	}
	return testResult{context: c}
}
