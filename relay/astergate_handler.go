package relay

import (
	"cmp"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/relay/channel/astergate"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/tokenkit"
	"github.com/gin-gonic/gin"
)

// AsterGateClaudeHelper deliberately bypasses ClaudeHelper and all provider DTO
// conversions. The host still owns authorization, selection and billing.
func AsterGateClaudeHelper(c *gin.Context, info *relaycommon.RelayInfo) *types.NewAPIError {
	info.InitChannelMeta(c)
	return forwardAsterGate(c, info, "messages", true)
}

func AsterGateCountTokens(c *gin.Context, info *relaycommon.RelayInfo) *types.NewAPIError {
	info.InitChannelMeta(c)
	return forwardAsterGate(c, info, "messages/count_tokens", false)
}

func forwardAsterGate(c *gin.Context, info *relaycommon.RelayInfo, path string, bill bool) *types.NewAPIError {
	if !constant.AsterGateEnabled() {
		return types.NewError(errors.New("channel is disabled"), types.ErrorCodeGetChannelFailed, types.ErrOptionWithSkipRetry())
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return types.NewError(err, types.ErrorCodeReadRequestBodyFailed, types.ErrOptionWithSkipRetry())
	}
	client, err := service.GetHttpClientWithProxySettings(info.ChannelSetting.Proxy, info.ChannelSetting)
	if err != nil {
		return types.NewError(errors.New(i18n.T(c, i18n.MsgAsterGateTransport)), types.ErrorCodeDoRequestFailed, types.ErrOptionWithSkipRetry())
	}
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	var idleTimer *time.Timer
	if constant.StreamingTimeout > 0 {
		idleTimer = time.AfterFunc(time.Duration(constant.StreamingTimeout)*time.Second, cancel)
		defer idleTimer.Stop()
	}
	resp, err := astergate.Request(ctx, client, info.ChannelBaseUrl, path, c.Request.URL.RawQuery, info.ApiKey, c.Request.Header, common.NewReplayableBodyReader(storage))
	if err != nil {
		return types.NewError(errors.New(i18n.T(c, i18n.MsgAsterGateUpstream)), types.ErrorCodeDoRequestFailed, types.ErrOptionWithSkipRetry())
	}
	defer resp.Body.Close()
	info.IsStream = strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	observer := &astergate.Observer{Stream: info.IsStream, CountTokens: func(text string) int {
		return tokenkit.Estimate(info.UpstreamModelName, text)
	}}
	if info.IsStream {
		info.StreamStatus = relaycommon.NewStreamStatus()
		info.StreamStatus.RequireTerminal()
	}
	headers := astergate.Headers(resp.Header)
	// These are upstream-owned quota observations, not inbound identity claims.
	for key, values := range resp.Header {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-astergate-quota-") || lower == "x-astergate-version" || lower == "x-astergate-usage-protocol" || lower == "x-astergate-usage-protocol-supported" || lower == "x-astergate-account-ref" {
			headers[key] = values
		}
	}
	for key, values := range headers {
		c.Writer.Header()[key] = values
	}
	c.Status(resp.StatusCode)
	c.Set("astergate_response_committed", true)
	c.Writer.WriteHeaderNow()
	buffer := make([]byte, 32*1024)
	var copyErr error
	var generationEnd time.Time
	for {
		n, readErr := resp.Body.Read(buffer)
		if n > 0 {
			if idleTimer != nil {
				idleTimer.Reset(time.Duration(constant.StreamingTimeout) * time.Second)
			}
			observer.Write(buffer[:n])
			if !info.IsStream || observer.GenerationStarted {
				info.SetFirstResponseTime()
			}
			if generationEnd.IsZero() && (observer.Done || observer.Failed) {
				generationEnd = time.Now()
			}
			_, copyErr = c.Writer.Write(buffer[:n])
			info.IncrSendResponseCount()
			if info.IsStream {
				c.Writer.Flush()
			}
			if copyErr != nil {
				break
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				copyErr = readErr
			} else {
				observer.Finalize(true)
				if observer.GenerationStarted {
					info.SetFirstResponseTime()
				}
			}
			break
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Parse only protocol classification; the client's response stays untouched.
		observer.Usage()
		return types.WithClaudeError(types.ClaudeError{Type: cmp.Or(observer.ErrorType, "api_error"), Message: "AsterGate upstream rejected request"}, resp.StatusCode, types.ErrOptionWithSkipRetry())
	}
	if info.IsStream {
		info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonEOF, copyErr)
		if observer.Failed {
			info.StreamStatus.MarkFailed(observer.ErrorType, observer.ErrorType, observer.ErrorStatus)
		}
		if observer.Done && !observer.Failed {
			info.StreamStatus.MarkCompleted()
		}
		if (observer.Failed || !observer.Done) && copyErr == nil {
			copyErr = errors.New("incomplete AsterGate stream")
		}
	}
	if err := observer.Err(); err != nil {
		return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
	}
	if bill {
		// A routing failure after HTTP 200 heartbeats has no billable usage.
		// Leave the reservation unsettled so the controller refunds the failure.
		usage := observer.BillingUsage(info.GetEstimatePromptTokens())
		if info.IsStream && usage == nil {
			return types.NewError(errors.New("AsterGate stream ended without usage or content"), types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
		}
		if usage != nil {
			if generationEnd.IsZero() {
				generationEnd = time.Now()
			}
			info.SetAsterGatePerformance(generationEnd, usage.CompletionTokens, observer.ThinkingTokens)
		}
		if observer.Estimated {
			common.SetContextKey(c, constant.ContextKeyLocalCountTokens, true)
		}
		if observer.QuotaClamp != nil {
			info.QuotaClamp = observer.QuotaClamp
		}
		for name, count := range observer.ToolCounts {
			if name == "web_search" {
				name = cmp.Or(info.WebSearchBillingKey, dto.BuildInToolWebSearch)
			}
			if count > 0 {
				info.SetBillableToolCount(name, count)
			}
		}
		service.PostTextConsumeQuota(c, info, usage, nil)
	}
	if copyErr != nil {
		return types.NewError(errors.New("AsterGate response interrupted"), types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
	}
	return nil
}
