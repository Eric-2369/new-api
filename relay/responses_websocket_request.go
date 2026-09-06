package relay

import (
	"fmt"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/model_setting"
	"github.com/gin-gonic/gin"
)

// Responses WebSocket conversion intentionally stays separate from HTTP:
// preserve Codex frame fields and extensions while applying channel rules.
func prepareResponsesWebSocketPayload(
	c *gin.Context,
	info *relaycommon.RelayInfo,
	adaptor channel.Adaptor,
	request *dto.OpenAIResponsesRequest,
	payload []byte,
) ([]byte, *types.NewAPIError) {
	if model_setting.GetGlobalSettings().PassThroughRequestEnabled || info.ChannelSetting.PassThroughBodyEnabled {
		return payload, nil
	}

	convertedRequest, err := common.DeepCopy(request)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	if err = helper.ModelMappedHelper(c, info, convertedRequest); err != nil {
		return nil, types.NewError(err, types.ErrorCodeChannelModelMappedError, types.ErrOptionWithSkipRetry())
	}
	if err = helper.ApplyReasoningModelSuffix(c, info, convertedRequest); err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	if info.ChannelType == constant.ChannelTypeCodex && common.GetJsonType(convertedRequest.Input) == "string" {
		var inputText string
		if err = common.Unmarshal(convertedRequest.Input, &inputText); err != nil {
			return nil, types.NewError(err, types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
		}
		content, marshalErr := common.Marshal([]struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{{Type: "input_text", Text: inputText}})
		if marshalErr != nil {
			return nil, types.NewError(marshalErr, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
		convertedRequest.Input, marshalErr = common.Marshal([]dto.Input{{Role: "user", Content: content}})
		if marshalErr != nil {
			return nil, types.NewError(marshalErr, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
	}
	converted, err := adaptor.ConvertOpenAIResponsesRequest(c, info, *convertedRequest)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	relaycommon.AppendRequestConversionFromRequest(info, converted)

	convertedJSON, err := common.Marshal(converted)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	var normalized map[string]common.RawMessage
	if err = common.Unmarshal(payload, &normalized); err != nil {
		return nil, types.NewError(err, types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}
	var known map[string]common.RawMessage
	if err = common.Unmarshal(convertedJSON, &known); err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	streamOptions := normalized["stream_options"]
	var wireReasoning map[string]common.RawMessage
	_ = common.Unmarshal(normalized["reasoning"], &wireReasoning)
	if common.GetJsonType(wireReasoning["effort"]) == "number" {
		var convertedReasoning map[string]common.RawMessage
		if err = common.Unmarshal(known["reasoning"], &convertedReasoning); err != nil {
			return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
		if _, overridden := convertedReasoning["effort"]; !overridden {
			if convertedReasoning == nil {
				convertedReasoning = make(map[string]common.RawMessage)
			}
			convertedReasoning["effort"] = wireReasoning["effort"]
			known["reasoning"], err = common.Marshal(convertedReasoning)
			if err != nil {
				return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
			}
		}
	}
	for _, field := range []string{
		"model", "input", "include", "conversation", "context_management", "instructions",
		"max_output_tokens", "top_logprobs", "metadata", "moderation", "parallel_tool_calls",
		"frequency_penalty", "presence_penalty", "previous_response_id", "reasoning", "service_tier",
		"store", "prompt_cache_key", "prompt_cache_options", "prompt_cache_retention",
		"safety_identifier", "stream", "stream_options", "temperature", "text", "tool_choice",
		"tools", "top_p", "truncation", "user", "max_tool_calls", "prompt", "client_metadata",
		"enable_thinking", "thinking_budget", "preset",
		"chat_template_kwargs", "top_k", "min_p", "repetition_penalty", "stop", "cache_salt",
	} {
		delete(normalized, field)
	}
	for key, value := range known {
		normalized[key] = value
	}
	// WS extensions evolve independently of the HTTP DTO. Preserve the raw
	// options, including explicit false values, before applying channel rules.
	if len(streamOptions) > 0 {
		normalized["stream_options"] = streamOptions
	}
	eventType, err := common.Marshal("response.create")
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	normalized["type"] = eventType
	outbound, err := common.Marshal(normalized)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	outbound, err = relaycommon.RemoveDisabledFields(outbound, info.ChannelOtherSettings, false)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	if len(info.ParamOverride) > 0 {
		outbound, err = relaycommon.ApplyParamOverrideWithRelayInfo(outbound, info)
		if err != nil {
			return nil, types.NewError(err, types.ErrorCodeChannelParamOverrideInvalid, types.ErrOptionWithSkipRetry())
		}
	}
	return outbound, nil
}

// decodeResponsesWSRequest keeps numeric Codex effort in the wire body while
// decoding a typed view for routing and quota validation. HTTP DTO semantics
// stay unchanged. A channel's explicit reasoning conversion still takes priority.
func decodeResponsesWSRequest(payload []byte, request *dto.OpenAIResponsesRequest) error {
	var body map[string]common.RawMessage
	if err := common.Unmarshal(payload, &body); err != nil {
		return err
	}
	var reasoning map[string]common.RawMessage
	if err := common.Unmarshal(body["reasoning"], &reasoning); err != nil || common.GetJsonType(reasoning["effort"]) != "number" {
		return common.Unmarshal(payload, request)
	}
	var budget uint
	if err := common.Unmarshal(reasoning["effort"], &budget); err != nil || helper.ExceedsMaxTokensLimit(&budget) {
		return fmt.Errorf("reasoning.effort must be a non-negative integer within the token limit")
	}
	delete(reasoning, "effort")
	var err error
	body["reasoning"], err = common.Marshal(reasoning)
	if err != nil {
		return err
	}
	typed, err := common.Marshal(body)
	if err != nil {
		return err
	}
	return common.Unmarshal(typed, request)
}

// responsesWSHasUsage checks raw counters before the shared accumulator estimates
// missing usage. Nested cache/audio/tool counters count, even if totals are absent.
func responsesWSHasUsage(value any) bool {
	switch value := value.(type) {
	case float64:
		return value != 0
	case map[string]any:
		for _, child := range value {
			if responsesWSHasUsage(child) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if responsesWSHasUsage(child) {
				return true
			}
		}
	}
	return false
}
