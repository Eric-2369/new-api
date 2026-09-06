package service

import (
	"fmt"
	"strings"

	"github.com/QuantumNous/new-api/common"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/tokenkit"
)

// ResponsesUsageAccumulator owns the accounting facts for one Responses stream.
// HTTP SSE and WebSocket transports feed the same events into it, then settle
// Finish's usage through the normal text billing path, including interrupted
// streams. Observe and Finish must be called by the same stream owner.
type ResponsesUsageAccumulator struct {
	info           *relaycommon.RelayInfo
	usage          *dto.Usage
	outputText     strings.Builder
	toolOutputs    map[string]*responsesToolOutput
	toolIDs        map[string]string
	toolIndexes    map[int]string
	billableTools  map[string]struct{}
	imageCounter   relaycommon.ImageGenerationCallCounter
	imageCommitted bool
	started        bool
	finished       bool
}

func NewResponsesUsageAccumulator(info *relaycommon.RelayInfo) *ResponsesUsageAccumulator {
	return &ResponsesUsageAccumulator{info: info, usage: &dto.Usage{}}
}

// Observe feeds one decoded stream event. raw is the same event's wire bytes;
// the vendor tool-usage reader runs on it once, at the terminal event.
func (a *ResponsesUsageAccumulator) Observe(event *dto.ResponsesStreamResponse, raw []byte) {
	if a == nil || event == nil || a.finished {
		return
	}
	if event.Response != nil {
		a.info.ObserveResponseModel(event.Response.Model)
	}
	a.started = true
	ObserveResponsesOutcome(a.info, event)
	switch event.Type {
	case "response.completed", "response.done", "response.failed", "response.incomplete", "response.cancelled", "response.canceled":
		if event.Response != nil {
			ApplyResponsesUsage(a.usage, event.Response.Usage)
			for i := range event.Response.Output {
				a.observeToolOutput(&event.Response.Output[i], "", &i, "")
				item := &event.Response.Output[i]
				if item.Status == "" || item.Status == "completed" {
					a.observeBillableToolCall(item, &i)
				}
			}
			if a.outputText.Len() == 0 {
				// Some upstreams carry the output only on the terminal event.
				a.outputText.WriteString(relayconvert.ExtractOutputTextFromResponses(event.Response))
			}
		}
		// Vendor counts are cumulative on the terminal event and replace the
		// web_search_call items counted from output_item.done.
		a.info.ApplyVendorToolUsage(raw)
		if a.imageCommitted {
			return
		}
		// Images that completed before any terminal, failed ones included,
		// were delivered and stay billable; Observe still skips unfinished
		// image items.
		if event.Response != nil {
			for i := range event.Response.Output {
				a.imageCounter.Observe(&event.Response.Output[i], &i)
			}
		}
		a.imageCounter.Commit(a.info)
		a.imageCommitted = true
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		a.observeToolOutput(nil, event.ItemID, event.OutputIndex, event.Delta)
	case "response.output_text.delta",
		"response.reasoning_summary_text.delta", "response.reasoning_text.delta", "response.refusal.delta":
		// Every delta kind here is generated output that upstream bills as
		// output tokens, so all of them feed the missing-usage estimate.
		a.outputText.WriteString(event.Delta)
	case dto.ResponsesOutputTypeItemDone:
		if event.Item == nil {
			return
		}
		a.observeToolOutput(event.Item, event.Item.ID, event.OutputIndex, "")
		switch event.Item.Type {
		case dto.BuildInCallWebSearchCall, dto.BuildInCallFileSearchCall, dto.BuildInCallFunctionCall:
			a.observeBillableToolCall(event.Item, event.OutputIndex)
		case dto.ResponsesOutputTypeImageGenerationCall:
			if !a.imageCommitted {
				a.imageCounter.Observe(event.Item, event.OutputIndex)
			}
		}
	}
}

// Completed item events and terminal snapshots describe the same tool calls.
// Remember every available identity, including aliases learned on duplicates.
func (a *ResponsesUsageAccumulator) observeBillableToolCall(item *dto.ResponsesOutput, index *int) {
	switch item.Type {
	case dto.BuildInCallWebSearchCall, dto.BuildInCallFileSearchCall, dto.BuildInCallFunctionCall:
	default:
		return
	}
	var aliases []string
	if item.ID != "" {
		aliases = append(aliases, "id:"+item.ID)
	}
	if item.CallId != "" {
		aliases = append(aliases, "call:"+item.CallId)
	}
	if index != nil && *index >= 0 {
		aliases = append(aliases, fmt.Sprintf("index:%d", *index))
	}
	if a.billableTools == nil {
		a.billableTools = make(map[string]struct{})
	}
	seen := false
	for _, alias := range aliases {
		if _, exists := a.billableTools[alias]; exists {
			seen = true
		}
		a.billableTools[alias] = struct{}{}
	}
	// Without an identity, preserve per-event counting rather than collapsing
	// distinct calls with identical names or arguments.
	if !seen {
		a.info.CountBillableToolCall(item.Type, item.Name)
	}
}

func (a *ResponsesUsageAccumulator) Finish() *dto.Usage {
	if a.finished {
		return a.usage
	}
	a.finished = true
	// A final image item can already have reached the client before the stream
	// disconnects, so completed tool usage is retained even without a terminal.
	if !a.imageCommitted {
		a.imageCounter.Commit(a.info)
		a.imageCommitted = true
	}
	if a.usage.CompletionTokens == 0 {
		if output := a.outputText.String(); output != "" {
			a.usage.CompletionTokens = tokenkit.Count(a.info.GetUpstreamModelName(), output)
		}
		for _, output := range a.toolOutputs {
			text := output.delta.String()
			if output.complete != nil {
				text = *output.complete
			}
			var clamp *common.QuotaClamp
			a.usage.CompletionTokens, clamp = common.QuotaFromFloatChecked(float64(a.usage.CompletionTokens) + float64(tokenkit.Count(a.info.GetUpstreamModelName(), text)))
			if clamp != nil {
				a.info.QuotaClamp = clamp
			}
		}
	}
	// Upstream bills the prompt as soon as it starts generating, so a stream
	// that produced any event but no usage still owes its input tokens unless
	// upstream reported an explicit failure.
	billsPrompt := a.usage.CompletionTokens != 0 || (a.started && !a.info.StreamStatus.ResponseFailed())
	if a.usage.PromptTokens == 0 && billsPrompt {
		a.usage.PromptTokens = a.info.GetEstimatePromptTokens()
	}
	a.usage.TotalTokens = a.usage.PromptTokens + a.usage.CompletionTokens
	if a.usage.BillingUsage != nil {
		a.usage.BillingUsage = dto.CloneBillingUsageWithEstimatedCompletion(a.usage.BillingUsage, a.usage.CompletionTokens)
	}
	return a.usage
}

// ObserveResponsesOutcome records the protocol outcome of one Responses event
// on the stream status for health classification. Only codes and types are
// kept; messages never leave the event.
func ObserveResponsesOutcome(info *relaycommon.RelayInfo, event *dto.ResponsesStreamResponse) {
	if info == nil || info.StreamStatus == nil || event == nil {
		return
	}
	var responseStatus string
	if event.Response != nil {
		_ = common.Unmarshal(event.Response.Status, &responseStatus)
	}
	switch {
	case event.Type == "error" || event.Type == "response.failed" || event.Type == "response.error" || responseStatus == "failed":
		code, errorType := event.Code, ""
		if event.Response != nil {
			if oaiErr := event.Response.GetOpenAIError(); oaiErr != nil {
				if oaiErr.Code != nil {
					code = fmt.Sprint(oaiErr.Code)
				}
				errorType = oaiErr.Type
			}
		}
		info.StreamStatus.MarkFailed(code, errorType, 0)
	case event.Type == "response.incomplete" || responseStatus == "incomplete":
		reason := ""
		if event.Response != nil && event.Response.IncompleteDetails != nil {
			reason = event.Response.IncompleteDetails.Reason
		}
		info.StreamStatus.MarkIncomplete(reason)
	case event.Type == "response.cancelled" || event.Type == "response.canceled" || responseStatus == "cancelled":
		info.StreamStatus.MarkCancelled()
	case event.Type == "response.completed" || event.Type == "response.done" || responseStatus == "completed":
		info.StreamStatus.MarkCompleted()
	}
}

func ApplyResponsesUsage(dst *dto.Usage, src *dto.Usage) {
	if dst == nil || src == nil {
		return
	}
	incoming := relayconvert.NormalizeResponsesUsage(src)
	if src.InputTokensDetails != nil {
		inputDetails := *src.InputTokensDetails
		incoming.InputTokensDetails = &inputDetails
	}
	if src.OutputTokensDetails != nil {
		incoming.CompletionTokenDetails = *src.OutputTokensDetails
	}
	incoming.PromptCacheHitTokens = src.PromptCacheHitTokens
	dto.MergeUsageNonZero(dst, incoming)
	outputDetails := dst.CompletionTokenDetails
	if outputDetails != (dto.OutputTokenDetails{}) {
		dst.OutputTokensDetails = &outputDetails
	}
}

// A completed tool payload replaces its deltas, while another item's output
// remains independent. IDs and output indices can be supplied at different stages.
type responsesToolOutput struct {
	delta    strings.Builder
	complete *string
}

func (a *ResponsesUsageAccumulator) observeToolOutput(item *dto.ResponsesOutput, id string, index *int, delta string) {
	var complete *string
	if item != nil {
		var payload common.RawMessage
		switch item.Type {
		case "function_call":
			payload = item.Arguments
		case "custom_tool_call":
			payload = item.Input
		default:
			return
		}
		id = item.ID
		if len(payload) > 0 && string(payload) != "null" {
			var text string
			if common.Unmarshal(payload, &text) != nil {
				text = string(payload)
			}
			complete = &text
		}
	}
	if a.toolOutputs == nil {
		a.toolOutputs = make(map[string]*responsesToolOutput)
		a.toolIDs = make(map[string]string)
		a.toolIndexes = make(map[int]string)
	}
	key := ""
	if id != "" {
		key = "id:" + id
	}
	if index != nil {
		key = fmt.Sprintf("index:%d", *index)
		if existing, ok := a.toolIndexes[*index]; ok {
			key = existing
		}
	}
	if existing, ok := a.toolIDs[id]; id != "" && ok {
		key = existing
	}
	// Legacy delta frames omit both identities. A following identified item
	// can adopt that sole anonymous stream rather than count it twice.
	if key != "" && len(a.toolOutputs) == 1 && a.toolOutputs[""] != nil {
		a.toolOutputs[key] = a.toolOutputs[""]
		delete(a.toolOutputs, "")
	}
	if id != "" {
		a.toolIDs[id] = key
	}
	if index != nil {
		a.toolIndexes[*index] = key
	}
	output := a.toolOutputs[key]
	if output == nil {
		output = &responsesToolOutput{}
		a.toolOutputs[key] = output
	}
	if complete != nil {
		output.complete = complete
		output.delta.Reset()
	}
	if output.complete == nil {
		output.delta.WriteString(delta)
	}
}
