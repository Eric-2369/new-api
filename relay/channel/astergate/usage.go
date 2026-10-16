package astergate

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/tidwall/gjson"
)

// Observer reads a bounded side copy; it never rewrites or buffers the forwarded
// stream. An oversized event is skipped and subsequent events are still observed.
type Observer struct {
	Started           bool
	ErrorType         string
	ErrorStatus       int
	eventName         string
	HasContent        bool
	GenerationStarted bool
	ThinkingTokens    int
	Estimated         bool
	CountTokens       func(string) int
	output            strings.Builder
	estimatedOutput   int
	QuotaClamp        *common.QuotaClamp
	Done              bool
	Failed            bool
	ToolCounts        map[string]int
	Stream            bool
	line, event       []byte
	oversized         bool
	lineLength        int
	usage             dto.ClaudeUsage
	seen              bool
}

// Match AsterGate's default bounds: 8 MiB per SSE event and 64 MiB per
// buffered response. A larger unary response must not silently lose billing.
const observationLimit = 8 << 20
const unaryObservationLimit = 64 << 20

func (o *Observer) Err() error {
	if !o.Stream && o.oversized {
		return errors.New("AsterGate response exceeds the 64 MiB observation limit")
	}
	return nil
}

func (o *Observer) Write(p []byte) {
	if !o.Stream {
		if len(o.event)+len(p) > unaryObservationLimit {
			o.oversized = true
			o.event = nil
		}
		if !o.oversized {
			o.event = append(o.event, p...)
		}
		return
	}
	for _, b := range p {
		if b != '\n' {
			o.lineLength++
			if len(o.line)+len(o.event) >= observationLimit {
				o.oversized = true
				o.line = nil
				o.event = nil
			}
			if !o.oversized {
				o.line = append(o.line, b)
			}
			continue
		}
		line := bytes.TrimSuffix(o.line, []byte{'\r'})
		emptyLine := o.lineLength == 0 || (o.lineLength == 1 && len(line) == 0)
		o.lineLength = 0
		if emptyLine {
			if !o.oversized {
				o.observe(o.event)
			} else {
				o.observe(nil)
			}
			o.event = nil
			o.eventName = ""
			o.oversized = false
		} else if !o.oversized && bytes.HasPrefix(line, []byte("event:")) {
			o.eventName = strings.TrimSpace(string(line[6:]))
		} else if !o.oversized && bytes.HasPrefix(line, []byte("data:")) {
			data := bytes.TrimPrefix(line[5:], []byte{' '})
			o.event = append(o.event, data...)
			o.event = append(o.event, '\n')
		}
		o.line = nil
	}
}

// Finalize accepts the peer's final event without a trailing blank line only on
// clean EOF. A transport error must not turn an unfinished event into success.
func (o *Observer) Finalize(cleanEOF bool) {
	if !o.Stream || !cleanEOF {
		return
	}
	if o.lineLength > 0 {
		o.Write([]byte("\n"))
	}
	o.Write([]byte("\n"))
}

func (o *Observer) observe(body []byte) {
	kind := o.eventName
	var root gjson.Result
	if gjson.ValidBytes(body) {
		root = gjson.ParseBytes(body)
		if typed := root.Get("type").String(); typed != "" {
			kind = typed
		}
	}
	switch kind {
	case "message_start":
		o.Started = root.Get("message").IsObject()
	case "message_stop":
		o.Done = true
	case "error":
		o.Failed = true
		// Retain protocol classification, never the upstream's free-form message.
		o.ErrorType = root.Get("error.type").String()
		switch o.ErrorType {
		case "invalid_request_error":
			o.ErrorStatus = 400
		case "authentication_error":
			o.ErrorStatus = 401
		case "permission_error":
			o.ErrorStatus = 403
		case "not_found_error":
			o.ErrorStatus = 404
		case "request_too_large":
			o.ErrorStatus = 413
		case "rate_limit_error":
			o.ErrorStatus = 429
		case "overloaded_error":
			o.ErrorStatus = 529
		default:
			o.ErrorType, o.ErrorStatus = "api_error", 502
		}
	}
	if !root.IsObject() {
		return
	}
	// Match AsterGate/Claude Code first_content_ms: an empty block start
	// starts generation too. Omitted thinking may emit only a signature later.
	// Keep this separate from billable content and interrupted-output estimates.
	if kind == "content_block_start" || kind == "content_block_delta" {
		o.GenerationStarted = true
	}
	switch kind {
	case "content_block_start":
		block := root.Get("content_block")
		if block.Get("type").String() == "tool_use" || block.Get("type").String() == "server_tool_use" {
			o.HasContent = true
			o.addOutputEstimate(40)
			o.observeOutput(block.Get("name").String())
		}
		o.observeOutput(block.Get("text").String())
		o.observeOutput(block.Get("thinking").String())
	case "content_block_delta":
		delta := root.Get("delta")
		for _, key := range []string{"text", "thinking", "partial_json"} {
			o.observeOutput(delta.Get(key).String())
		}
	}
	usage := root.Get("usage")
	if kind == "message_start" {
		usage = root.Get("message.usage")
	}
	if !usage.IsObject() {
		return
	}
	// Optional thinking metadata must not invalidate otherwise valid token usage.
	if v := usage.Get("output_tokens_details.thinking_tokens"); v.Type == gjson.Number {
		if n, err := strconv.ParseInt(v.Raw, 10, 32); err == nil && n >= 0 {
			o.ThinkingTokens = int(n)
		}
	}
	// Only accept protocol quantities, never a caller-supplied billing_usage object.
	fields := map[string]*int{
		"input_tokens":                &o.usage.InputTokens,
		"output_tokens":               &o.usage.OutputTokens,
		"cache_creation_input_tokens": &o.usage.CacheCreationInputTokens,
		"cache_read_input_tokens":     &o.usage.CacheReadInputTokens,
	}
	values := make(map[string]int)
	for _, name := range []string{"input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "cache_creation.ephemeral_5m_input_tokens", "cache_creation.ephemeral_1h_input_tokens", "server_tool_use.web_search_requests", "server_tool_use.web_fetch_requests", "server_tool_use.code_execution_requests", "server_tool_use.tool_search_requests"} {
		v := usage.Get(name)
		if !v.Exists() {
			continue
		}
		n, err := strconv.ParseInt(v.Raw, 10, 32)
		if v.Type != gjson.Number || err != nil || n < 0 || n > math.MaxInt32 {
			return
		}
		values[name] = int(n)
	}
	for name, value := range values {
		if tool, ok := strings.CutPrefix(name, "server_tool_use."); ok {
			if o.ToolCounts == nil {
				o.ToolCounts = make(map[string]int)
			}
			o.ToolCounts[strings.TrimSuffix(tool, "_requests")] = value
			continue
		}
		if field := fields[name]; field != nil {
			*field = value
			continue
		}
		if o.usage.CacheCreation == nil {
			o.usage.CacheCreation = &dto.ClaudeCacheCreationUsage{}
		}
		if strings.HasSuffix(name, "ephemeral_5m_input_tokens") {
			o.usage.CacheCreation.Ephemeral5mInputTokens = value
		} else {
			o.usage.CacheCreation.Ephemeral1hInputTokens = value
		}
	}
	o.seen = o.seen || len(values) > 0
}

func (o *Observer) Usage() *dto.Usage {
	if !o.Stream && !o.oversized {
		o.observe(o.event)
	}
	if !o.seen {
		return nil
	}
	total, clamp := common.QuotaFromFloatChecked(float64(o.usage.InputTokens) + float64(o.usage.OutputTokens))
	if clamp != nil {
		o.QuotaClamp = clamp
	}
	u := &dto.Usage{
		PromptTokens:                o.usage.InputTokens,
		CompletionTokens:            o.usage.OutputTokens,
		TotalTokens:                 total,
		UsageSemantic:               "anthropic",
		BillingUsage:                dto.NewClaudeMessagesBillingUsage(&o.usage),
		ClaudeCacheCreation5mTokens: o.usage.GetCacheCreation5mTokens(),
		ClaudeCacheCreation1hTokens: o.usage.GetCacheCreation1hTokens(),
	}
	if u.BillingUsage != nil {
		u.BillingUsage.Estimated = o.Estimated
	}
	u.PromptTokensDetails.CachedTokens = o.usage.CacheReadInputTokens
	u.PromptTokensDetails.CachedCreationTokens = o.usage.CacheCreationInputTokens
	return u
}

// observeOutput keeps a bounded side copy for interrupted-stream estimates.
// Small responses are tokenized together; long responses are counted in chunks.
func (o *Observer) observeOutput(text string) {
	if text == "" {
		return
	}
	o.HasContent = true
	if o.CountTokens == nil {
		return
	}
	o.output.WriteString(text)
	if o.output.Len() >= 64*1024 {
		o.addOutputEstimate(o.CountTokens(o.output.String()))
		o.output.Reset()
	}
}

func (o *Observer) addOutputEstimate(tokens int) {
	total, clamp := common.QuotaFromFloatChecked(float64(o.estimatedOutput) + float64(max(0, tokens)))
	o.estimatedOutput = total
	if clamp != nil {
		o.QuotaClamp = clamp
	}
}

// BillingUsage returns nil when a stream has neither usage nor generated content.
// Preserve upstream cache quantities and authoritative completed output usage.
func (o *Observer) BillingUsage(promptEstimate int) *dto.Usage {
	usage := o.Usage()
	if !o.Stream {
		return usage
	}
	if !o.seen && !o.HasContent {
		return nil
	}
	if !o.seen {
		o.Estimated = true
		o.usage.InputTokens = max(0, promptEstimate)
		o.seen = true
	}
	if o.HasContent && (!o.Done || o.usage.OutputTokens == 0) {
		if o.CountTokens != nil && o.output.Len() > 0 {
			o.addOutputEstimate(o.CountTokens(o.output.String()))
			o.output.Reset()
		}
		if o.estimatedOutput > o.usage.OutputTokens {
			o.Estimated = true
			o.usage.OutputTokens = o.estimatedOutput
		}
	}
	return o.Usage()
}
