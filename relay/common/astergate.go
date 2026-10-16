package common

import "time"

// AsterGatePerformance records total output (including thinking) over the
// observed generation interval, independently of billing and request latency.
type AsterGatePerformance struct {
	DurationMS     int64  `json:"duration_ms"`
	GenerationMS   int64  `json:"generation_ms"`
	OutputTokens   int64  `json:"output_tokens"`
	ThinkingTokens int    `json:"thinking_tokens"`
	Basis          string `json:"basis"`
}

func (info *RelayInfo) SetAsterGatePerformance(end time.Time, output, thinking int) {
	if info.StartTime.IsZero() || !end.After(info.StartTime) {
		return
	}
	p := &AsterGatePerformance{
		DurationMS:     max(1, end.Sub(info.StartTime).Milliseconds()),
		OutputTokens:   int64(max(0, output)),
		ThinkingTokens: thinking,
		Basis:          "generation",
	}
	// Unary responses and streams without a content event have no observable
	// generation start. Leave their TPS unavailable instead of inventing one.
	if info.IsStream && info.HasSendResponse() && end.After(info.FirstResponseTime) {
		p.GenerationMS = max(1, end.Sub(info.FirstResponseTime).Milliseconds())
	}
	info.AsterGatePerformance = p
}
