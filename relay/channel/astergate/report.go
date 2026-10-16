package astergate

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
)

type Window struct {
	UsedPercent   *float64   `json:"used_percent"`
	ResetsAt      *time.Time `json:"resets_at,omitempty"`
	WindowSeconds int64      `json:"window_seconds"`
	CheckedAt     *time.Time `json:"checked_at,omitempty"`
}

type Report struct {
	Protocol        string    `json:"protocol"`
	ProtocolVersion int       `json:"protocol_version"`
	Status          string    `json:"status"`
	Scope           string    `json:"scope"`
	Reason          string    `json:"reason,omitempty"`
	FiveHour        *Window   `json:"five_hour"`
	Weekly          *Window   `json:"weekly"`
	GeneratedAt     time.Time `json:"generated_at"`
}

func FetchReport(ctx context.Context, client *http.Client, base, key string) (*Report, error) {
	resp, err := Request(ctx, client, base, "usage", "", key, nil, nil)
	if err != nil {
		return nil, errors.New("AsterGate usage request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("AsterGate usage request rejected")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("invalid AsterGate usage response")
	}
	var report Report
	if common.Unmarshal(data, &report) != nil || report.Protocol != "astergate-usage" || report.ProtocolVersion != 1 || report.Scope != "bound_account" || (report.Status != "available" && report.Status != "unavailable") {
		return nil, errors.New("unsupported AsterGate usage report")
	}
	for _, window := range []*Window{report.FiveHour, report.Weekly} {
		if window != nil && (window.UsedPercent == nil || math.IsNaN(*window.UsedPercent) || math.IsInf(*window.UsedPercent, 0) || *window.UsedPercent < 0 || *window.UsedPercent > 100) {
			return nil, errors.New("invalid AsterGate usage window")
		}
	}
	if report.FiveHour != nil && report.FiveHour.WindowSeconds != 18000 || report.Weekly != nil && report.Weekly.WindowSeconds != 604800 {
		return nil, errors.New("invalid AsterGate usage window")
	}
	// Reasons are protocol enums, not arbitrary upstream messages that may echo secrets.
	switch report.Reason {
	case "", "account_not_authorized", "key_not_bound_to_single_account", "account_not_direct", "not_observed", "usage_not_permitted":
	default:
		report.Reason = "unavailable"
	}
	return &report, nil
}
