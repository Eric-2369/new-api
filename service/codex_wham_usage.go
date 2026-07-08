package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/google/uuid"
)

func FetchCodexWhamUsage(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	accessToken string,
	accountID string,
) (statusCode int, body []byte, err error) {
	if client == nil {
		return 0, nil, fmt.Errorf("nil http client")
	}
	bu := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if bu == "" {
		return 0, nil, fmt.Errorf("empty baseURL")
	}
	at := strings.TrimSpace(accessToken)
	aid := strings.TrimSpace(accountID)
	if at == "" {
		return 0, nil, fmt.Errorf("empty accessToken")
	}
	if aid == "" {
		return 0, nil, fmt.Errorf("empty accountID")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bu+"/backend-api/wham/usage", nil)
	if err != nil {
		return 0, nil, err
	}
	setCodexWhamRequestHeaders(req, at, aid)

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func FetchCodexWhamRateLimitResetCredits(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	accessToken string,
	accountID string,
) (statusCode int, body []byte, err error) {
	if client == nil {
		return 0, nil, fmt.Errorf("nil http client")
	}
	bu := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if bu == "" {
		return 0, nil, fmt.Errorf("empty baseURL")
	}
	at := strings.TrimSpace(accessToken)
	aid := strings.TrimSpace(accountID)
	if at == "" {
		return 0, nil, fmt.Errorf("empty accessToken")
	}
	if aid == "" {
		return 0, nil, fmt.Errorf("empty accountID")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, bu+"/backend-api/wham/rate-limit-reset-credits", nil)
	if err != nil {
		return 0, nil, err
	}
	setCodexWhamRequestHeaders(req, at, aid)

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func ConsumeCodexWhamRateLimitResetCredit(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	accessToken string,
	accountID string,
) (statusCode int, body []byte, err error) {
	if client == nil {
		return 0, nil, fmt.Errorf("nil http client")
	}
	bu := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if bu == "" {
		return 0, nil, fmt.Errorf("empty baseURL")
	}
	at := strings.TrimSpace(accessToken)
	aid := strings.TrimSpace(accountID)
	if at == "" {
		return 0, nil, fmt.Errorf("empty accessToken")
	}
	if aid == "" {
		return 0, nil, fmt.Errorf("empty accountID")
	}

	requestPayload := map[string]string{
		"redeem_request_id": uuid.NewString(),
	}
	if creditsStatusCode, creditsBody, creditsErr := FetchCodexWhamRateLimitResetCredits(ctx, client, baseURL, accessToken, accountID); creditsErr == nil && creditsStatusCode >= 200 && creditsStatusCode < 300 {
		var resetCredits struct {
			Credits []struct {
				ID        string `json:"id"`
				Status    string `json:"status"`
				ExpiresAt string `json:"expires_at"`
			} `json:"credits"`
		}
		if common.Unmarshal(creditsBody, &resetCredits) == nil {
			selectedCreditID := ""
			selectedExpiresAt := time.Time{}
			for _, credit := range resetCredits.Credits {
				creditID := strings.TrimSpace(credit.ID)
				if creditID == "" || strings.ToLower(strings.TrimSpace(credit.Status)) != "available" {
					continue
				}
				expiresAt, parseErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(credit.ExpiresAt))
				if selectedCreditID == "" || (parseErr == nil && (selectedExpiresAt.IsZero() || expiresAt.Before(selectedExpiresAt))) {
					selectedCreditID = creditID
					if parseErr == nil {
						selectedExpiresAt = expiresAt
					}
				}
			}
			if selectedCreditID != "" {
				requestPayload["credit_id"] = selectedCreditID
			}
		}
	}

	requestBody, err := common.Marshal(requestPayload)
	if err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		bu+"/backend-api/wham/rate-limit-reset-credits/consume",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return 0, nil, err
	}
	setCodexWhamRequestHeaders(req, at, aid)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func setCodexWhamRequestHeaders(req *http.Request, accessToken string, accountID string) {
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("chatgpt-account-id", accountID)
	req.Header.Set("Accept", "application/json")
	if req.Header.Get("originator") == "" {
		req.Header.Set("originator", "codex_cli_rs")
	}
}
