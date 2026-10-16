package helper

import (
	"errors"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/gin-gonic/gin"
)

// GetAndValidateAsterGateRequest reads only routing and accounting fields. New
// Claude protocol options belong to the peer; its original body stays in storage.
func GetAndValidateAsterGateRequest(c *gin.Context) (*dto.ClaudeRequest, error) {
	var input struct {
		Model             string              `json:"model"`
		Messages          []dto.ClaudeMessage `json:"messages"`
		System            any                 `json:"system"`
		Tools             any                 `json:"tools"`
		MaxTokens         *uint               `json:"max_tokens"`
		MaxTokensToSample *uint               `json:"max_tokens_to_sample"`
		Stream            *bool               `json:"stream"`
	}
	if err := common.UnmarshalBodyReusable(c, &input); err != nil {
		return nil, err
	}
	if input.Model == "" || len(input.Messages) == 0 {
		return nil, errors.New(i18n.T(c, i18n.MsgAsterGateInvalidRequest))
	}
	if ExceedsMaxTokensLimit(input.MaxTokens, input.MaxTokensToSample) {
		return nil, errors.New(i18n.T(c, i18n.MsgAsterGateMaxTokens))
	}
	return &dto.ClaudeRequest{Model: input.Model, Messages: input.Messages, System: input.System, Tools: input.Tools, MaxTokens: input.MaxTokens, MaxTokensToSample: input.MaxTokensToSample, Stream: input.Stream}, nil
}
