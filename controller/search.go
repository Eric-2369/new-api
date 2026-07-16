package controller

import (
	"bytes"
	"io"
	"net/http"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/relay"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/sjson"
)

func RelaySearch(c *gin.Context) {
	channelType := common.GetContextKeyInt(c, constant.ContextKeyChannelType)
	if channelType != constant.ChannelTypeOpenAI && channelType != constant.ChannelTypeCodex {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "search endpoint only supports OpenAI and Codex channels"}})
		return
	}

	storage, err := common.GetBodyStorage(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "failed to read request body"}})
		return
	}
	body, err := storage.Bytes()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "failed to read request body"}})
		return
	}

	request := &dto.GeneralOpenAIRequest{Model: common.GetContextKeyString(c, constant.ContextKeyOriginalModel)}
	info := relaycommon.GenRelayInfoOpenAI(c, request)
	info.InitChannelMeta(c)
	if err = helper.ModelMappedHelper(c, info, request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "failed to map model"}})
		return
	}
	if info.IsModelMapped {
		body, err = sjson.SetBytes(body, "model", info.UpstreamModelName)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "failed to map model"}})
			return
		}
	}

	adaptor := relay.GetAdaptor(info.ApiType)
	if adaptor == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"message": "invalid channel type"}})
		return
	}
	adaptor.Init(info)
	info.UpstreamRequestBodySize = int64(len(body))
	result, err := adaptor.DoRequest(c, info, bytes.NewReader(body))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": gin.H{"message": "upstream request failed"}})
		return
	}
	resp, ok := result.(*http.Response)
	if !ok || resp == nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": gin.H{"message": "invalid upstream response"}})
		return
	}
	defer resp.Body.Close()

	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		c.Header("Content-Type", contentType)
	}
	c.Status(resp.StatusCode)
	_, _ = io.Copy(c.Writer, resp.Body)
}
