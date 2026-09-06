package relay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	appconstant "github.com/QuantumNous/new-api/constant"
	appdto "github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/middleware"
	appmodel "github.com/QuantumNous/new-api/model"
	perfmetrics "github.com/QuantumNous/new-api/pkg/perf_metrics"
	"github.com/QuantumNous/new-api/pkg/wsmanager"
	relaychannel "github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/tokenkit"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const responsesWSEventTypeResponseCreate = "response.create"
const responsesWSWriteTimeout = 30 * time.Second

// Bound per-connection bookkeeping without retaining prompt contents.
const responsesWSContinuationLimit = 32

type responsesWSContinuation struct {
	model       string
	inputTokens int
}

// ResponsesWSRequestRunner executes the existing authentication and rate-limit
// middleware around one complete request, without a second HTTP connection.
type ResponsesWSRequestRunner func(*http.Request, string, func(*gin.Context) *types.NewAPIError) *types.NewAPIError

type responsesWSCreateEvent struct {
	Type       string            `json:"type"`
	ResponseID string            `json:"response_id,omitempty"`
	EventID    string            `json:"event_id,omitempty"`
	StreamID   common.RawMessage `json:"stream_id,omitempty"`
	Generate   common.RawMessage `json:"generate,omitempty"`
	Request    common.RawMessage `json:"response,omitempty"`
}

type responsesWSCreateRequest struct {
	Request  dto.OpenAIResponsesRequest
	Body     []byte
	Generate common.RawMessage
	StreamID string
}

type responsesWSErrorEvent struct {
	Type       string             `json:"type"`
	Status     int                `json:"status"`
	StatusCode int                `json:"status_code,omitempty"`
	EventID    string             `json:"event_id,omitempty"`
	StreamID   string             `json:"stream_id,omitempty"`
	ResponseID string             `json:"response_id,omitempty"`
	Headers    map[string]any     `json:"headers,omitempty"`
	Error      *types.OpenAIError `json:"error"`
}

type responsesWSMessage struct {
	kind int
	body []byte
	err  error
}

// responsesWSControl is a client control event (cancel or interrupt) whose
// envelope the read loop already parsed.
type responsesWSControl struct {
	body              []byte
	eventID, streamID string
}

// Only the request worker reads or changes billing state. Socket readers pass
// bounded messages to it; cancellation never performs an independent refund.
type responsesWSCallState struct {
	inbox          chan responsesWSMessage
	controls       chan responsesWSControl
	pendingControl *responsesWSControl // Worker-owned control not yet sent upstream.
	done           chan struct{}
	terminal       *responsesWSMessage
	rejection      *responsesWSMessage
	errorHeaders   map[string]any
	// A nonzero closeCode ends the connection after this request.
	closeCode   int
	closeReason string
}

type responsesWSSession struct {
	ctx            context.Context
	cancel         context.CancelFunc
	client         *websocket.Conn
	runner         ResponsesWSRequestRunner
	request        *http.Request
	requestID      string
	nextEventIndex int
	workers        sync.WaitGroup
	closeOnce      sync.Once

	clientWriteMu    sync.Mutex
	targetWriteMu    sync.Mutex
	connectionMu     sync.Mutex
	target           *websocket.Conn
	targetDone       chan struct{}
	targetReaderDone chan struct{}
	unregister       func()
	stateMu          sync.Mutex
	current          *responsesWSCallState
	// Guarded by stateMu; published before admitting the next request.
	finishedResponseID string

	// These fields belong to the serial request worker and describe the actual
	// established connection. Per-request token/user data is never stored here.
	lastResponseID    string
	continuations     map[string]responsesWSContinuation
	continuationOrder []string
	lockedModel       string
	lockedChannelID   int
	lockedGroup       string
	lockedKey         string
	lockedKeyIndex    int
	lockedContext     map[appconstant.ContextKey]any
	lockedRoute       dto.AdvancedCustomRoute
}

func ResponsesWebSocketHelper(c *gin.Context, client *websocket.Conn, runner ResponsesWSRequestRunner) *types.NewAPIError {
	ctx, cancel := context.WithCancel(c.Request.Context())
	s := &responsesWSSession{ctx: ctx, cancel: cancel, client: client, runner: runner,
		request: c.Request.Clone(ctx), requestID: c.GetString(common.RequestIdKey)}
	if s.requestID == "" {
		s.requestID = common.NewRequestId()
	}
	maxMB := appconstant.MaxRequestBodyMB
	if maxMB <= 0 {
		maxMB = 128
	}
	client.SetReadLimit(int64(maxMB) << 20)
	defer func() {
		// Unless an earlier shutdown ended the read, the client closed or left.
		// Gorilla already answered a client close frame, so this tells the upstream.
		s.shutdown(websocket.CloseNormalClosure, "")
		s.workers.Wait()
	}()

	s.workers.Go(func() {
		ticker := time.NewTicker(helper.DefaultPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				deadline := time.Now().Add(responsesWSWriteTimeout)
				if err := client.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
					s.shutdown(websocket.CloseInternalServerErr, "keepalive failed")
					return
				}
				if target := s.getTarget(); target != nil {
					if err := target.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
						_ = target.Close()
					}
				}
			case <-s.ctx.Done():
				return
			}
		}
	})
	for {
		_, message, err := readResponsesWSMessage(client, int64(maxMB)<<20)
		if err != nil {
			return nil
		}
		envelope, streamID, err := parseResponsesWSEnvelope(message)
		eventType := envelope.Type
		if err != nil {
			s.sendError(envelope.EventID, streamID, newResponsesWSInvalidRequestError(err))
			continue
		}
		if eventType != responsesWSEventTypeResponseCreate {
			// Controls are owned by the active request too. In particular a cancel
			// arriving during authentication must not precede its upstream create.
			s.stateMu.Lock()
			state := s.current
			finishedInterrupt := eventType == "response.interrupt" && envelope.ResponseID != "" && envelope.ResponseID == s.finishedResponseID
			// Admission and finalization share this lock: a control is either
			// queued for this request or handled against its published completion.
			queued := false
			if !finishedInterrupt && state != nil && (eventType == "response.cancel" || eventType == "response.interrupt") {
				select {
				case state.controls <- responsesWSControl{body: message, eventID: envelope.EventID, streamID: streamID}:
					queued = true
				default:
				}
			}
			s.stateMu.Unlock()
			// Codex can send an interrupt while a terminal frame is in flight.
			// Acknowledge the already finished response so Codex can reuse the
			// connection after cancellation, without interrupting a newer response.
			if finishedInterrupt {
				body, err := buildResponsesWSInterruptAcknowledgement(envelope.ResponseID)
				if err != nil {
					return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
				}
				if err := s.writeClient(websocket.TextMessage, body); err != nil {
					return nil
				}
				continue
			}
			if (eventType != "response.cancel" && eventType != "response.interrupt") || state == nil {
				s.sendError(envelope.EventID, streamID, newResponsesWSInvalidRequestError(fmt.Errorf("unsupported websocket event %q", eventType)))
				continue
			}
			if !queued {
				s.sendError(envelope.EventID, streamID, newResponsesWSInvalidRequestError(errors.New("a response control event is already pending")))
			}
			continue
		}
		state := &responsesWSCallState{inbox: make(chan responsesWSMessage), controls: make(chan responsesWSControl, 1), done: make(chan struct{})}
		if !s.tryReserveCurrent(state) {
			s.sendError(envelope.EventID, streamID, types.NewErrorWithStatusCode(errors.New("another response.create is already in progress on this websocket connection"), types.ErrorCodeInvalidRequest, http.StatusConflict, types.ErrOptionWithSkipRetry()))
			continue
		}
		requestID := fmt.Sprintf("%s-ws-%d", s.requestID, s.nextEventIndex)
		s.nextEventIndex++
		s.workers.Go(func() { s.runRequest(state, message, envelope, streamID, requestID) })
	}
}

func (s *responsesWSSession) runRequest(state *responsesWSCallState, message []byte, envelope responsesWSCreateEvent, streamID string, requestID string) {
	create, parseErr := normalizeResponsesWSCreateEvent(message, envelope, streamID)
	var apiErr *types.NewAPIError
	var upstreamURLs []string
	defer func() {
		if recovered := recover(); recovered != nil {
			apiErr = types.NewError(fmt.Errorf("responses websocket request panic: %v", recovered), types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
			state.closeCode = websocket.CloseInternalServerErr
		}
		// Finish the middleware count before publishing the terminal event. Hold
		// admission while writing it so an immediate next create cannot race
		// the current request's release. A failing request also shuts down
		// before that release, so a concurrent upstream close cannot report it
		// as a normal closure; shutdown itself takes neither lock.
		outgoing := state.terminal
		if apiErr != nil && state.rejection != nil {
			outgoing = state.rejection
		} else if apiErr != nil {
			if body, err := buildResponsesWSErrorPayload(envelope.EventID, streamID, apiErr, state.errorHeaders, upstreamURLs...); err == nil {
				outgoing = &responsesWSMessage{kind: websocket.TextMessage, body: body}
			}
		}
		s.clientWriteMu.Lock()
		s.stateMu.Lock()
		if outgoing != nil {
			if err := s.client.SetWriteDeadline(time.Now().Add(responsesWSWriteTimeout)); err != nil {
				state.closeCode = websocket.CloseInternalServerErr
			} else if err := s.client.WriteMessage(outgoing.kind, outgoing.body); err != nil {
				state.closeCode = websocket.CloseInternalServerErr
			}
		}
		// The event loop may have exited before a queued control was consumed.
		// Resolve only unsent controls locally; upstream owns acknowledgements
		// for forwarded controls. Admission remains locked until this is done.
		controls := make([]responsesWSControl, 0, 2)
		if state.pendingControl != nil {
			controls = append(controls, *state.pendingControl)
		}
		select {
		case control := <-state.controls:
			controls = append(controls, control)
		default:
		}
		for _, control := range controls {
			if state.closeCode != 0 {
				break
			}
			var event responsesWSCreateEvent
			_ = common.Unmarshal(control.body, &event)
			var body []byte
			var err error
			if event.Type == "response.interrupt" && event.ResponseID != "" && event.ResponseID == s.lastResponseID {
				body, err = buildResponsesWSInterruptAcknowledgement(event.ResponseID)
			} else {
				body, err = buildResponsesWSErrorPayload(control.eventID, control.streamID, newResponsesWSInvalidRequestError(errors.New("response control target is no longer active")), nil)
			}
			if err == nil {
				err = s.client.SetWriteDeadline(time.Now().Add(responsesWSWriteTimeout))
			}
			if err == nil {
				err = s.client.WriteMessage(websocket.TextMessage, body)
			}
			if err != nil {
				state.closeCode = websocket.CloseInternalServerErr
			}
		}
		if state.closeCode != 0 {
			s.shutdown(state.closeCode, state.closeReason)
		}
		s.finishedResponseID = s.lastResponseID
		s.current = nil
		close(state.done)
		s.stateMu.Unlock()
		s.clientWriteMu.Unlock()
	}()
	request := s.request.Clone(s.ctx)
	request.Body = io.NopCloser(bytes.NewReader(message))
	request.ContentLength = int64(len(message))
	request.Header.Set("Content-Type", "application/json")
	apiErr = s.runner(request, requestID, func(c *gin.Context) *types.NewAPIError {
		if parseErr != nil {
			return newResponsesWSInvalidRequestError(parseErr)
		}
		if create.Request.Model == "" && create.Request.PreviousResponseID != "" {
			if previous, ok := s.continuations[create.Request.PreviousResponseID]; ok {
				modelName := previous.model
				create.Request.Model = modelName
				var body map[string]common.RawMessage
				if err := common.Unmarshal(create.Body, &body); err != nil {
					return newResponsesWSInvalidRequestError(err)
				}
				var err error
				body["model"], err = common.Marshal(modelName)
				if err != nil {
					return newResponsesWSInvalidRequestError(err)
				}
				create.Body, err = common.Marshal(body)
				if err != nil {
					return newResponsesWSInvalidRequestError(err)
				}
			}
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(create.Body))
		c.Request.ContentLength = int64(len(create.Body))
		callErr := s.runCall(c, state, create)
		upstreamURLs = common.UpstreamAddresses(c)
		return callErr
	})
	if apiErr != nil && (apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden) {
		state.closeCode = websocket.ClosePolicyViolation
	}
}

func (s *responsesWSSession) runCall(c *gin.Context, state *responsesWSCallState, create responsesWSCreateRequest) (apiErr *types.NewAPIError) {
	policy := service.RequestPolicy(c)
	modelName := create.Request.Model
	started := time.Now()
	var info *relaycommon.RelayInfo
	billingPrepared := false
	var payload []byte
	defer func() {
		if recovered := recover(); recovered != nil {
			apiErr = types.NewError(fmt.Errorf("responses websocket call panic: %v", recovered), types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
			state.closeCode = websocket.CloseInternalServerErr
		}
		if info == nil && modelName != "" {
			info = &relaycommon.RelayInfo{OriginModelName: modelName, UsingGroup: common.GetContextKeyString(c, appconstant.ContextKeyUsingGroup), StartTime: started}
		}
		perfmetrics.RecordRelayResult(c.Request.Context(), info, apiErr)
		// Settlement already marks the request policy successful, and nothing
		// reads a termination decision after this point on the WebSocket path,
		// so neither policy record belongs here.
		if info != nil && billingPrepared {
			apiErr = RefundFailedRequestBilling(c, info, apiErr)
		}
	}()
	if modelName == "" {
		return newResponsesWSInvalidRequestError(errors.New("model is required"))
	}
	if apiErr = checkResponsesWSModelAccess(c, modelName); apiErr != nil {
		return apiErr
	}
	if s.lockedModel != "" && modelName != s.lockedModel {
		// A model switch needs a new upstream connection, but the client session
		// stays open. Keep previous_response_id intact for upstream validation.
		s.closeTarget()
		if s.targetReaderDone != nil {
			<-s.targetReaderDone
		}
		s.lockedChannelID = 0
		s.lockedModel = ""
		s.lockedContext = nil
		s.continuations = nil
		s.continuationOrder = nil
		s.lastResponseID = ""
	}
	common.SetContextKey(c, appconstant.ContextKeyOriginalModel, modelName)
	common.SetContextKey(c, appconstant.ContextKeyRequestStartTime, time.Now())
	service.GetChannelConstraints(c).AddFilter(appdto.ChannelFilter{Kind: appdto.FilterRequestPath, RequestPath: c.Request.URL.Path})
	previous := s.continuations[create.Request.PreviousResponseID]
	cachedInputTokens := 0
	if previous.model == modelName {
		cachedInputTokens = previous.inputTokens
	}

	if s.lockedChannelID != 0 {
		if apiErr = s.restoreConnectionContext(c, modelName); apiErr != nil {
			return apiErr
		}
		info = relaycommon.GenRelayInfoResponses(c, &create.Request)
		info.IsStream = true
		common.SetContextKey(c, appconstant.ContextKeyIsStream, true)
		if apiErr = PrepareRequestBilling(c, info, cachedInputTokens); apiErr != nil {
			return apiErr
		}
		billingPrepared = true
		payload, apiErr = buildResponsesWSCreatePayload(c, info, create.Request, create.Generate, create.StreamID)
		if apiErr != nil {
			return apiErr
		}
		if err := s.writeTarget(websocket.TextMessage, payload); err != nil {
			state.closeCode = websocket.CloseInternalServerErr
			return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
		}
	} else {
		retry := &service.RetryParam{Ctx: c, TokenGroup: common.GetContextKeyString(c, appconstant.ContextKeyUsingGroup), ModelName: modelName, RequestPath: c.Request.URL.Path, Retry: common.GetPointer(0)}
		for ; retry.GetRetry() <= common.RetryTimes; retry.IncreaseRetry() {
			var channel *appmodel.Channel
			channel, apiErr = selectResponsesWSChannel(c, modelName, retry)
			if apiErr != nil {
				return apiErr
			}
			service.AppendUsedChannel(c, channel.Id)
			if info == nil {
				info = relaycommon.GenRelayInfoResponses(c, &create.Request)
				info.IsStream = true
				common.SetContextKey(c, appconstant.ContextKeyIsStream, true)
				if apiErr = PrepareRequestBilling(c, info, cachedInputTokens); apiErr != nil {
					return apiErr
				}
				billingPrepared = true
			} else {
				info.PriceData.GroupRatioInfo = helper.HandleGroupRatio(c, info)
				if apiErr = service.PrepareTieredBillingForSelectedGroup(c, info); apiErr != nil {
					return apiErr
				}
			}
			info.RetryIndex = retry.GetRetry()
			policy.BeginAttempt(channel, info.UsingGroup)
			payload, apiErr = buildResponsesWSCreatePayload(c, info, create.Request, create.Generate, create.StreamID)
			if apiErr != nil {
				return apiErr
			}
			adaptor := GetAdaptor(info.ApiType)
			adaptor.Init(info)
			target, responseHeaders, dialErr := dialResponsesWebSocketUpstream(c, adaptor, info)
			if dialErr != nil {
				apiErr = service.NormalizeViolationFeeError(types.NewError(dialErr, types.ErrorCodeDoRequestFailed))
				service.ResetStatusCode(apiErr, c.GetString("status_code_mapping"))
				info.LastError = apiErr
				decision := service.DecideRelayRetry(c, apiErr, common.RetryTimes-retry.GetRetry())
				service.RecordPolicyFailure(c, channel.Id, apiErr, decision)
				service.ProcessChannelError(c, *types.NewChannelError(channel.Id, channel.Type, channel.Name, channel.ChannelInfo.IsMultiKey, info.ApiKey, channel.GetAutoBan()), apiErr, info)
				if decision.Action == "retry" {
					continue
				}
				state.errorHeaders = responseHeaders
				return apiErr
			}
			if !s.setTarget(target) {
				return types.NewError(context.Canceled, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
			}
			if len(responseHeaders) > 0 {
				// Model and turn-state are understood by Codex as event metadata.
				// x-reasoning-included is preserved for clients that support it,
				// but current Codex still reads that flag only from its HTTP 101.
				metadata, err := common.Marshal(map[string]any{"type": "response.metadata", "headers": responseHeaders})
				if err != nil {
					return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
				}
				if err = s.writeClient(websocket.TextMessage, metadata); err != nil {
					state.closeCode = websocket.CloseInternalServerErr
					return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
				}
			}
			if err := s.writeTarget(websocket.TextMessage, payload); err != nil {
				state.closeCode = websocket.CloseInternalServerErr
				return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
			}
			s.lockedModel, s.lockedChannelID, s.lockedGroup = modelName, channel.Id, info.UsingGroup
			s.lockedKey, s.lockedKeyIndex = info.ApiKey, info.ChannelMultiKeyIndex
			s.lockedRoute, _ = channel.GetOtherSettings().AdvancedCustom.MatchPathForModel(c.Request.URL.Path, modelName)
			s.lockedContext = make(map[appconstant.ContextKey]any)
			for _, key := range []appconstant.ContextKey{
				appconstant.ContextKeyChannelId, appconstant.ContextKeyChannelName, appconstant.ContextKeyChannelType,
				appconstant.ContextKeyChannelCreateTime, appconstant.ContextKeyChannelSetting, appconstant.ContextKeyChannelOtherSetting,
				appconstant.ContextKeyChannelParamOverride, appconstant.ContextKeyChannelHeaderOverride, appconstant.ContextKeyChannelOrganization,
				appconstant.ContextKeyChannelAutoBan, appconstant.ContextKeyChannelModelMapping, appconstant.ContextKeyChannelStatusCodeMapping,
				appconstant.ContextKeyChannelIsMultiKey, appconstant.ContextKeyChannelMultiKeyIndex, appconstant.ContextKeyChannelKey, appconstant.ContextKeyChannelBaseUrl,
				appconstant.ContextKeyUpstreamRequestOrigin,
			} {
				if value, ok := c.Get(string(key)); ok {
					s.lockedContext[key] = value
				}
			}
			s.registerChannelClose(channel.Id)
			s.startTargetReader(target)
			apiErr = nil
			break
		}
		if apiErr != nil {
			return apiErr
		}
	}

	var effectiveRequest struct {
		Generate common.RawMessage `json:"generate"`
	}
	if err := common.Unmarshal(payload, &effectiveRequest); err != nil {
		return newResponsesWSInvalidRequestError(err)
	}
	warmup := bytes.Equal(bytes.TrimSpace(effectiveRequest.Generate), []byte("false"))
	accumulator := service.NewResponsesUsageAccumulator(info)
	info.StreamStatus = relaycommon.NewStreamStatus()
	info.StreamStatus.RequireTerminal()
	common.SetContextKey(c, appconstant.ContextKeyResponseStreamStatus, info.StreamStatus)
	timeout := time.Duration(appconstant.StreamingTimeout) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}
	idle := time.NewTimer(timeout)
	defer idle.Stop()
	// Request-level instructions/tools are supplied again each turn, so they
	// must not accumulate in the conversation's input estimate.
	overheadRequest := create.Request
	overheadRequest.Input = nil
	overheadTokens, estimateErr := service.EstimateRequestToken(c, overheadRequest.GetTokenCountMeta(), info)
	if estimateErr != nil {
		return types.NewError(estimateErr, types.ErrorCodeCountTokenFailed)
	}
	accepted := false
	var completedResponseID string
	retainedOutputTokens := -1
	completedOutputTokens := make(map[string]int)
	discardedPartial := false
	defer func() {
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
		if !accepted {
			if apiErr == nil {
				apiErr = types.NewErrorWithStatusCode(errors.New("upstream ended before accepting the response"), types.ErrorCodeBadResponse, http.StatusBadGateway, types.ErrOptionWithSkipRetry())
			}
			return
		}
		// Decide before Finish: ordinary interrupted streams estimate prompt usage,
		// but an empty generate:false exchange only prepared the connection.
		var contextTokens int
		if warmup {
			contextTokens = max(0, info.GetEstimatePromptTokens()-overheadTokens)
			if err := service.SettleBilling(c, info, 0); err != nil {
				apiErr = types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
			}
		} else {
			usage := accumulator.Finish()
			var clamp *common.QuotaClamp
			outputTokens := max(0, usage.CompletionTokens)
			if retainedOutputTokens >= 0 {
				outputTokens = retainedOutputTokens
			}
			contextTokens, clamp = common.QuotaFromFloatChecked(float64(max(0, max(0, usage.PromptTokens)-overheadTokens)) + float64(outputTokens))
			if clamp != nil {
				info.QuotaClamp = clamp
			}
			ConsumeResponsesQuota(c, info, usage)
		}
		if completedResponseID != "" && apiErr == nil {
			if s.continuations == nil {
				s.continuations = make(map[string]responsesWSContinuation)
			}
			s.continuationOrder = slices.DeleteFunc(s.continuationOrder, func(id string) bool { return id == completedResponseID })
			if len(s.continuationOrder) == responsesWSContinuationLimit {
				delete(s.continuations, s.continuationOrder[0])
				s.continuationOrder = s.continuationOrder[1:]
			}
			s.continuationOrder = append(s.continuationOrder, completedResponseID)
			s.continuations[completedResponseID] = responsesWSContinuation{model: modelName, inputTokens: contextTokens}
		}
	}()
	var sentControl []byte
	var responseID string
	for {
		select {
		case incoming := <-state.inbox:
			idle.Reset(timeout)
			if incoming.err != nil {
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonScannerErr, incoming.err)
				state.closeCode, state.closeReason = websocket.CloseInternalServerErr, "upstream connection closed"
				return nil
			}
			info.SetFirstResponseTime()
			var event dto.ResponsesStreamResponse
			streamID, err := decodeResponsesWSStreamEvent(incoming.body, &event)
			if err != nil {
				warmup = false
				info.StreamStatus.RecordError("invalid upstream websocket event")
			} else {
				if event.Type != "error" && streamID != "" && streamID != create.StreamID {
					if err := s.writeClient(incoming.kind, incoming.body); err != nil {
						s.shutdown(websocket.CloseInternalServerErr, "")
					}
					continue
				}
				// A repeated terminal from the previous response must never finish
				// a subsequent request on this persistent connection.
				if event.Response != nil && event.Response.ID != "" && event.Response.ID == s.lastResponseID {
					continue
				}
				if event.Type == "error" {
					var rejection responsesWSErrorEvent
					_ = common.Unmarshal(incoming.body, &rejection)
					if rejection.Status == 0 {
						rejection.Status = rejection.StatusCode
					}
					if rejection.ResponseID != "" && rejection.ResponseID == s.lastResponseID {
						continue
					}
					terminal, ambiguous, controlError := responsesWSErrorEndsRequest(rejection, create.StreamID, responseID, sentControl)
					if !terminal {
						if err := s.writeClient(incoming.kind, incoming.body); err != nil {
							s.shutdown(websocket.CloseInternalServerErr, "")
						}
						// Only a control error in this stream resolves its pending control.
						if controlError {
							sentControl = nil
						}
						continue
					}
					if rejection.Status < 400 || rejection.Status > 599 {
						rejection.Status = http.StatusBadRequest
					}
					var errorBody map[string]common.RawMessage
					if err := common.Unmarshal(incoming.body, &errorBody); err != nil {
						return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
					}
					if rejection.Error == nil {
						rejection.Error = &types.OpenAIError{Type: "invalid_request_error", Message: event.Message, Code: event.Code}
						errorBody["error"], err = common.Marshal(rejection.Error)
						if err != nil {
							return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
						}
					}
					errorBody["status"], err = common.Marshal(rejection.Status)
					if err != nil {
						return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
					}
					incoming.body, err = common.Marshal(errorBody)
					if err != nil {
						return types.NewError(err, types.ErrorCodeBadResponse, types.ErrOptionWithSkipRetry())
					}
					if event.Code == "previous_response_not_found" || (rejection.Error != nil && rejection.Error.Code == "previous_response_not_found") {
						delete(s.continuations, create.Request.PreviousResponseID)
					}
					if accepted {
						var reported struct {
							Response struct {
								Usage any `json:"usage"`
							} `json:"response"`
						}
						_ = common.Unmarshal(incoming.body, &reported)
						if responsesWSHasUsage(reported.Response.Usage) {
							warmup = false
						}
						if rejection.Error != nil {
							code := ""
							if rejection.Error.Code != nil {
								code = fmt.Sprint(rejection.Error.Code)
							}
							info.StreamStatus.MarkFailed(code, rejection.Error.Type, rejection.Status)
						}
						accumulator.Observe(&event, incoming.body)
						info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonDone, nil)
						s.lastResponseID = responseID
						state.terminal = &incoming
						if ambiguous {
							state.closeCode = websocket.CloseInternalServerErr
						}

						return nil
					}
					rejected := types.WithOpenAIError(*rejection.Error, rejection.Status)
					if rejected.StatusCode < 400 || rejected.StatusCode > 599 {
						rejected.StatusCode = http.StatusBadRequest
					}
					// The create was already sent: audit/disable as appropriate,
					// but never replay this request on another connection.
					service.RecordPolicyFailure(c, s.lockedChannelID, rejected, service.PolicyDecision{Action: "stop", Reason: "non_retryable_error", Source: "system"})
					service.ProcessChannelError(c, *types.NewChannelError(info.ChannelId, info.ChannelType, common.GetContextKeyString(c, appconstant.ContextKeyChannelName), info.ChannelIsMultiKey, info.ApiKey, common.GetContextKeyBool(c, appconstant.ContextKeyChannelAutoBan)), rejected, info)
					state.rejection = &incoming
					return rejected
				}
				if warmup {
					var reported struct {
						Response struct {
							Usage any `json:"usage"`
						} `json:"response"`
					}
					_ = common.Unmarshal(incoming.body, &reported)
					if event.Item != nil || event.Delta != "" || strings.HasPrefix(event.Type, "response.output_") || strings.HasPrefix(event.Type, "response.reasoning") ||
						(event.Response != nil && len(event.Response.Output) > 0) || responsesWSHasUsage(reported.Response.Usage) {
						warmup = false
					}
				}
				generationEvent := event.Item != nil || event.Delta != ""
				switch event.Type {
				case "response.created", "response.in_progress", "response.completed", "response.done", "response.incomplete", "response.failed", "response.cancelled", "response.canceled":
					generationEvent = true
				}
				if generationEvent {
					if !accepted {
						// Like HTTP, bind the session only once upstream accepted the request.
						service.RecordChannelAffinity(c, s.lockedChannelID)
					}
					accepted = true
					if event.Response != nil && event.Response.ID != "" {
						responseID = event.Response.ID
					}
				}
				// Keep only counts, not item contents, for a terminal that omits
				// output. Interrupted items must never enter the next prompt.
				itemKey := event.ItemID
				if event.Item != nil && event.Item.ID != "" {
					itemKey = event.Item.ID
				}
				if itemKey == "" && event.OutputIndex != nil {
					itemKey = fmt.Sprintf("index:%d", *event.OutputIndex)
				}
				if appconstant.CountToken && event.Type == "response.output_item.done" && event.Item != nil {
					completedOutputTokens[itemKey] = responsesWSRetainedOutputTokens(info.GetUpstreamModelName(), []dto.ResponsesOutput{*event.Item})
				} else if event.Type == "response.output_item.interrupted" {
					discardedPartial = true
					delete(completedOutputTokens, itemKey)
				}
				if event.Type == "response.incomplete" && event.Response != nil && event.Response.IncompleteDetails != nil && event.Response.IncompleteDetails.Reason == "interrupted" {
					// Interrupt usage includes generated but discarded partial items.
					// Charge that usage this turn; estimate the next turn from the
					// retained output only. An explicit empty output is authoritative.
					var control struct {
						Mode string `json:"mode"`
					}
					_ = common.Unmarshal(sentControl, &control)
					if appconstant.CountToken && (discardedPartial || control.Mode == "discard_partial_items") {
						retainedOutputTokens = 0
						if event.Response.Output != nil {
							retainedOutputTokens = responsesWSRetainedOutputTokens(info.GetUpstreamModelName(), event.Response.Output)
						} else {
							for _, tokens := range completedOutputTokens {
								var clamp *common.QuotaClamp
								retainedOutputTokens, clamp = common.QuotaFromFloatChecked(float64(retainedOutputTokens) + float64(tokens))
								if clamp != nil {
									info.QuotaClamp = clamp
								}
							}
						}
					}
				}
				accumulator.Observe(&event, incoming.body)
			}
			switch event.Type {
			case "response.completed", "response.done", "response.incomplete", "response.failed", "response.cancelled", "response.canceled":
				if event.Response != nil {
					s.lastResponseID = event.Response.ID
					if !info.StreamStatus.ResponseFailed() && (event.Type == "response.completed" || event.Type == "response.done" || event.Type == "response.incomplete") {
						completedResponseID = event.Response.ID
					}
				}
				info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonDone, nil)
				state.terminal = &incoming
				return nil
			}
			if err := s.writeClient(incoming.kind, incoming.body); err != nil {
				s.shutdown(websocket.CloseInternalServerErr, "")
			}
			if accepted && state.pendingControl != nil {
				if err := s.writeTarget(websocket.TextMessage, state.pendingControl.body); err != nil {
					s.shutdown(websocket.CloseInternalServerErr, "")
				}
				sentControl = state.pendingControl.body
				state.pendingControl = nil
			}
		case control := <-state.controls:
			if state.pendingControl != nil || sentControl != nil {
				s.sendError(control.eventID, control.streamID, newResponsesWSInvalidRequestError(errors.New("a response control event is already pending")))
				continue
			}
			if !accepted {
				state.pendingControl = &control
				continue
			}
			if err := s.writeTarget(websocket.TextMessage, control.body); err != nil {
				s.shutdown(websocket.CloseInternalServerErr, "")
			}
			sentControl = control.body
		case <-idle.C:
			info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonTimeout, context.DeadlineExceeded)
			state.closeCode, state.closeReason = websocket.CloseInternalServerErr, "upstream response timed out"
			return nil
		case <-s.ctx.Done():
			info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonClientGone, s.ctx.Err())
			return nil
		}
	}
}

// decodeResponsesWSStreamEvent decodes accounting and lifecycle facts while
// treating the echoed reasoning configuration as opaque. Codex can use numeric
// effort, unlike the HTTP DTO. The original frame is always forwarded unchanged.
func decodeResponsesWSStreamEvent(payload []byte, event *dto.ResponsesStreamResponse) (string, error) {
	wire := struct {
		*dto.ResponsesStreamResponse
		StreamID string            `json:"stream_id"`
		Response common.RawMessage `json:"response"`
	}{ResponsesStreamResponse: event}
	if err := common.Unmarshal(payload, &wire); err != nil {
		return wire.StreamID, err
	}
	if len(wire.Response) == 0 || bytes.Equal(bytes.TrimSpace(wire.Response), []byte("null")) {
		return wire.StreamID, nil
	}
	event.Response = &dto.OpenAIResponsesResponse{}
	response := struct {
		*dto.OpenAIResponsesResponse
		Reasoning common.RawMessage `json:"reasoning"`
	}{OpenAIResponsesResponse: event.Response}
	return wire.StreamID, common.Unmarshal(wire.Response, &response)
}

// A single generation is active, but a cancel can fail independently. Correlate
// available IDs first; uncorrelated control/server errors require a bounded
// connection close because continuing could leave the generation stuck forever.
func responsesWSErrorEndsRequest(event responsesWSErrorEvent, streamID, responseID string, control []byte) (terminal, ambiguous, controlError bool) {
	if len(control) > 0 {
		var pending struct {
			EventID    string `json:"event_id"`
			StreamID   string `json:"stream_id"`
			ResponseID string `json:"response_id"`
		}
		_ = common.Unmarshal(control, &pending)
		if event.EventID != "" && event.EventID == pending.EventID {
			return false, false, true
		}
		if event.ResponseID != "" && event.ResponseID == pending.ResponseID && event.ResponseID != responseID {
			return false, false, true
		}
		if (event.StreamID == "" || event.StreamID == pending.StreamID) && event.Error != nil && event.Error.Type == "invalid_request_error" {
			switch event.Error.Code {
			case "response_not_found", "response_not_active", "response_already_completed":
				return false, false, true
			}
		}
	}
	if event.StreamID != "" && event.StreamID != streamID || responseID != "" && event.ResponseID != "" && event.ResponseID != responseID {
		return false, false, false
	}
	return true, len(control) > 0 && event.ResponseID == "", false
}

func (s *responsesWSSession) restoreConnectionContext(c *gin.Context, model string) *types.NewAPIError {
	// The channel type is not rechecked: a locked channel keeps its type, and
	// the FilterResponsesWebSocket rules were applied when it was selected.
	channel, err := appmodel.CacheGetChannel(s.lockedChannelID)
	if err != nil || channel == nil || channel.Status != common.ChannelStatusEnabled || !channel.GetSetting().ResponsesWebSocketEnabled {
		return types.NewErrorWithStatusCode(errors.New("Responses WebSocket is disabled for this channel"), types.ErrorCode(appdto.FilterResponsesWebSocket), http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	keyEnabled := channel.Key == s.lockedKey
	if channel.ChannelInfo.IsMultiKey {
		keys := channel.GetKeys()
		status := channel.ChannelInfo.MultiKeyStatusList[s.lockedKeyIndex]
		keyEnabled = s.lockedKeyIndex >= 0 && s.lockedKeyIndex < len(keys) && keys[s.lockedKeyIndex] == s.lockedKey && (status == 0 || status == common.ChannelStatusEnabled)
	}
	if !keyEnabled {
		return types.NewErrorWithStatusCode(errors.New("the upstream connection credential is no longer enabled"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	// Changes that alter the physical upstream connection require a new
	// handshake. Request-level settings can be refreshed without rotating keys.
	if channel.GetBaseURL() != s.lockedContext[appconstant.ContextKeyChannelBaseUrl] ||
		!reflect.DeepEqual(channel.GetHeaderOverride(), s.lockedContext[appconstant.ContextKeyChannelHeaderOverride]) {
		return types.NewErrorWithStatusCode(errors.New("upstream connection settings changed; reconnect required"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	if previous, ok := s.lockedContext[appconstant.ContextKeyChannelSetting].(dto.ChannelSettings); ok && previous.Proxy != channel.GetSetting().Proxy {
		return types.NewErrorWithStatusCode(errors.New("upstream proxy changed; reconnect required"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	// An advanced custom connection was dialed through the route matched for
	// this model. Only the fields that shape the upstream handshake force a
	// reconnect: target path, protocol conversion and credential placement.
	// Other channel types match the zero route on both sides.
	route, _ := channel.GetOtherSettings().AdvancedCustom.MatchPathForModel(c.Request.URL.Path, model)
	if strings.TrimSpace(route.UpstreamPath) != strings.TrimSpace(s.lockedRoute.UpstreamPath) ||
		route.IsNative() != s.lockedRoute.IsNative() ||
		!reflect.DeepEqual(route.Auth, s.lockedRoute.Auth) {
		return types.NewErrorWithStatusCode(errors.New("upstream route changed; reconnect required"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	pin, pinned, _ := service.GetChannelConstraints(c).ResolvedPin()
	if pinned {
		if pin.ChannelId != s.lockedChannelID {
			return types.NewErrorWithStatusCode(errors.New("channel pin changed; reconnect required"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
		}
	} else {
		group := common.GetContextKeyString(c, appconstant.ContextKeyUsingGroup)
		if group == "auto" {
			if !slices.Contains(service.GetRequestAutoGroups(c, common.GetContextKeyString(c, appconstant.ContextKeyUserGroup)), s.lockedGroup) {
				return types.NewErrorWithStatusCode(errors.New("the connection group is no longer allowed"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
			}
			group = s.lockedGroup
			common.SetContextKey(c, appconstant.ContextKeyAutoGroup, group)
		}
		if !appmodel.IsChannelEnabledForGroupModel(group, model, s.lockedChannelID) {
			return types.NewErrorWithStatusCode(errors.New("the connection channel is no longer allowed for this group and model"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
		}
	}
	for key, value := range s.lockedContext {
		c.Set(string(key), value)
	}
	common.SetContextKey(c, appconstant.ContextKeyChannelSetting, channel.GetSetting())
	common.SetContextKey(c, appconstant.ContextKeyChannelOtherSetting, channel.GetOtherSettings())
	service.MarkResponsesWSConnectionReused(c, s.lockedGroup, s.lockedChannelID)
	// Reevaluate this request's rule without selecting a different connection.
	// Use the original token group (including auto), as initial selection does.
	if !pinned {
		boundChannel, found := service.GetPreferredChannelByAffinity(c, model, common.GetContextKeyString(c, appconstant.ContextKeyUsingGroup))
		if found && service.RequestPolicy(c).SessionMode == "strict" && boundChannel != s.lockedChannelID {
			return types.NewErrorWithStatusCode(errors.New("strict session binding changed; reconnect required"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
		}
	}
	paramOverride := channel.GetParamOverride()
	if merged, applied := service.ApplyChannelAffinityOverrideTemplate(c, paramOverride); applied {
		paramOverride = merged
	}
	common.SetContextKey(c, appconstant.ContextKeyChannelParamOverride, paramOverride)
	common.SetContextKey(c, appconstant.ContextKeyChannelModelMapping, channel.GetModelMapping())
	common.SetContextKey(c, appconstant.ContextKeyChannelStatusCodeMapping, channel.GetStatusCodeMapping())
	policy := service.RequestPolicy(c)
	policy.BeginAttempt(channel, s.lockedGroup)
	policy.AddEvent(service.PolicyEvent{ChannelID: channel.Id, Decision: service.PolicyDecision{Action: "select", Reason: "pinned_channel", Source: "channel_constraint"}})
	return nil
}

func buildResponsesWSCreatePayload(c *gin.Context, info *relaycommon.RelayInfo, req dto.OpenAIResponsesRequest, generate common.RawMessage, streamID string) ([]byte, *types.NewAPIError) {
	info.InitChannelMeta(c)
	adaptor := GetAdaptor(info.ApiType)
	if adaptor == nil {
		return nil, types.NewError(fmt.Errorf("invalid api type: %d", info.ApiType), types.ErrorCodeInvalidApiType)
	}
	adaptor.Init(info)
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeReadRequestBodyFailed, types.ErrOptionWithSkipRetry())
	}
	jsonData, err := io.ReadAll(common.NewReplayableBodyReader(storage))
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeReadRequestBodyFailed, types.ErrOptionWithSkipRetry())
	}
	event, err := buildResponsesWSCreateEvent(jsonData, generate, streamID)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	converted, apiErr := prepareResponsesWebSocketPayload(c, info, adaptor, &req, event)
	if apiErr != nil {
		return nil, apiErr
	}
	event, err = buildResponsesWSCreateEvent(converted, nil, streamID)
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	return event, nil
}

func (s *responsesWSSession) startTargetReader(target *websocket.Conn) {
	limit := int64(helper.DefaultMaxScannerBufferSize)
	if appconstant.StreamScannerMaxBufferMB > 0 {
		limit = int64(appconstant.StreamScannerMaxBufferMB) << 20
	}
	target.SetReadLimit(limit)
	s.connectionMu.Lock()
	done := s.targetDone
	s.connectionMu.Unlock()
	readerDone := make(chan struct{})
	s.targetReaderDone = readerDone
	s.workers.Go(func() {
		defer close(readerDone)
		for {
			kind, body, err := readResponsesWSMessage(target, limit)
			select {
			case <-done:
				return
			default:
			}
			incoming := responsesWSMessage{kind: kind, body: body, err: err}
			if state := s.getCurrent(); state != nil {
				select {
				case state.inbox <- incoming:
					if err != nil {
						return
					}
					continue
				case <-done:
					return
				case <-state.done:
				case <-s.ctx.Done():
					return
				}
			}
			// No request owns this message. An idle upstream that closed
			// normally ends the client session with the same code.
			if err != nil {
				code := websocket.CloseInternalServerErr
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					code = err.(*websocket.CloseError).Code
				}
				s.shutdown(code, "upstream connection closed")
				return
			}
			if writeErr := s.writeClient(kind, body); writeErr != nil {
				s.shutdown(websocket.CloseInternalServerErr, "")
				return
			}
		}
	})
}

func (s *responsesWSSession) getCurrent() *responsesWSCallState {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.current
}

func (s *responsesWSSession) tryReserveCurrent(state *responsesWSCallState) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.current != nil {
		return false
	}
	s.current = state
	return true
}

func (s *responsesWSSession) getTarget() *websocket.Conn {
	s.connectionMu.Lock()
	defer s.connectionMu.Unlock()
	return s.target
}

func (s *responsesWSSession) setTarget(target *websocket.Conn) bool {
	s.connectionMu.Lock()
	defer s.connectionMu.Unlock()
	if s.ctx.Err() != nil {
		_ = target.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = target.Close()
		return false
	}
	s.target = target
	s.targetDone = make(chan struct{})
	return true
}

func (s *responsesWSSession) writeTarget(kind int, message []byte) error {
	s.targetWriteMu.Lock()
	defer s.targetWriteMu.Unlock()
	target := s.getTarget()
	if target == nil {
		return errors.New("responses websocket upstream is not connected")
	}
	if err := target.SetWriteDeadline(time.Now().Add(responsesWSWriteTimeout)); err != nil {
		return err
	}
	return target.WriteMessage(kind, message)
}

func (s *responsesWSSession) writeClient(kind int, message []byte) error {
	s.clientWriteMu.Lock()
	defer s.clientWriteMu.Unlock()
	if err := s.client.SetWriteDeadline(time.Now().Add(responsesWSWriteTimeout)); err != nil {
		return err
	}
	return s.client.WriteMessage(kind, message)
}

func (s *responsesWSSession) sendError(eventID, streamID string, apiErr *types.NewAPIError) {
	if apiErr == nil {
		return
	}
	payload, err := buildResponsesWSErrorPayload(eventID, streamID, apiErr, nil)
	if err == nil {
		_ = s.writeClient(websocket.TextMessage, payload)
	}
}

func (s *responsesWSSession) closeTarget() {
	s.connectionMu.Lock()
	target, unregister := s.target, s.unregister
	s.target, s.unregister = nil, nil
	if s.targetDone != nil {
		close(s.targetDone)
		s.targetDone = nil
	}
	s.connectionMu.Unlock()
	if unregister != nil {
		unregister()
	}
	if target != nil {
		_ = target.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "model changed"), time.Now().Add(time.Second))
		_ = target.Close()
	}
}

// shutdown ends the connection once. Each peer gets a close frame that says
// why, bounded to one second, and both sockets close even if a write failed.
// Close frames are control writes, which gorilla allows alongside a blocked
// data writer, so shutdown takes no write lock and never waits for workers.
func (s *responsesWSSession) shutdown(code int, reason string) {
	s.closeOnce.Do(func() {
		s.cancel()
		s.connectionMu.Lock()
		target, unregister := s.target, s.unregister
		s.target, s.unregister = nil, nil
		s.connectionMu.Unlock()
		if unregister != nil {
			unregister()
		}
		closeMessage := websocket.FormatCloseMessage(code, reason)
		_ = s.client.WriteControl(websocket.CloseMessage, closeMessage, time.Now().Add(time.Second))
		if target != nil {
			_ = target.WriteControl(websocket.CloseMessage, closeMessage, time.Now().Add(time.Second))
			_ = target.Close()
		}
		_ = s.client.Close()
	})
}

func (s *responsesWSSession) registerChannelClose(channelID int) {
	unregister := wsmanager.Register(channelID, wsmanager.KindResponses, s.closeForPolicy)
	s.connectionMu.Lock()
	if s.ctx.Err() != nil {
		s.connectionMu.Unlock()
		unregister()
		return
	}
	s.unregister = unregister
	s.connectionMu.Unlock()
}

// closeForPolicy tells both peers why the connection ends, like the realtime
// relay does.
func (s *responsesWSSession) closeForPolicy(reason string) {
	s.shutdown(websocket.ClosePolicyViolation, reason)
}

// Stream identity belongs to the WebSocket envelope. For the legacy wrapped
// input, the top-level field takes precedence over response.stream_id.
func parseResponsesWSEnvelope(message []byte) (responsesWSCreateEvent, string, error) {
	var event responsesWSCreateEvent
	if err := common.Unmarshal(message, &event); err != nil {
		return event, "", fmt.Errorf("invalid websocket event json: %w", err)
	}
	streamRaw := event.StreamID
	if len(streamRaw) == 0 && len(event.Request) > 0 {
		var wrapped struct {
			StreamID common.RawMessage `json:"stream_id"`
		}
		if err := common.Unmarshal(event.Request, &wrapped); err == nil {
			streamRaw = wrapped.StreamID
		}
	}
	var streamID string
	if len(streamRaw) > 0 {
		if err := common.Unmarshal(streamRaw, &streamID); err != nil || len(streamID) < 1 || len(streamID) > 256 {
			return event, "", errors.New("stream_id must contain 1-256 ASCII letters, digits, underscores, hyphens, or periods")
		}
		for _, char := range streamID {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.') {
				return event, "", errors.New("stream_id must contain only ASCII letters, digits, underscores, hyphens, or periods")
			}
		}
	}
	if strings.TrimSpace(event.Type) == "" {
		return event, streamID, errors.New("websocket event type is required")
	}
	return event, streamID, nil
}

func newResponsesWSInvalidRequestError(err error) *types.NewAPIError {
	return types.NewErrorWithStatusCode(err, types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
}

// normalizeResponsesWSCreateEvent builds the HTTP-shaped request body from a
// response.create whose envelope the read loop already parsed and validated.
// The message is decoded once more as a raw map (the body source) and once as
// the typed request; the envelope is not parsed again.
func normalizeResponsesWSCreateEvent(message []byte, event responsesWSCreateEvent, streamID string) (responsesWSCreateRequest, error) {
	create := responsesWSCreateRequest{StreamID: streamID, Generate: event.Generate}
	// Decode the wrapped request on its own so outer fields cannot enter the HTTP body.
	source := message
	if len(event.Request) > 0 {
		source = event.Request
	}
	var raw map[string]common.RawMessage
	if err := common.Unmarshal(source, &raw); err != nil {
		return create, err
	}
	if len(event.Request) > 0 {
		if len(create.Generate) == 0 {
			create.Generate = raw["generate"]
		}
	} else {
		delete(raw, "type")
	}
	delete(raw, "generate")
	delete(raw, "stream_id")
	payload, err := common.Marshal(raw)
	if err != nil {
		return create, err
	}
	if err := decodeResponsesWSRequest(payload, &create.Request); err != nil {
		return create, err
	}
	if helper.ExceedsMaxTokensLimit(create.Request.MaxOutputTokens) {
		return create, errors.New("max_output_tokens is invalid")
	}
	create.Body = payload
	return create, nil
}

func buildResponsesWSCreateEvent(jsonData []byte, generate common.RawMessage, streamID string) ([]byte, error) {
	var event map[string]common.RawMessage
	if err := common.Unmarshal(jsonData, &event); err != nil {
		return nil, err
	}
	typeData, err := common.Marshal(responsesWSEventTypeResponseCreate)
	if err != nil {
		return nil, err
	}
	event["type"] = typeData
	// The normalized body never carries stream_id; a channel parameter
	// override may inject one, and it must not shadow the envelope's value.
	delete(event, "stream_id")
	if streamID != "" {
		streamData, err := common.Marshal(streamID)
		if err != nil {
			return nil, err
		}
		event["stream_id"] = streamData
	}
	if len(generate) > 0 {
		event["generate"] = generate
	}
	return common.Marshal(event)
}

func buildResponsesWSInterruptAcknowledgement(responseID string) ([]byte, error) {
	return common.Marshal(map[string]string{
		"type":        "response.interrupt.accepted",
		"response_id": responseID,
	})
}

func buildResponsesWSErrorPayload(eventID, streamID string, apiErr *types.NewAPIError, headers map[string]any, upstreamURLs ...string) ([]byte, error) {
	if apiErr == nil {
		return nil, errors.New("api error is nil")
	}
	status := apiErr.StatusCode
	if status == 0 {
		status = http.StatusInternalServerError
	}
	openaiErr := apiErr.ToOpenAIError(upstreamURLs...)
	return common.Marshal(&responsesWSErrorEvent{
		Type:     "error",
		Status:   status,
		EventID:  eventID,
		StreamID: streamID,
		Error:    &openaiErr,
		Headers:  headers,
	})
}

// checkResponsesWSModelAccess applies the token model limit with the same name
// matching as the HTTP distributor. Unlike HTTP, it also runs for requests
// pinned to a channel: the check is stricter there on purpose, because a
// persistent connection keeps serving the model after the pin was resolved.
func checkResponsesWSModelAccess(c *gin.Context, modelName string) *types.NewAPIError {
	if !common.GetContextKeyBool(c, appconstant.ContextKeyTokenModelLimitEnabled) {
		return nil
	}
	raw, ok := common.GetContextKey(c, appconstant.ContextKeyTokenModelLimit)
	if !ok {
		return types.NewErrorWithStatusCode(errors.New("token has no model access"), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	tokenModelLimit, ok := raw.(map[string]bool)
	if !ok {
		tokenModelLimit = map[string]bool{}
	}
	if !middleware.TokenModelLimitAllows(tokenModelLimit, modelName) {
		return types.NewErrorWithStatusCode(fmt.Errorf("token is not allowed to use model %s", modelName), types.ErrorCodeAccessDenied, http.StatusForbidden, types.ErrOptionWithSkipRetry())
	}
	return nil
}

// selectResponsesWSChannel narrows the shared HTTP selection to channels that
// speak the Responses WebSocket protocol and renders its outcome as a
// non-retryable NewAPIError.
func selectResponsesWSChannel(c *gin.Context, modelName string, retryParam *service.RetryParam) (*appmodel.Channel, *types.NewAPIError) {
	constraints := service.GetChannelConstraints(c)
	if !slices.ContainsFunc(constraints.Filters, func(filter appdto.ChannelFilter) bool {
		return filter.Kind == appdto.FilterResponsesWebSocket
	}) {
		constraints.AddFilter(appdto.ChannelFilter{Kind: appdto.FilterResponsesWebSocket})
	}
	channel, _, selectErr := service.SelectChannelForRequest(c, modelName, retryParam)
	if selectErr != nil {
		message := selectErr.Message
		if selectErr.MessageID != "" {
			message = i18n.T(c, selectErr.MessageID, selectErr.Params)
		}
		code := selectErr.Code
		if code == "" {
			code = types.ErrorCodeGetChannelFailed
		}
		return nil, types.NewErrorWithStatusCode(errors.New(message), code, selectErr.StatusCode, types.ErrOptionWithSkipRetry())
	}
	if err := middleware.SetupContextForSelectedChannel(c, channel, modelName); err != nil {
		return nil, err
	}
	return channel, nil
}

func dialResponsesWebSocketUpstream(c *gin.Context, adaptor relaychannel.Adaptor, info *relaycommon.RelayInfo) (*websocket.Conn, map[string]any, *types.NewAPIError) {
	target, resp, err := relaychannel.DoResponsesWebSocketRequest(adaptor, c, info)
	if err == nil {
		var headers map[string]any
		if resp != nil {
			for _, name := range []string{"openai-model", "x-codex-turn-state", "x-reasoning-included"} {
				values, present := resp.Header[http.CanonicalHeaderKey(name)]
				if present && len(values) > 0 {
					if headers == nil {
						headers = make(map[string]any)
					}
					headers[name] = values[0]
				}
			}
		}
		return target, headers, nil
	}
	var headers map[string]any
	if resp != nil {
		// Forward only recovery hints, never arbitrary upstream credentials or cookies.
		for _, name := range []string{"retry-after", "retry-after-ms"} {
			if value := resp.Header.Get(name); value != "" {
				if headers == nil {
					headers = make(map[string]any)
				}
				headers[name] = value
			}
		}
		if resp.Body != nil {
			return nil, headers, service.RelayErrorHandler(c.Request.Context(), resp, false)
		}
	}
	return nil, headers, types.NewErrorWithStatusCode(err, types.ErrorCodeDoRequestFailed, http.StatusBadGateway)
}

// Gorilla's frame limit applies before decompression. Bound the decoded message
// as well, before JSON parsing or allocating an entire oversized message.
func readResponsesWSMessage(conn *websocket.Conn, limit int64) (int, []byte, error) {
	kind, reader, err := conn.NextReader()
	if err != nil {
		return kind, nil, err
	}
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if int64(len(body)) > limit {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "message too large"), time.Now().Add(time.Second))
		return kind, nil, websocket.ErrReadLimit
	}
	return kind, body, err
}

// responsesWSRetainedOutputTokens estimates the visible items retained after an
// interrupt. Opaque reasoning cannot be reconstructed from billed output usage:
// that usage also includes discarded items. Use the ordinary input estimator's
// text view, with visible reasoning summaries, rather than charging discarded text
// again as input. The current response's actual usage is settled independently.
func responsesWSRetainedOutputTokens(model string, output []dto.ResponsesOutput) int {
	retained := make([]dto.ResponsesOutput, 0, len(output))
	var reasoning strings.Builder
	for _, item := range output {
		if item.Status == "incomplete" || item.Status == "in_progress" {
			continue
		}
		retained = append(retained, item)
		for _, summary := range item.Summary {
			reasoning.WriteString(summary.Text)
			reasoning.WriteByte('\n')
		}
	}
	body, err := common.Marshal(retained)
	if err != nil {
		return 0
	}
	request := dto.OpenAIResponsesRequest{Input: body}
	text := request.GetTokenCountMeta().CombineText
	return tokenkit.Count(model, text+reasoning.String())
}
