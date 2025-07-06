package workload

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/mattn/go-colorable"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NetworkHandler handles requests from arbitrary sources (WebSocket, HTTP, etc.).
type NetworkHandler struct {
	workloadManager     *BasicWorkloadManager
	workloadStartedChan chan<- string
	logger              *zap.Logger
}

func NewNetworkHandler(atom *zap.AtomicLevel) *NetworkHandler {
	handler := &NetworkHandler{}

	zapConfig := zap.NewDevelopmentEncoderConfig()
	zapConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zapConfig), zapcore.AddSync(colorable.NewColorableStdout()), atom)
	logger := zap.New(core, zap.Development())
	if logger == nil {
		panic("failed to create logger for workload driver")
	}

	handler.logger = logger

	return handler
}

func (w *NetworkHandler) Initialize(workloadManager *BasicWorkloadManager, workloadStartedChan chan<- string) {
	w.workloadManager = workloadManager
	w.workloadStartedChan = workloadStartedChan
}

func (w *NetworkHandler) StartWorkload(msgId string, workloadId string) (*domain.WorkloadResponse, error) {
	startedWorkload, err := w.workloadManager.StartWorkload(workloadId)
	if err != nil {
		w.logger.Error("Failed to start workload.", zap.String("workload_id", workloadId), zap.Error(err))
		return nil, err
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	// Notify the server-push goroutine that the workload has started.
	w.workloadStartedChan <- workloadId

	startedWorkload.UpdateTimeElapsed()
	respBuilder := newResponseBuilder(msgId, OpStartWorkload)
	response := respBuilder.WithModifiedWorkload(startedWorkload).BuildResponse()
	return response, nil
}

// GetWorkload returns the currently-registered workloads.
func (w *NetworkHandler) GetWorkload(msgId string, workloadId string) (*domain.WorkloadResponse, error) {
	workload, loaded := w.workloadManager.GetWorkload(workloadId)
	if !loaded {
		return nil, fmt.Errorf("%w: \"%s\"", ErrWorkloadNotFound, workloadId)
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	respBuilder := newResponseBuilder(msgId, OpGetWorkloads)
	response := respBuilder.WithModifiedWorkload(workload).BuildResponse()
	return response, nil
}

// GetWorkloads returns the currently-registered workloads.
func (w *NetworkHandler) GetWorkloads(msgId string) (*domain.WorkloadResponse, error) {
	workloads := w.workloadManager.GetWorkloads()

	if msgId == "" {
		msgId = uuid.NewString()
	}

	respBuilder := newResponseBuilder(msgId, OpGetWorkloads)
	response := respBuilder.WithModifiedWorkloads(workloads).BuildResponse()
	return response, nil
}

// ToggleDebugLogs handles a request to toggle debug logging on/off for a particular workload.
func (w *NetworkHandler) ToggleDebugLogs(msgId string, workloadId string, enable bool) (*domain.WorkloadResponse, error) {
	modifiedWorkload, err := w.workloadManager.ToggleDebugLogging(workloadId, enable)
	if err != nil {
		w.logger.Error("Failed to toggle debug logs for workload.",
			zap.String("workload_id", workloadId),
			zap.Bool("enable", enable),
			zap.Error(err))

		return nil, err
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	respBuilder := newResponseBuilder(msgId, OpWorkloadToggleDebugLogs)
	response := respBuilder.WithModifiedWorkload(modifiedWorkload).BuildResponse()
	return response, nil
}

// StopWorkload handles a request to stop a particular workload.
func (w *NetworkHandler) StopWorkload(msgId string, workloadId string) (*domain.WorkloadResponse, error) {
	stoppedWorkload, err := w.workloadManager.StopWorkload(workloadId)
	if err != nil {
		w.logger.Error("Failed to stop workload.", zap.String("workload_id", workloadId), zap.Error(err))
		return nil, err
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	stoppedWorkload.UpdateTimeElapsed()
	respBuilder := newResponseBuilder(msgId, OpStopWorkload)
	response := respBuilder.WithModifiedWorkload(stoppedWorkload).BuildResponse()
	return response, nil
}

// StopWorkloads handles a request to stop 1 or more active workloads.
//
// If one or more of the specified workloads are not stoppable (i.e., they either do not exist, or they're not actively running),
// then this will return an error. However, this will stop all valid workloads specified within the request before returning said error.
func (w *NetworkHandler) StopWorkloads(msgId string, workloadIds []string) (*domain.WorkloadResponse, error) {
	// Create a slice for all the workloads that were stopped.
	// Optimistically pre-allocate enough slots for every workload specified in the request to be successfully stopped.
	// It should usually work, as the frontend generally prevents users for submitting invalid requests.
	// It would only "go wrong" if the frontend's state is out of sync, which should be very uncommon.
	var stoppedWorkloads = make([]domain.Workload, 0, len(workloadIds))

	// Errors accumulated while stopping the workloads specified in the request.
	var errs = make([]error, 0)

	for _, workloadId := range workloadIds {
		stoppedWorkload, err := w.workloadManager.StopWorkload(workloadId)
		if err != nil {
			w.logger.Error("Failed to stop workload.", zap.String("workload_id", workloadId), zap.Error(err))
			errs = append(errs, err)
			continue
		}

		stoppedWorkload.UpdateTimeElapsed()
		stoppedWorkloads = append(stoppedWorkloads, stoppedWorkload)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	respBuilder := newResponseBuilder(msgId, OpStopWorkloads)
	response := respBuilder.WithModifiedWorkloads(stoppedWorkloads).BuildResponse()
	return response, nil
}

// PauseWorkload handles a request to pause (i.e., temporarily suspend/halt the execution of) an actively-running
// workload.
func (w *NetworkHandler) PauseWorkload(msgId string, workloadId string) (*domain.WorkloadResponse, error) {
	pausedWorkload, err := w.workloadManager.PauseWorkload(workloadId)
	if err != nil {
		w.logger.Error("Failed to pause workload.", zap.String("workload_id", workloadId), zap.Error(err))
		return nil, err
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	pausedWorkload.UpdateTimeElapsed()
	respBuilder := newResponseBuilder(msgId, OpStopWorkload)
	response := respBuilder.WithModifiedWorkload(pausedWorkload).BuildResponse()
	return response, nil
}

// UnpauseWorkload handles a request to unpause (i.e., resume the execution of) an active workload that has previously
// been paused.
func (w *NetworkHandler) UnpauseWorkload(msgId string, workloadId string) (*domain.WorkloadResponse, error) {
	unpausedWorkload, err := w.workloadManager.UnpauseWorkload(workloadId)
	if err != nil {
		w.logger.Error("Failed to unpause workload.", zap.String("workload_id", workloadId), zap.Error(err))
		return nil, err
	}

	if msgId == "" {
		msgId = uuid.NewString()
	}

	unpausedWorkload.UpdateTimeElapsed()
	respBuilder := newResponseBuilder(msgId, OpStopWorkload)
	response := respBuilder.WithModifiedWorkload(unpausedWorkload).BuildResponse()
	return response, nil
}

// handleRegisterWorkload registers a workload.
//
// handleRegisterWorkload is not exported/public because it is only meant to be used by the workload websocket.
func (w *NetworkHandler) handleRegisterWorkload(msgId string, req *domain.WorkloadRegistrationRequestWrapper, ws domain.ConcurrentWebSocket) (*domain.WorkloadResponse, error) {
	workload, err := w.workloadManager.RegisterWorkload(req.WorkloadRegistrationRequest, ws)
	if err != nil {
		w.logger.Error("Failed to register new workload.", zap.Any("workload-registration-request", req.WorkloadRegistrationRequest), zap.Error(err))
		return nil, err
	}

	respBuilder := newResponseBuilder(msgId, OpRegisterWorkloads)
	response := respBuilder.WithNewWorkload(workload).BuildResponse()
	return response, nil
}
