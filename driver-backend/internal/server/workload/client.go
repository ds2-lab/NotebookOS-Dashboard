package workload

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/mattn/go-colorable"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto_utilities"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/clock"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/event_queue"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/metrics"
	"github.com/scusemua/workload-driver-react/m/v2/pkg/jupyter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/util/wait"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	zmq  requestSource = "ZMQ"
	gRPC requestSource = "gRPC"
)

var (
	// ErrClientNotRunning  = errors.New("cannot stop client as it is not running")

	ErrInvalidFirstEvent = errors.New("client received invalid first event")

	errShutdownClient = errors.New("client should terminate")

	SmrLeadTask jupyter.MessageType = "smr_lead_task"
)

type requestSource string

func (s requestSource) String() string {
	return string(s)
}

// ClientBuilder constructs a Client instance step-by-step.
type ClientBuilder struct {
	sessionId                 string
	workloadId                string
	sessionReadyEvent         *domain.Event
	startingTick              time.Time
	atom                      *zap.AtomicLevel
	targetTickDurationSeconds int64
	timescaleAdjustmentFactor float64
	errorChan                 chan<- error
	session                   *domain.WorkloadTemplateSession
	workload                  *Workload
	kernelSessionManager      jupyter.KernelSessionManager
	notifyCallback            func(notification *proto.Notification)
	schedulingPolicy          string
	waitGroup                 *sync.WaitGroup
	fileOutputPath            string
	assignedModel             string // assignedModel is the name of the model to be assigned to the client.
	assignedDataset           string // assignedDataset is the name of the dataset to be assigned to the client.
	modelDatasetCategory      string // modelDatasetCategory is the category of the assignedModel and assignedDataset.

	// maxSleepDuringInitSec is the maximum amount of time that the Client should sleep for during exponential
	// backoff when it is first being created.
	maxSleepDuringInitSec int

	// dropSessionsWithNoTrainingEvents is a flag that, when true, will cause the Client to return immediately
	// if it finds it has no training events.
	dropSessionsWithNoTrainingEvents bool

	// maxCreationAttempts is the maximum number of times the Client will attempt to create its kernel
	// before giving up.
	maxCreationAttempts int

	// isKernelTrainingCallback is used to query whether our kernel is actively training as far as the
	// cluster gateway knows.
	isKernelTrainingCallback IsKernelTrainingCallback

	// getJupyterMessageCallback returns the configured scheduling policy along with a flag indicating whether the
	// returned policy name is valid.
	getJupyterMessageCallback GetJupyterMessageCallback
}

// NewClientBuilder initializes a new ClientBuilder.
func NewClientBuilder() *ClientBuilder {
	return &ClientBuilder{
		maxSleepDuringInitSec:     120, // Default
		timescaleAdjustmentFactor: 1.0, // Default
	}
}

// WithMaxInitializationSleepIntervalSeconds is used to set the value of the maxSleepDuringInitSec field.
// The maxSleepDuringInitSec field is the maximum amount of time that the Client should sleep for during
// exponential backoff when it is first being created.
func (b *ClientBuilder) WithMaxInitializationSleepIntervalSeconds(intervalSec int) *ClientBuilder {
	b.maxSleepDuringInitSec = intervalSec
	return b
}

func (b *ClientBuilder) WithIsKernelTrainingCallback(callback IsKernelTrainingCallback) *ClientBuilder {
	b.isKernelTrainingCallback = callback
	return b
}

func (b *ClientBuilder) WithGetJupyterMessageCallback(callback GetJupyterMessageCallback) *ClientBuilder {
	b.getJupyterMessageCallback = callback
	return b
}

func (b *ClientBuilder) WithDeepLearningModel(model string) *ClientBuilder {
	b.assignedModel = model
	return b
}

func (b *ClientBuilder) WithDataset(dataset string) *ClientBuilder {
	b.assignedDataset = dataset
	return b
}

func (b *ClientBuilder) WithModelDatasetCategory(modelDatasetCategory string) *ClientBuilder {
	b.modelDatasetCategory = modelDatasetCategory
	return b
}

// WithFileOutput will instruct the Client [that is to be built] to also output its logs to a file (at the specified
// path) in addition to outputting its logs to the console/terminal (stdout).
func (b *ClientBuilder) WithFileOutput(path string) *ClientBuilder {
	b.fileOutputPath = path
	return b
}

func (b *ClientBuilder) WithSessionId(sessionId string) *ClientBuilder {
	b.sessionId = sessionId
	return b
}

func (b *ClientBuilder) WithWorkloadId(workloadId string) *ClientBuilder {
	b.workloadId = workloadId
	return b
}

func (b *ClientBuilder) WithSessionReadyEvent(event *domain.Event) *ClientBuilder {
	b.sessionReadyEvent = event
	return b
}

func (b *ClientBuilder) WithStartingTick(startingTick time.Time) *ClientBuilder {
	b.startingTick = startingTick
	return b
}

func (b *ClientBuilder) WithAtom(atom *zap.AtomicLevel) *ClientBuilder {
	b.atom = atom
	return b
}

func (b *ClientBuilder) WithSchedulingPolicy(schedulingPolicy string) *ClientBuilder {
	if schedulingPolicy == "" {
		panic("Cannot use the empty string as a scheduling policy when creating a Client")
	}

	b.schedulingPolicy = schedulingPolicy
	return b
}

func (b *ClientBuilder) WithKernelManager(kernelSessionManager jupyter.KernelSessionManager) *ClientBuilder {
	b.kernelSessionManager = kernelSessionManager
	return b
}

func (b *ClientBuilder) WithTimescaleAdjustmentFactor(timescaleAdjustmentFactor float64) *ClientBuilder {
	b.timescaleAdjustmentFactor = timescaleAdjustmentFactor
	return b
}

func (b *ClientBuilder) WithTargetTickDurationSeconds(seconds int64) *ClientBuilder {
	b.targetTickDurationSeconds = seconds
	return b
}

func (b *ClientBuilder) WithErrorChan(errorChan chan<- error) *ClientBuilder {
	b.errorChan = errorChan
	return b
}

func (b *ClientBuilder) WithWorkload(workload *Workload) *ClientBuilder {
	b.workload = workload
	return b
}

func (b *ClientBuilder) WithSession(session *domain.WorkloadTemplateSession) *ClientBuilder {
	b.session = session
	return b
}

func (b *ClientBuilder) WithNotifyCallback(notifyCallback func(notification *proto.Notification)) *ClientBuilder {
	b.notifyCallback = notifyCallback
	return b
}

func (b *ClientBuilder) WithDropSessionsWithNoTrainingEvents(shouldDrop bool) *ClientBuilder {
	b.dropSessionsWithNoTrainingEvents = shouldDrop
	return b
}

func (b *ClientBuilder) WithWaitGroup(waitGroup *sync.WaitGroup) *ClientBuilder {
	b.waitGroup = waitGroup
	return b
}

// WithMaxCreationAttempts allows specification of the ClientBuilder's maxCreationAttempts parameter, which is the
// maximum number of times the Client will attempt to create its kernel before giving up.
func (b *ClientBuilder) WithMaxCreationAttempts(maxCreationAttempts int) *ClientBuilder {
	b.maxCreationAttempts = maxCreationAttempts
	return b
}

// Build constructs the Client instance.
func (b *ClientBuilder) Build() *Client {
	sessionMeta := b.sessionReadyEvent.Data.(domain.SessionMetadata)

	if b.maxCreationAttempts <= 0 {
		b.maxCreationAttempts = 3
	}

	client := &Client{
		SessionId:  b.sessionId,
		EventQueue: event_queue.NewSessionEventQueue(b.sessionId),
		WorkloadId: b.workloadId,
		Workload:   b.workload,
		maximumResourceSpec: &domain.ResourceRequest{
			Cpus:     sessionMeta.GetMaxSessionCPUs(),
			MemoryMB: sessionMeta.GetMaxSessionMemory(),
			Gpus:     sessionMeta.GetMaxSessionGPUs(),
			VRAM:     sessionMeta.GetMaxSessionVRAM(),
		},
		currentTick:                      clock.NewSimulationClockFromTime(b.startingTick),
		currentTime:                      clock.NewSimulationClockFromTime(b.startingTick),
		targetTickDurationSeconds:        b.targetTickDurationSeconds,
		timescaleAdjustmentFactor:        b.timescaleAdjustmentFactor,
		targetTickDuration:               time.Second * time.Duration(b.targetTickDurationSeconds),
		clockTrigger:                     clock.NewTrigger(),
		errorChan:                        b.errorChan,
		TrainingStartedChannel:           make(chan interface{}, 1),
		TrainingStoppedChannel:           make(chan interface{}, 1),
		trainingEndedRequestMap:          make(map[string]*atomic.Int32),
		trainingStartedRequestMap:        make(map[string]*atomic.Int32),
		Session:                          b.session,
		kernelSessionManager:             b.kernelSessionManager,
		sessionReadyEvent:                b.sessionReadyEvent,
		notifyCallback:                   b.notifyCallback,
		waitGroup:                        b.waitGroup,
		getJupyterMessageCallback:        b.getJupyterMessageCallback,
		isKernelTrainingCallback:         b.isKernelTrainingCallback,
		schedulingPolicy:                 b.schedulingPolicy,
		AssignedModel:                    b.assignedModel,
		AssignedDataset:                  b.assignedDataset,
		ModelDatasetCategory:             b.modelDatasetCategory,
		maxSleepDuringInitSec:            b.maxSleepDuringInitSec,
		dropSessionsWithNoTrainingEvents: b.dropSessionsWithNoTrainingEvents,
		MaxCreationAttempts:              b.maxCreationAttempts,
		lastTrainingSubmittedAt:          time.UnixMilli(0),
	}

	zapEncoderConfig := zap.NewDevelopmentEncoderConfig()
	zapEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(zapEncoderConfig), zapcore.AddSync(colorable.NewColorableStdout()), b.atom)

	var core zapcore.Core
	if b.fileOutputPath == "" {
		core = zapcore.NewTee(consoleCore)
	} else {
		//logFile, err := os.Create(b.fileOutputPath)
		//if err != nil {
		//	panic(err)
		//}

		writer, closeFile, err := zap.Open(b.fileOutputPath)
		if err != nil {
			panic(err)
		}

		zapFileEncoderConfig := zap.NewDevelopmentEncoderConfig()
		zapFileEncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		fileCore := zapcore.NewCore(zapcore.NewJSONEncoder(zapFileEncoderConfig), writer, b.atom)
		core = zapcore.NewTee(consoleCore, fileCore)

		// Save a reference to the closeFunc in the Client's closeLogFileFunc field so that
		// we can close the log file explicitly when stopping the client.
		client.closeLogFileFunc = closeFile
	}

	logger := zap.New(core, zap.Development())
	if logger == nil {
		panic(fmt.Sprintf("failed to create logger for client %s", b.sessionId))
	}
	client.logger = logger

	client.ticker = client.clockTrigger.NewSyncTicker(time.Second*time.Duration(client.targetTickDurationSeconds), fmt.Sprintf("Client-%s", client.SessionId), client.currentTick)

	client.EventQueue.Push(b.sessionReadyEvent)

	// Assign the model and dataset to the session.
	// They may already be assigned; that's fine.
	client.Session.AssignedModel = b.assignedModel
	client.Session.AssignedDataset = b.assignedDataset
	client.Session.ModelDatasetCategory = b.modelDatasetCategory

	return client
}

// IsKernelTrainingCallback is used to query whether the Cluster Gateway believes that a particular kernel is
// actively training or not
type IsKernelTrainingCallback func(kernelId string) (bool, error)

// GetJupyterMessageCallback returns the configured scheduling policy along with a flag indicating whether the returned
// policy name is valid.
type GetJupyterMessageCallback func(kernelId string, messageId string, messageType string) (*proto.GetJupyterMessageResponse, error)

// trainingStoppedNotification is sent over the TrainingStoppedChannel of a Client when it receives an "execute_reply".
type trainingStoppedNotification struct {
	// Response is the "execute_reply" that the Client received, informing it that the training had
	// completed successfully.
	Response jupyter.KernelMessage

	// ReceivedAt is the time at which the Client received the "execute_reply" message.
	ReceivedAt time.Time
}

// Client encapsulates a Session and runs as a dedicated goroutine, processing events for that Session.
type Client struct {
	Workload *Workload
	Session  *domain.WorkloadTemplateSession

	maxSleepDuringInitSec            int                                    // maxSleepDuringInitSec is the maximum amount of time that the Client should sleep for during exponential backoff when it is first being created.
	SessionId                        string                                 // SessionId is the Jupyter kernel/session ID of this Client
	WorkloadId                       string                                 // WorkloadId is the ID of the workload that the Client is a part of.
	errorChan                        chan<- error                           // errorChan is used to notify the WorkloadDriver that an error has occurred.
	kernelConnection                 jupyter.KernelConnection               // kernelConnection is the Client's Jupyter kernel connection. The Client uses this to send messages to its kernel.
	sessionConnection                *jupyter.SessionConnection             // sessionConnection is the Client's Jupyter session connection.
	kernelSessionManager             jupyter.KernelSessionManager           // kernelSessionManager is used by the Client to create its sessionConnection and subsequently its kernelConnection.
	sessionReadyEvent                *domain.Event                          // sessionReadyEvent is the "session-ready" event that triggered the creation of this Client.
	schedulingPolicy                 string                                 // schedulingPolicy is the name of the scheduling policy that the cluster is configured to use.
	EventQueue                       *event_queue.SessionEventQueue         // EventQueue contains events to be processed by this Client.
	maximumResourceSpec              *domain.ResourceRequest                // maximumResourceSpec is the maximum amount of resources this Client may use at any point in its lifetime.
	targetTickDurationSeconds        int64                                  // targetTickDurationSeconds is how long each tick was in the trace data used to generate this workload
	targetTickDuration               time.Duration                          // targetTickDuration is how long each tick is supposed to last. This is the tick interval/step rate of the simulation.
	timescaleAdjustmentFactor        float64                                // timescaleAdjustmentFactor controls the amount/degree of time compression that is used/applied.
	currentTick                      domain.SimulationClock                 // currentTick maintains the time for this Client.
	currentTime                      domain.SimulationClock                 // currentTime contains the current clock time of the workload, which will be sometime between currentTick and currentTick + TickDuration.
	ticker                           *clock.Ticker                          // ticker delivers ticks, which drives this Client's workload. Each time a tick is received, the Client will process events for that tick.
	clockTrigger                     *clock.Trigger                         // clockTrigger is a trigger for the clock ticks
	logger                           *zap.Logger                            // logger is how the Client prints log messages.
	running                          atomic.Int32                           // running indicates whether this Client is actively processing events.
	ticksHandled                     atomic.Int64                           // ticksHandled is the number of ticks handled by this Client.
	failedToStart                    atomic.Bool                            // failedToStart indicates that this Client completely failed to start -- it never succeeded in creating its kernel/session.
	numSessionStartAttempts          atomic.Int32                           // numSessionStartAttempts counts the number of attempts that were required when initially creating the session/kernel before the session/kernel was successfully created.
	trainingEventsHandled            atomic.Int32                           // trainingEventsHandled is the number of training events successfully processed by this Client.
	trainingEventsDelayed            atomic.Int32                           // trainingEventsDelayed returns the number of times that a training event was delayed after failing to start. The same training event can be delayed multiple times, and each of those delays is counted independently.
	lastTrainingSubmittedAt          time.Time                              // lastTrainingSubmittedAt is the real-world clock time at which the last training was submitted to the kernel.
	TrainingStartedChannel           chan interface{}                       // TrainingStartedChannel is used to notify that the last/current training has started.
	TrainingStoppedChannel           chan interface{}                       // TrainingStoppedChannel is used to notify that the last/current training has ended.
	notifyCallback                   func(notification *proto.Notification) // notifyCallback is used to send notifications directly to the frontend.
	waitGroup                        *sync.WaitGroup                        // waitGroup is used to alert the WorkloadDriver that the Client has finished.
	AssignedModel                    string                                 // AssignedModel is the name of the model assigned to this client.
	AssignedDataset                  string                                 // AssignedDataset is the name of the dataset assigned to this client.
	ModelDatasetCategory             string                                 // ModelDatasetCategory is the category of the AssignedModel and AssignedDataset.
	explicitlyStopped                atomic.Int32                           // ExplicitlyStopped is used to signal to the client that it should stop. Setting this to a value > 0 will instruct the client to stop.
	closeLogFileFunc                 func()                                 // closeLogFileFunc is returned by zap.Open when we create a Client that is supposed to also output its logs to a file. The closeFile function can be used to close the log file.
	dropSessionsWithNoTrainingEvents bool                                   // dropSessionsWithNoTrainingEvents is a flag that, when true, will cause the Client to return immediately if it finds it has no training events.
	MaxCreationAttempts              int                                    // MaxCreationAttempts is the maximum number of times the Client will attempt to create its kernel before giving up.
	handledStopEvent                 atomic.Bool                            // handledStopEvent is set to true when the client handles the 'session-stopped' event for its session.
	terminatedEarly                  atomic.Bool                            // terminatedEarly indicates that the Client exited early because one of its requests timed out.
	isKernelTrainingCallback         IsKernelTrainingCallback               // isKernelTrainingCallback is used to query whether our kernel is actively training as far as the cluster gateway knows.
	getJupyterMessageCallback        GetJupyterMessageCallback              // getJupyterMessageCallback returns the configured scheduling policy along with a flag indicating whether the returned policy name is valid.
	outstandingExecuteRequestId      string                                 // outstandingExecuteRequestId is the jupyter message ID of the last outstanding jupyter "execute_request" message -- that is, this field is set to the empty string once the response is received.
	lastSubmittedExecuteRequestId    string                                 // lastSubmittedExecuteRequestId is the jupyter message ID of the last jupyter "execute_request" message.
	trainingEndedRequestMap          map[string]*atomic.Int32               // trainingEndedRequestMap provides an atomic way to keep track of if we've received a particular "execute_reply" message or not.
	trainingEndedRequestMapMutex     sync.Mutex                             // trainingEndedRequestMapMutex provides atomic access when getting or setting values from/in the trainingEndedRequestMap field.
	trainingStartedRequestMap        map[string]*atomic.Int32               // trainingStartedRequestMap provides an atomic way to keep track of if we've received a particular "smr_lead_task" message or not.
	trainingStartedRequestMapMutex   sync.Mutex                             // trainingStartedRequestMapMutex provides atomic access when getting or setting values from/in the trainingStartedRequestMap field.
}

func (c *Client) closeLogFile() error {
	if c.closeLogFileFunc == nil {
		return fmt.Errorf("cannot close log file; no log file open")
	}

	c.closeLogFileFunc()
	return nil
}

// TotalNumTrainings returns the total number of training events that will ultimately be processed by
// the kernel associated with this Client.
func (c *Client) TotalNumTrainings() int {
	return len(c.Session.TrainingEvents)
}

// FailedToStart returns a bool that, when true, indicates that this Client completely failed to start.
// That is, the Client never succeeded in creating its kernel/session.
func (c *Client) FailedToStart() bool {
	return c.failedToStart.Load()
}

// NumSessionStartAttempts returns the number of attempts that were required when initially
// creating the session/kernel before the session/kernel was successfully created.
func (c *Client) NumSessionStartAttempts() int32 {
	return c.numSessionStartAttempts.Load()
}

// TrainingEventsDelayed returns the number of times that a training event was delayed after failing to start.
// The same training event can be delayed multiple times, and each of those delays is counted independently.
func (c *Client) TrainingEventsDelayed() int32 {
	return c.trainingEventsDelayed.Load()
}

// TrainingEventsHandled returns the number of training events successfully processed by this Client.
func (c *Client) TrainingEventsHandled() int32 {
	return c.trainingEventsHandled.Load()
}

// TicksHandled returns the number of ticks that the target Client has handled.
func (c *Client) TicksHandled() int64 {
	return c.ticksHandled.Load()
}

// Run starts the Client and instructs the Client to begin processing its events in a loop.
func (c *Client) Run() {
	if !c.running.CompareAndSwap(0, 1) {
		c.logger.Warn("Client is already running.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId))
		return
	}

	if len(c.Session.TrainingEvents) == 0 && c.dropSessionsWithNoTrainingEvents {
		c.logger.Warn("Session has 0 trainings. Exiting.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId))

		_ = c.Stop()
		return
	}

	initStart := time.Now()
	numAttempts, err := c.initialize()
	if err != nil {
		c.logger.Error("Failed to initialize client.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId),
			zap.Error(err))

		_ = c.Stop()
		return
	}

	timeElapsed := time.Since(initStart)
	c.logger.Debug("Successfully initialized client.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.WorkloadId),
		zap.Duration("time_elapsed", timeElapsed))

	c.Workload.SessionCreated(c.SessionId)
	c.Workload.ProcessedEvent(domain.NewEmptyWorkloadEvent().
		WithEventId(c.sessionReadyEvent.Id()).
		WithSessionId(c.sessionReadyEvent.SessionID()).
		WithEventName(c.sessionReadyEvent.Name).
		WithEventTimestamp(c.sessionReadyEvent.Timestamp).
		WithNumberOfTimesEnqueued(int32(numAttempts)).
		WithProcessedAtTime(time.Now()).
		WithMetadata("duration_milliseconds", timeElapsed.Milliseconds()).
		WithError(err))

	time.Sleep((time.Second * 5) + (time.Second * time.Duration(rand.Int31n(5))))

	var wg sync.WaitGroup
	wg.Add(2)

	go c.issueClockTicks(&wg)
	c.run(&wg)

	wg.Wait()

	// Do some clean-up.
	c.stop()
}

// Stop explicitly and forcibly stops the client.
func (c *Client) Stop() error {
	c.logger.Warn("Explicitly instructed to stop.")

	// Signal to the client to stop.
	c.explicitlyStopped.Store(1)

	// Wait briefly.
	time.Sleep(time.Millisecond * 100)

	c.stop()

	return nil
}

// stop performs the stopping logic for the Client without setting explicitlyStopped to 1 like Stop does.
// Specifically, stop flushes any buffered logs before closing the log file. Finally, stop sets running to 0.
func (c *Client) stop() {
	// Flush any buffered logs.
	_ = c.logger.Core().Sync()

	// Close the log file.
	_ = c.closeLogFile()

	c.running.Store(0)

	if c.waitGroup != nil {
		c.waitGroup.Done()
	}
}

func (c *Client) getInitialResourceRequest() *jupyter.ResourceSpec {
	var initialResourceRequest *jupyter.ResourceSpec

	if c.schedulingPolicy == "static" || c.schedulingPolicy == "dynamic-v3" || c.schedulingPolicy == "dynamic-v4" || c.schedulingPolicy == "fcfs-batch" || c.schedulingPolicy == "middle-ground" || c.schedulingPolicy == "gandiva" {
		// Try to get the first training event of the session, and just reserve those resources.
		firstTrainingEvent := c.Session.TrainingEvents[0]

		if firstTrainingEvent != nil {
			initialResourceRequest = &jupyter.ResourceSpec{
				Cpu:  int(firstTrainingEvent.Millicpus),
				Mem:  firstTrainingEvent.MemUsageMB,
				Gpu:  firstTrainingEvent.NumGPUs(),
				Vram: firstTrainingEvent.VRamUsageGB,
			}
		} else {
			c.logger.Warn("Could not find first training event of session.",
				zap.String("workload_id", c.WorkloadId),
				zap.String("session_id", c.SessionId))
		}
	}

	if initialResourceRequest == nil {
		initialResourceRequest = &jupyter.ResourceSpec{
			Cpu:  int(c.maximumResourceSpec.Cpus),
			Mem:  c.maximumResourceSpec.MemoryMB,
			Gpu:  c.maximumResourceSpec.Gpus,
			Vram: c.maximumResourceSpec.VRAM,
		}
	}

	return initialResourceRequest
}

// createKernel attempts to create the kernel for the Client, possibly handling any errors that are encountered
// if the errors are something we can deal with. If not, they're returned, and the workload explodes.
func (c *Client) createKernel(evt *domain.Event) (*jupyter.SessionConnection, error) {
	initialResourceRequest := c.getInitialResourceRequest()

	initialDuration := time.Millisecond * time.Duration(math.Min(float64(c.maxSleepDuringInitSec*1000)*0.10, 20_000))
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   1.5,
		Jitter:   1.125,
		Steps:    10,
		Cap:      time.Second * time.Duration(c.maxSleepDuringInitSec),
	}

	var (
		sessionConnection *jupyter.SessionConnection
		err               error
	)

	// Keep looping as long as the standard conditions for "continuing to do whatever it is the Client is doing" are
	// true and while we've not yet successfully created and established the Jupyter session connection and have not
	// yet run out of attempts to do so.
	for sessionConnection == nil && backoff.Steps > 0 && c.shouldContinue() {
		c.logger.Debug("Issuing create-session request now.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId),
			zap.String("resource_request", initialResourceRequest.String()))
		c.numSessionStartAttempts.Add(1)
		sessionConnection, err = c.kernelSessionManager.CreateSession(
			c.SessionId, fmt.Sprintf("%s.ipynb", c.SessionId),
			"notebook", "distributed", initialResourceRequest)
		if err != nil {
			c.logger.Warn("Failed to create session/kernel.",
				zap.String("workload_id", c.WorkloadId),
				zap.String("session_id", c.SessionId),
				zap.Error(err))

			if strings.Contains(err.Error(), "insufficient hosts available") {
				sleepInterval := backoff.Step()

				c.logger.Warn("Failed to create session due to insufficient hosts available. Will requeue event and try again later.",
					zap.String("workload_id", c.Workload.GetId()),
					zap.String("workload_name", c.Workload.WorkloadName()),
					zap.String("session_id", c.SessionId),
					zap.Time("original_timestamp", evt.OriginalTimestamp),
					zap.Time("current_timestamp", evt.Timestamp),
					zap.Time("current_tick", c.currentTick.GetClockTime()),
					zap.Int32("num_times_enqueued", evt.GetNumTimesEnqueued()),
					zap.Duration("total_delay", evt.TotalDelay()),
					zap.Int("attempt_number", 10-backoff.Steps+1),
					zap.Duration("sleep_interval", sleepInterval))

				// TODO: How to accurately compute the delay here? Since we're using ticks, so one minute is the
				// 		 minimum meaningful delay, really, but we're also using big time compression factors?
				c.incurDelay(sleepInterval)

				time.Sleep(sleepInterval)
				continue
			}

			// Will return nil and a non-nil error.
			return nil, err
		}
	}

	if sessionConnection != nil {
		c.logger.Debug("Successfully created kernel.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId),
			zap.Int32("num_attempts_required", c.numSessionStartAttempts.Load()))

		c.logger.Debug(fmt.Sprintf("Handled \"%s\" event.", domain.ColorizeText("session-started", domain.LightBlue)),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId))
	}

	return sessionConnection, err
}

type parsedIoPubMessage struct {
	Stream string
	Text   string
}

// initialize creates the associated kernel and connects to it.
//
// initialize returns the number of times the event was attempted before succeeding/failing entirely.
func (c *Client) initialize() (int, error) {
	evt := c.EventQueue.Pop()

	if evt.Name != domain.EventSessionReady {
		c.logger.Error("Received unexpected event for first event.",
			zap.String("session_id", c.SessionId),
			zap.String("event_name", evt.Name.String()),
			zap.String("event", evt.String()),
			zap.String("workload_id", c.WorkloadId))

		return 0, fmt.Errorf("%w: \"%s\"", ErrInvalidFirstEvent, evt.Name.String())
	}

	c.logger.Debug("Initializing client.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.WorkloadId))

	initialDuration := time.Millisecond * time.Duration(math.Min(float64(c.maxSleepDuringInitSec*1000)*0.10, 30_000))
	maximumNumberOfAttempts := 5
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   1.25,
		Jitter:   1.25,
		Steps:    maximumNumberOfAttempts,
		Cap:      time.Second * time.Duration(float64(c.maxSleepDuringInitSec)),
	}

	var (
		sessionConnection *jupyter.SessionConnection
		err               error
	)

	// There are two layers of retrying for creating the session. The first level is here while the second level is
	// found within the createKernel method.
	//
	// The delay/backoff interval in this outer retry loop is significantly longer than the inner retry loop
	// found within createKernel.
	//
	// Keep looping as long as the standard conditions for "continuing to do whatever it is the Client is doing" are
	// true and while we've not yet successfully created and established the Jupyter session connection and have not
	// yet run out of attempts to do so.
	for sessionConnection == nil && backoff.Steps >= 0 && c.shouldContinue() {
		sessionConnection, err = c.createKernel(evt)

		// If the session connection was created successfully, then exit the loop.
		if sessionConnection != nil && err == nil {
			break
		}

		// Get the next sleep interval.
		sleepInterval := backoff.Step()

		c.logger.Warn("Failed to create kernel/session.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId),
			zap.Duration("sleep_interval", sleepInterval),
			zap.Int("attempt_number", maximumNumberOfAttempts-backoff.Steps+1),
			zap.Error(err))

		time.Sleep(sleepInterval)
	}

	// If we just failed completely to create the session, then we'll return, and this session won't be included in the workload.
	if sessionConnection == nil || err != nil {
		c.logger.Error("Completely failed to create kernel.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId),
			zap.Error(err))
		c.failedToStart.Store(true)
		return maximumNumberOfAttempts - backoff.Steps + 1, err
	}

	c.sessionConnection = sessionConnection

	// ioPubHandler is a session-specific wrapper around the standard Driver::HandleIOPubMessage method.
	// This returns true if the received IOPub message is a "stream" message and is parsed successfully.
	// Otherwise, this returns false.
	//
	// The return value is not really used.
	ioPubHandler := func(conn jupyter.KernelConnection, kernelMessage jupyter.KernelMessage) interface{} {
		// Parse the IOPub message.
		// If it is a stream message, this will return a *parsedIoPubMessage variable.
		parsedIoPubMsgVal := c.HandleIOPubMessage(kernelMessage)

		if parsedIoPubMsg, ok := parsedIoPubMsgVal.(*parsedIoPubMessage); ok {
			switch parsedIoPubMsg.Stream {
			case "stdout":
				{
					c.Session.AddStdoutIoPubMessage(parsedIoPubMsg.Text)
				}
			case "stderr":
				{
					c.Session.AddStderrIoPubMessage(parsedIoPubMsg.Text)
				}
			default:
				c.logger.Warn("Unexpected stream specified by IOPub message.",
					zap.String("workload_id", c.Workload.GetId()),
					zap.String("workload_name", c.Workload.WorkloadName()),
					zap.String("session_id", c.SessionId),
					zap.String("stream", parsedIoPubMsg.Stream))
				return false
			}
			return true
		}

		return false
	}

	if err = sessionConnection.RegisterIoPubHandler(c.SessionId, ioPubHandler); err != nil {
		c.logger.Error("Failed to register IOPub message handler.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId), zap.Error(err))
		return maximumNumberOfAttempts - backoff.Steps + 1, err
	}

	c.sessionConnection = sessionConnection

	return maximumNumberOfAttempts - backoff.Steps + 1, nil
}

// issueClockTicks issues clock ticks for this Client, driving this Client's execution.
//
// issueClockTicks should be executed in its own goroutine.
func (c *Client) issueClockTicks(wg *sync.WaitGroup) {
	defer wg.Done()

	c.logger.Debug("Client is preparing to begin incrementing client-level ticker.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.WorkloadId))

	// Keep issuing clock ticks as long as the standard conditions for "continuing to do whatever it is the Client is
	// doing" are true.
	for c.shouldContinue() {
		tickStart := time.Now()

		// Increment the clock.
		tick, err := c.currentTick.IncrementClockBy(c.targetTickDuration)
		if err != nil {
			c.logger.Error("Error while incrementing clock time.",
				zap.Duration("tick-duration", c.targetTickDuration),
				zap.String("workload_id", c.WorkloadId),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.Error(err))
			c.errorChan <- err
			break
		}

		//c.logger.Debug("Client incremented client-level ticker. Triggering events now.",
		//	zap.String("session_id", c.SessionId),
		//	zap.String("workload_id", c.WorkloadId),
		//	zap.Time("tick", tick))
		c.clockTrigger.Trigger(tick)

		tickElapsedBase := time.Since(tickStart)
		tickRemaining := time.Duration(c.timescaleAdjustmentFactor * float64(c.targetTickDuration-tickElapsedBase))
		if tickRemaining > 0 {
			time.Sleep(tickRemaining)
		}
	}

	c.logger.Debug("Client has finished issuing clock ticks.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.WorkloadId),
		zap.String("workload_state", c.Workload.GetState().String()),
		zap.Int32("explicitly_stopped", c.explicitlyStopped.Load()),
		zap.Bool("handled_stop_event", c.handledStopEvent.Load()),
		zap.Time("final_tick", c.currentTick.GetClockTime()))

	if !c.handledStopEvent.Load() {
		err := c.Session.SetState(domain.SessionClientExited)
		if err != nil {
			c.logger.Error("Failed to set state to SessionClientExited.",
				zap.String("session_id", c.SessionId),
				zap.String("session_state", c.Session.GetState().String()),
				zap.String("workload_id", c.WorkloadId),
				zap.String("workload_state", c.Workload.GetState().String()),
				zap.Int32("explicitly_stopped", c.explicitlyStopped.Load()),
				zap.Bool("handled_stop_event", c.handledStopEvent.Load()),
				zap.Time("final_tick", c.currentTick.GetClockTime()),
				zap.Error(err))
		}
	}
}

// run is the private, core implementation of Run.
func (c *Client) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		c.logger.Debug("Client is done running.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId))

		if swapped := c.running.CompareAndSwap(1, 0); !swapped {
			c.logger.Error("Running was not set to 1.",
				zap.String("session_id", c.SessionId),
				zap.String("workload_id", c.WorkloadId))
		}
	}()

	// Keep handling clock ticks as long as the standard conditions for "continuing to do whatever it is the Client is
	// doing" are true.
	for c.shouldContinue() {
		select {
		case tick := <-c.ticker.TickDelivery:
			err := c.handleTick(tick)

			if err != nil {
				c.errorChan <- err
				return
			}
		}
	}
}

// shouldContinue generically returns a flag indicating whether the Client should keep doing whatever its doing --
// typically executing some sort of loop.
//
// This is used while initializing the client, while issuing clock ticks, while looping over the events to handle for
// a particular tick, etc.
//
// In general, a Client should keep doing whatever it's doing as long as the following conditions are true:
// - The workload hasn't been aborted entirely
// - This client hasn't handled its 'stop' event, which naturally (and non-erroneously) stops the client
// - This client hasn't been explicitly told to stop
// - This client didn't make the decision itself to abort early due to timing out while handling an event
func (c *Client) shouldContinue() bool {
	return c.Workload.IsInProgress() && c.explicitlyStopped.Load() <= 0 && !c.handledStopEvent.Load() && !c.terminatedEarly.Load()
}

func (c *Client) handleTick(tick time.Time) error {
	c.Session.CurrentTickNumber = c.convertTimestampToTickNumber(tick)

	_, _, err := c.currentTick.IncreaseClockTimeTo(tick)
	if err != nil {
		c.logger.Error("Failed to increase CurrentTick.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.WorkloadId),
			zap.Time("target_time", tick),
			zap.Error(err))
		return err
	}

	// If there are no events processed this tick, then we still need to increment the clock time so we're in-line with the simulation.
	// Check if the current clock time is earlier than the start of the previous tick. If so, increment the clock time to the beginning of the tick.
	prevTickStart := tick.Add(-c.targetTickDuration)
	if c.currentTime.GetClockTime().Before(prevTickStart) {
		if _, _, err = c.incrementClockTime(prevTickStart); err != nil {
			c.logger.Error("Failed to increase CurrentTime.",
				zap.String("session_id", c.SessionId),
				zap.String("workload_id", c.WorkloadId),
				zap.Time("target_time", tick),
				zap.Error(err))
			return err
		}
	}

	err = c.processEventsForTick(tick)
	if err != nil {
		return err
	}

	c.ticksHandled.Add(1)
	c.ticker.Done()
	return nil
}

// incrementClockTime sets the c.clockTime clock to the given timestamp, verifying that the new timestamp is either
// equal to or occurs after the old one.
//
// incrementClockTime returns a tuple where the first element is the new time, and the second element is the difference
// between the new time and the old time.
func (c *Client) incrementClockTime(time time.Time) (time.Time, time.Duration, error) {
	newTime, timeDifference, err := c.currentTime.IncreaseClockTimeTo(time)

	if err != nil {
		c.logger.Error("Critical error occurred when attempting to increase clock time.", zap.Error(err))
	}

	return newTime, timeDifference, err // err will be nil if everything was OK.
}

// processEventsForTick processes events in chronological/simulation order.
// This accepts the "current tick" as an argument. The current tick is basically an upper-bound on the times for
// which we'll process an event. For example, if `tick` is 19:05:00, then we will process all cluster and session
// events with timestamps that are (a) equal to 19:05:00 or (b) come before 19:05:00. Any events with timestamps
// that come after 19:05:00 will not be processed until the next tick.
func (c *Client) processEventsForTick(tick time.Time) error {
	numEventsProcessed := 0

	// Keep looping as long as the standard conditions for "continuing to do whatever it is the Client is doing" are
	// true and there are more events to process for the current tick.
	for c.EventQueue.HasEventsForTick(tick) && c.shouldContinue() {
		event := c.EventQueue.Pop()

		c.logger.Debug("Handling workload event.",
			zap.String("event_name", event.Name.String()),
			zap.String("session", c.SessionId),
			zap.String("event_name", event.Name.String()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("workload_id", c.Workload.GetId()),
			zap.Time("tick", tick))

		err := c.handleEvent(event, tick)

		if errors.Is(err, errShutdownClient) {
			c.logger.Warn("Received errShutdownClient. Client will terminate early.",
				zap.String("event_name", event.Name.String()),
				zap.String("session", c.SessionId),
				zap.String("event_name", event.Name.String()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.String("workload_id", c.Workload.GetId()),
				zap.Time("tick", tick))

			c.stopSession()

			c.terminatedEarly.Store(true)

			return nil
		}

		if err != nil {
			workloadWillAbort := c.handleError(event, err, tick)
			if workloadWillAbort {
				return err
			}
		}

		numEventsProcessed += 1
	}

	if numEventsProcessed > 0 {
		c.logger.Debug("Finished processing events for tick.",
			zap.String("session", c.SessionId),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("workload_id", c.Workload.GetId()),
			zap.Int("num_events_processed", numEventsProcessed),
			zap.Time("tick", tick))
	}

	return nil
}

// handleError is called by processEventsForTick when handleEvent returns an error.
//
// handleError returns true if the workload is going to be aborted (due to the error being irrecoverable) and
// false if the workload will continue.
func (c *Client) handleError(event *domain.Event, err error, tick time.Time) bool {
	c.logger.Error("Failed to handle event workload event.",
		zap.String("event_name", event.Name.String()),
		zap.String("session", c.SessionId),
		zap.String("event_name", event.Name.String()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("workload_id", c.Workload.GetId()),
		zap.Time("tick", tick),
		zap.Error(err))

	// In these cases, we'll just discard the events and continue.
	if errors.Is(err, domain.ErrUnknownSession) || errors.Is(err, ErrUnknownEventType) {
		return false
	}

	return true
}

// handleEvent handles a single *domain.Event.
func (c *Client) handleEvent(event *domain.Event, tick time.Time) error {
	var err error
	switch event.Name {
	case domain.EventSessionTraining:
		err = c.handleTrainingEvent(event, tick)
	case domain.EventSessionStopped:
		stopStartTime := time.Now()
		err = c.handleSessionStoppedEvent()

		// Record it as processed even if there was an error when processing the event.
		c.Workload.ProcessedEvent(domain.NewEmptyWorkloadEvent().
			WithEventId(event.Id()).
			WithSessionId(event.SessionID()).
			WithEventName(event.Name).
			WithNumberOfTimesEnqueued(event.GetNumTimesEnqueued()).
			WithEventTimestamp(event.Timestamp).
			WithProcessedAtTime(time.Now()).
			WithMetadata("duration_milliseconds", time.Since(stopStartTime).Milliseconds()).
			WithError(err))
	default:
		c.logger.Error("Received event of unknown or unexpected type.",
			zap.String("session_id", c.SessionId),
			zap.String("event_name", event.Name.String()),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("event", event.String()))

		err = fmt.Errorf("%w: \"%s\"", ErrUnknownEventType, event.Name.String())
	}

	return err // Will be nil on success
}

// handleTrainingEvent handles a domain.EventSessionTraining *domain.Event.
func (c *Client) handleTrainingEvent(event *domain.Event, tick time.Time) error {
	startedHandlingAt := time.Now()

	startTrainingTimeoutInterval := c.getTimeoutInterval(event)
	startTrainingCtx, startTrainingCancel := context.WithTimeout(context.Background(), startTrainingTimeoutInterval)
	defer startTrainingCancel()

	sentRequestAt, executeRequestId, err := c.submitTrainingToKernel(event)
	if err != nil {
		c.logger.Error("Failed to submit training to kernel.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("event", event.StringJson()),
			zap.Error(err))
		c.trainingEventsDelayed.Add(1)
		return err
	}

	c.Workload.ProcessedEvent(domain.NewEmptyWorkloadEvent().
		WithEventId(event.Id()).
		WithSessionId(event.SessionID()).
		WithEventName(domain.EventSessionTrainingSubmitted).
		WithEventTimestamp(event.Timestamp).
		WithProcessedAtTime(sentRequestAt).
		WithNumberOfTimesEnqueued(event.GetNumTimesEnqueued()).
		WithMetadata("latency_milliseconds", time.Since(sentRequestAt).Milliseconds()).
		WithError(err)) // Will be nil on success

	trainingStarted, trainingStartedAtUnixMillis, err := c.waitForTrainingToStart(startTrainingCtx, event, startedHandlingAt,
		sentRequestAt, startTrainingTimeoutInterval, executeRequestId)
	trainingStartedAt := time.UnixMilli(trainingStartedAtUnixMillis)

	if !trainingStarted {
		c.trainingEventsDelayed.Add(1)
		return nil
	}

	if err != nil {
		c.logger.Error("Failed to wait for training to start.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("event", event.String()),
			zap.String("execute_request_msg_id", executeRequestId),
			zap.Error(err))

		c.trainingEventsDelayed.Add(1)
		return err
	}

	c.Workload.ProcessedEvent(domain.NewEmptyWorkloadEvent().
		WithEventId(event.Id()).
		WithSessionId(event.SessionID()).
		WithEventName(domain.EventSessionTrainingStarted).
		WithEventTimestamp(event.Timestamp).
		WithProcessedAtTime(trainingStartedAt).
		WithMetadata("delay_milliseconds", trainingStartedAt.Sub(sentRequestAt).Milliseconds()).
		WithNumberOfTimesEnqueued(event.GetNumTimesEnqueued()).
		WithError(err)) // Will be nil on success

	c.logger.Debug(fmt.Sprintf("Handled \"%s\" event.", domain.ColorizeText("training-started", domain.Green)),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.Time("tick", tick),
		zap.Time("submitted_at", sentRequestAt),
		zap.Time("started_training_at", trainingStartedAt),
		zap.Duration("delay", trainingStartedAt.Sub(sentRequestAt)),
		zap.Duration("time_since_submission", time.Since(sentRequestAt)),
		zap.Int64("tick_number", c.convertTimestampToTickNumber(tick)),
		zap.String("execute_request_msg_id", executeRequestId))

	stopTrainingTimeoutInterval := c.getTimeoutInterval(event)
	stopTrainingCtx, stopTrainingCancel := context.WithTimeout(context.Background(), stopTrainingTimeoutInterval)
	defer stopTrainingCancel()

	err = c.waitForTrainingToEnd(stopTrainingCtx, event, executeRequestId, stopTrainingTimeoutInterval)
	if err == nil {
		c.Workload.ProcessedEvent(domain.NewEmptyWorkloadEvent().
			WithEventId(event.Id()).
			WithSessionId(event.SessionID()).
			WithEventName(domain.EventSessionTrainingEnded).
			WithEventTimestamp(event.Timestamp).
			WithProcessedAtTime(time.Now()).
			WithMetadata("duration_milliseconds", time.Since(trainingStartedAt).Milliseconds()).
			WithNumberOfTimesEnqueued(event.GetNumTimesEnqueued()).
			WithError(err)) // Will be nil on success

		trainingEventsHandled := c.trainingEventsHandled.Add(1)

		sleepInterval := (time.Second * 3) + (time.Millisecond * time.Duration(rand.Intn(2000)))

		c.logger.Debug(fmt.Sprintf("Handled \"%s\" event.", domain.ColorizeText("training-stopped", domain.LightGreen)),
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.Duration("time_elapsed", time.Since(startedHandlingAt)),
			zap.Int32("training_events_handled", trainingEventsHandled),
			zap.Int("total_training_events_for_session", len(c.Session.TrainingEvents)),
			zap.Float64("percent_done", float64(trainingEventsHandled)/float64(len(c.Session.TrainingEvents))),
			zap.Time("tick", tick),
			zap.Int64("tick_number", c.convertTimestampToTickNumber(tick)),
			zap.Duration("upcoming_sleep_interval", sleepInterval))

		time.Sleep(sleepInterval)
	}

	return err // Will be nil on success
}

// submitTrainingToKernel submits a training event to be processed/executed by the kernel.
func (c *Client) submitTrainingToKernel(evt *domain.Event) (sentRequestAt time.Time, executeRequestId string, err error) {
	c.logger.Debug("Client is submitting training event to kernel now.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.Duration("training_duration", evt.Duration),
		zap.String("event_id", evt.ID),
		zap.Float64("training_duration_sec", evt.Duration.Seconds()))

	kernelConnection := c.sessionConnection.Kernel
	if kernelConnection == nil {
		c.logger.Error("No kernel connection found for session connection.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId))
		err = ErrNoKernelConnection
		return
	}

	var executeRequestArgs *jupyter.RequestExecuteArgs
	executeRequestArgs, err = c.CreateExecuteRequestArguments(evt)
	if executeRequestArgs == nil || err != nil {
		c.logger.Error("Failed to create 'execute_request' arguments.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.Error(err))
		return time.Time{}, "", err
	}

	c.logger.Debug("Submitting \"execute_request\" now.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.Duration("original_training_duration", evt.Duration),
		zap.String("event_id", evt.ID),
		zap.Float64("training_duration_sec", evt.Duration.Seconds()),
		zap.String("execute_request_args", executeRequestArgs.String()))

	sentRequestAt = time.Now()

	_, executeRequestId, err = kernelConnection.RequestExecute(executeRequestArgs)
	if err != nil {
		c.logger.Error("Error while submitting training event to kernel.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId))
		return time.Time{}, executeRequestId, err
	}

	// We reset this once the training is handled, in case we have to resubmit,
	// as resubmissions count against the delay.
	if c.lastTrainingSubmittedAt.Equal(time.UnixMilli(0)) {
		c.lastTrainingSubmittedAt = time.Now()
	}

	c.Workload.TrainingSubmitted(c.SessionId, evt)

	// Update the 'training ended' map.
	c.trainingEndedRequestMapMutex.Lock()
	var receivedExecuteReplyFlag atomic.Int32
	c.trainingEndedRequestMap[executeRequestId] = &receivedExecuteReplyFlag
	c.lastSubmittedExecuteRequestId = executeRequestId
	c.outstandingExecuteRequestId = executeRequestId
	c.trainingEndedRequestMapMutex.Unlock()

	// Update the 'training started' map.
	c.trainingStartedRequestMapMutex.Lock()
	defer c.trainingStartedRequestMapMutex.Unlock()
	var receivedSmrLeadTaskFlag atomic.Int32
	c.trainingStartedRequestMap[executeRequestId] = &receivedSmrLeadTaskFlag

	return sentRequestAt, executeRequestId, nil
}

// OnReceiveExecuteReply is called when an "execute_reply" message is received by the associated kernel.
//
// OnReceiveExecuteReply is exported so that it can be called in unit tests.
func (c *Client) OnReceiveExecuteReply(response jupyter.KernelMessage) {
	receivedAt := time.Now()

	responseContent := response.GetContent().(map[string]interface{})
	if responseContent == nil {
		c.logger.Error("\"execute_reply\" message received via ZMQ does not have any content...",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("execute_reply_message", response.String()))
		return
	}

	val, ok := responseContent["status"]
	if !ok {
		c.logger.Error("\"execute_reply\" message received via ZMQ does not contain a \"status\" field in its content.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("execute_reply_message", response.String()))
		return
	}

	status := val.(string)

	if status == "error" {
		errorName := responseContent["ename"].(string)
		errorValue := responseContent["evalue"].(string)

		c.logger.Warn("Received \"execute_reply\" message with error status via ZMQ.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("ename", errorName),
			zap.String("evalue", errorValue),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("execute_reply_message", response.String()))

		// Notify the training started channel. There will not be a smr_lead_task sent at this point, since
		// there was an error, so we'll send the notification to the training_started channel.
		c.TrainingStartedChannel <- fmt.Errorf("%s: %s", errorName, errorValue)
		return
	}

	c.logger.Debug("Received \"execute_reply\" message with non-error status via ZMQ.",
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId))

	err := c.claimTrainingStoppedNotification(response, response.GetParentHeader().MessageId, zmq)
	if err != nil {
		return
	}

	// Handle the message by sending it over the channel.
	c.TrainingStoppedChannel <- &trainingStoppedNotification{
		ReceivedAt: receivedAt,
		Response:   response,
	}
}

// incurDelay is called when we experience some sort of delay and need to delay our future events accordingly.
func (c *Client) incurDelay(delayAmount time.Duration) {
	c.EventQueue.IncurDelay(delayAmount)

	c.Workload.SessionDelayed(c.SessionId, delayAmount)

	if metrics.PrometheusMetricsWrapperInstance != nil {
		metrics.PrometheusMetricsWrapperInstance.SessionDelayedDueToResourceContention.
			With(prometheus.Labels{
				"workload_id": c.Workload.GetId(),
				"session_id":  c.SessionId,
			}).Add(1)
	}
}

func (c *Client) handleExecuteRequestErred(err error, evt *domain.Event, startedHandlingAt time.Time, sentRequestAt time.Time,
	timeoutInterval time.Duration, executeRequestId string) {
	numTimesEnqueued := evt.GetNumTimesEnqueued()

	// Backoff.
	baseSleepInterval := time.Millisecond * 1250
	maxSleepInterval := time.Second * 10
	sleepInterval := (baseSleepInterval * time.Duration(numTimesEnqueued+1)) + (time.Millisecond * time.Duration(rand.Int63n(2500)))

	// Clamp with jitter.
	if sleepInterval > maxSleepInterval {
		sleepInterval = maxSleepInterval + (time.Millisecond * time.Duration(rand.Int63n(1250)))
	}

	c.Workload.UpdateStatistics(func(stats *Statistics) {
		stats.NumFailedExecutionAttempts += 1
	})

	c.logger.Warn(domain.ColorizeText("Session failed to start training.", domain.Orange),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.Int32("num_failures", numTimesEnqueued),
		zap.Duration("timeout_interval", timeoutInterval),
		zap.Duration("time_elapsed", time.Since(sentRequestAt)),
		zap.String("execute_request_id", executeRequestId),
		zap.Duration("sleep_interval", sleepInterval),
		zap.Error(err))

	// If we fail to start training for some reason, then we'll just try again later.
	c.incurDelay(time.Since(startedHandlingAt) + c.targetTickDuration*2)

	// Put the event back in the queue.
	c.EventQueue.Push(evt)
	time.Sleep(sleepInterval)
}

func (c *Client) doWaitForTrainingToStart(ctx context.Context, evt *domain.Event, startedHandlingAt time.Time,
	sentRequestAt time.Time, timeoutInterval time.Duration, execReqId string) (bool, int64, error) {

	select {
	case v := <-c.TrainingStartedChannel:
		{
			switch v.(type) {
			case error:
				{
					c.handleExecuteRequestErred(v.(error), evt, startedHandlingAt, sentRequestAt, timeoutInterval, execReqId)

					return false, -1, nil
				}
			default:
				{
					trainingStartedAt := v.(int64)
					delayMilliseconds := trainingStartedAt - c.lastTrainingSubmittedAt.UnixMilli()
					c.logger.Debug(domain.ColorizeText("Kernel started training.", domain.Green),
						zap.String("workload_id", c.Workload.GetId()),
						zap.String("workload_name", c.Workload.WorkloadName()),
						zap.String("session_id", c.SessionId),
						zap.Duration("timeout_interval", timeoutInterval),
						zap.String("execute_request_id", execReqId),
						zap.Int64("start_latency_milliseconds", delayMilliseconds))

					return true, trainingStartedAt, nil
				}
			}
		}
	case <-ctx.Done():
		{
			err := ctx.Err()
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				return false, -1, err // We'll check for context.DeadlineExceeded.
			}

			return false, -1, err
		}
	}
}

func (c *Client) shouldStopWaitingForTrainingToStart(err error) bool {
	if strings.Contains(err.Error(), "insufficient hosts available") {
		return true
	}

	if strings.Contains(err.Error(), "there is already an active scaling operation taking place") {
		return true
	}

	return false
}

// waitForTrainingToStart waits for a training to begin being processed by a kernel replica.
//
// waitForTrainingToStart is called by handleTrainingEvent after submitTrainingToKernel is called.
//
// waitForTrainingToStart returns a 3-tuple where the first element is a bool flag indicating whether training started,
// the second element is the unix milliseconds timestamp at which the training began, and the third is an error.
func (c *Client) waitForTrainingToStart(initialContext context.Context, evt *domain.Event, startedHandlingAt time.Time,
	sentRequestAt time.Time, originalTimeoutInterval time.Duration, execReqId string) (bool, int64, error) {

	c.logger.Debug("Waiting for session to start training before continuing...",
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.Duration("timeout_interval", originalTimeoutInterval),
		zap.String("execute_request_id", execReqId),
		zap.Duration("time_elapsed", time.Since(sentRequestAt)))

	// Wait for the training to end.
	trainingStarted, startedAtUnixMillis, err := c.doWaitForTrainingToStart(initialContext, evt, startedHandlingAt, sentRequestAt,
		originalTimeoutInterval, execReqId)
	if trainingStarted && err == nil {
		// Training started. We can return.
		return true, startedAtUnixMillis, nil
	} else if err != nil && c.shouldStopWaitingForTrainingToStart(err) {
		return false, -1, nil
	}

	isTraining := c.trainingStartTimedOut(sentRequestAt, originalTimeoutInterval, execReqId)

	// If the kernel IS training, then we'll try to retrieve the associated "smr_lead_task" message via gRPC,
	// in case it was dropped or otherwise delayed.
	if isTraining {
		startedAtUnixMillis, err = c.checkIfTrainingStartedViaGrpc(execReqId)
		if startedAtUnixMillis > 0 && err == nil {
			return true, startedAtUnixMillis, nil
		} else if err != nil && c.shouldStopWaitingForTrainingToStart(err) {
			return false, -1, nil
		}
	}

	startedWaitingAt := time.Now()
	maximumAdditionalWaitTime := time.Minute * 5

	cumulativeTimeoutInterval := originalTimeoutInterval
	timeoutInterval := time.Second * 60

	for time.Since(startedWaitingAt) < maximumAdditionalWaitTime {
		cumulativeTimeoutInterval = cumulativeTimeoutInterval + timeoutInterval
		ctx, cancel := context.WithTimeout(context.Background(), timeoutInterval)

		trainingStarted, startedAtUnixMillis, err = c.doWaitForTrainingToStart(ctx, evt, startedHandlingAt, sentRequestAt,
			originalTimeoutInterval, execReqId)
		if trainingStarted && startedAtUnixMillis > 0 && err == nil {
			cancel()
			return true, startedAtUnixMillis, nil
		} else if err != nil && c.shouldStopWaitingForTrainingToStart(err) {
			cancel()
			return false, -1, nil
		}

		// Error related to timing out? Or we simply haven't started training?
		// If so, log a message, send a notification, and keep on waiting.
		if (!trainingStarted && err == nil) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, jupyter.ErrRequestTimedOut) {
			isTraining = c.trainingStartTimedOut(sentRequestAt, timeoutInterval, execReqId)
			cancel()

			// After the initial timeout, we'll have been waiting long enough that we'll check even if it says that
			// the kernel is not yet training -- though this is unlikely to succeed in that case. The gateway learns that
			// the kernel is no longer training by receiving the "smr_lead_task" message.
			startedAtUnixMillis, err = c.checkIfTrainingStartedViaGrpc(execReqId)
			if startedAtUnixMillis > 0 && err == nil {
				return true, startedAtUnixMillis, nil
			} else if err != nil && c.shouldStopWaitingForTrainingToStart(err) {
				return false, -1, nil
			}

			c.logger.Debug("Failed to retrieve \"smr_lead_task\" message via gRPC.",
				zap.String("session_id", c.SessionId),
				zap.String("workload_id", c.Workload.GetId()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.Bool("is_actively_training", isTraining),
				zap.String("execute_request_msg_id", execReqId),
				zap.Error(err))

			continue
		}

		// We received some other error. We'll just return it. Something is wrong, apparently.
		cancel()
		return false, -1, err
	}

	c.logger.Error("Completely timed out waiting for training to begin. Assuming message was lost...",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("execute_request_msg_id", execReqId),
		zap.Bool("is_actively_training", isTraining),
		zap.Duration("original_timeout_interval", originalTimeoutInterval),
		zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)))

	// If there's an existing error, then we'll join it with an errShutdownClient.
	// errShutdownClient will instruct the client to terminate early.
	if err != nil {
		err = errors.Join(errShutdownClient, err)
	}

	// errShutdownClient will instruct the client to terminate early.
	return false, -1, errShutdownClient
}

// trainingStartTimedOut is called by waitForTrainingToEnd when we don't receive a notification that the submitted
// training event stopped being processed after the timeout interval elapsed.
//
// trainingStoppedTimedOut returns "true" if the kernel is actively training and "false" if not.
func (c *Client) trainingStoppedTimedOut(sentRequestAt time.Time, timeoutInterval time.Duration, execReqMsgId string, err error) bool {
	timeElapsed := time.Since(sentRequestAt)

	c.logger.Warn("Have been waiting for \"execute_reply\" message for a long time.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("execute_request_msg_id", execReqMsgId),
		zap.Duration("cumulative_timeout_interval", timeoutInterval),
		zap.Duration("time_elapsed", timeElapsed),
		zap.Error(err))

	var (
		activelyTrainingStatus string
		isActivelyTraining     bool
	)

	isTraining, rpcError := c.isKernelTrainingCallback(c.SessionId)
	if rpcError != nil {
		c.logger.Warn("Failed to query training status of our kernel.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("execute_request_msg_id", execReqMsgId),
			zap.Duration("timeout_interval", timeoutInterval),
			zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)),
			zap.Error(rpcError))

		activelyTrainingStatus = "UNKNOWN"
		isActivelyTraining = false
	} else if isTraining {
		activelyTrainingStatus = "YES"
		isActivelyTraining = true
	} else {
		// TODO: If this happens, then something is fairly wrong, no? As in, the kernel's reply may have been dropped?
		// 		 In theory, we could also return the Jupyter message ID of the last "execute_request"
		//		 processed by the kernel and, if it is equal to the one we're waiting on, then we
		//		 could assume that the ZMQ message was dropped.
		activelyTrainingStatus = "NO"
		isActivelyTraining = false
	}

	c.notifyCallback(&proto.Notification{
		Id:    uuid.NewString(),
		Title: fmt.Sprintf("Have Spent Over %v Waiting for 'Training Stopped' Notification", timeElapsed),
		Message: fmt.Sprintf("Submitted \"execute_request\" to kernel \"%s\" during workload \"%s\" (ID=\"%s\") "+
			"over %v ago and have not yet received \"execute_reply\" message. RequestID=\"%s\". Actively training: %s.",
			c.SessionId, c.Workload.WorkloadName(), c.Workload.GetId(), timeElapsed, execReqMsgId, activelyTrainingStatus),
		Panicked:         false,
		NotificationType: domain.WarningNotification.Int32(),
	})

	return isActivelyTraining
}

// trainingStoppedTimedOut is called by waitForTrainingToStart when we don't receive a notification that the submitted
// training event started being processed after the timeout interval elapses.
//
// trainingStartTimedOut returns a flag indicating whether the kernel is presently training based on our result of
// querying the cluster gateway directly for this information.
func (c *Client) trainingStartTimedOut(sentRequestAt time.Time, timeoutInterval time.Duration, executeRequestId string) bool {
	var isTraining bool
	if c.isKernelTrainingCallback != nil {
		var err error
		isTraining, err = c.isKernelTrainingCallback(c.SessionId)
		if err != nil {
			c.logger.Warn("Failed to query Cluster Gateway regarding training status of kernel on 'training started' time-out.",
				zap.String("session_id", c.SessionId),
				zap.String("workload_id", c.Workload.GetId()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.String("execute_request_msg_id", executeRequestId),
				zap.Duration("timeout_interval", timeoutInterval),
				zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)),
				zap.Error(err))
		}
	}

	timeElapsed := time.Since(sentRequestAt)
	c.logger.Warn(fmt.Sprintf("Have not received 'training started' notification for over %v.",
		time.Since(sentRequestAt)),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.Duration("timeout_interval", timeoutInterval),
		zap.String("execute_request_msg_id", executeRequestId),
		zap.Duration("time_elapsed", timeElapsed),
		zap.Bool("is_training", isTraining))

	c.notifyCallback(&proto.Notification{
		Id:    uuid.NewString(),
		Title: fmt.Sprintf("Have Spent Over %v Waiting for 'Training Started' Notification", timeElapsed),
		Message: fmt.Sprintf("Submitted \"execute_request\" to kernel \"%s\" during workload \"%s\" (ID=\"%s\") "+
			"over %v ago and have not yet received 'smr_lead_task' IOPub message. IsTraining=%v, RequestID=\"%s\".",
			c.SessionId, c.Workload.WorkloadName(), c.Workload.GetId(), timeElapsed, isTraining, executeRequestId),
		Panicked:         false,
		NotificationType: domain.WarningNotification.Int32(),
	})

	return isTraining
}

// convertTimestampToTickNumber converts the given tick, which is specified in the form of a time.Time,
// and returns what "tick number" that tick is.
//
// Basically, you just convert the timestamp to its unix epoch timestamp (in seconds), and divide by the
// trace step value (also in seconds).
func (c *Client) convertTimestampToTickNumber(tick time.Time) int64 {
	return tick.Unix() / c.targetTickDurationSeconds
}

// convertCurrentTickTimestampToTickNumber converts the current tick to what "tick number" it is.
func (c *Client) convertCurrentTickTimestampToTickNumber() int64 {
	return c.currentTick.GetClockTime().Unix() / c.targetTickDurationSeconds
}

// HandleIOPubMessage returns the extracted text.
// This is expected to be called within a session-specific wrapper.
//
// If the IOPub message is a "stream" message, then this returns a *parsedIoPubMessage
// wrapping the name of the stream and the message text.
func (c *Client) HandleIOPubMessage(kernelMessage jupyter.KernelMessage) interface{} {
	// We just want to extract the output from 'stream' IOPub messages.
	// We don't care about non-stream-type IOPub messages here, so we'll just return.
	messageType := kernelMessage.GetHeader().MessageType
	if messageType != "stream" && messageType != "smr_lead_task" {
		return nil
	}

	if messageType == "stream" {
		return c.handleIOPubStreamMessage(kernelMessage)
	}

	// Claim ownership over handling the notification. We're "competing" with the goroutine listening
	// for "smr_lead_task" messages sent via ZMQ/WebSockets/whatever it is that we're using.
	err := c.claimTrainingStartedNotification(kernelMessage, kernelMessage.GetParentHeader().MessageId, gRPC)
	if err != nil {
		c.logger.Debug("\"smr_lead_task\" message has already been claimed. Discarding ZMQ message.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("jupyter_message_id", kernelMessage.GetHeader().MessageId),
			zap.String("parent_jupyter_message_id", kernelMessage.GetParentHeader().MessageId))

		return kernelMessage
	}

	return c.handleTrainingStartedNotification(kernelMessage, zmq)
}

func (c *Client) handleTrainingStartedNotification(kernelMessage jupyter.KernelMessage, source requestSource) int64 {
	c.logger.Debug("Received 'smr_lead_task' message from kernel.",
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.String("jupyter_message_id", kernelMessage.GetHeader().MessageId),
		zap.String("request_source", source.String()),
		zap.String("parent_jupyter_message_id", kernelMessage.GetParentHeader().MessageId))

	c.Workload.TrainingStarted(c.SessionId, c.convertCurrentTickTimestampToTickNumber())

	// Use the timestamp encoded in the IOPub message to determine when the training actually began,
	// and then delay the session by how long it took for training to begin.
	content := kernelMessage.GetContent().(map[string]interface{})

	var trainingStartedAt int64
	val, ok := content["msg_created_at_unix_milliseconds"]
	if !ok {
		c.logger.Error("Could not recover unix millisecond timestamp from \"smr_lead_task\" IOPub message.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("request_source", source.String()),
			zap.Any("message_content", content))

		c.notifyCallback(&proto.Notification{
			Id:    uuid.NewString(),
			Title: fmt.Sprintf("Failed to Extract Message Creation Time from \"smr_lead_task\" IOPub Message"),
			Message: fmt.Sprintf("Client %s failed to extract \"msg_created_at_unix_milliseconds\" entry from "+
				"content of \"smr_lead_task\" IOPub message \"%s\" received via %s.",
				c.SessionId, kernelMessage.GetHeader().MessageId, source.String()),
			Panicked:         false,
			NotificationType: domain.ErrorNotification.Int32(),
		})

		panic("Could not recover unix millisecond timestamp from \"smr_lead_task\" IOPub message.")
	}

	switch val.(type) {
	case float64:
		{
			trainingStartedAt = int64(val.(float64))
		}
	case float32:
		{
			trainingStartedAt = int64(val.(float32))
		}
	case int64:
		{
			trainingStartedAt = val.(int64)
		}
	case int32:
		{
			trainingStartedAt = int64(val.(int32))
		}
	case int:
		{
			trainingStartedAt = int64(val.(int))
		}
	default:
		c.logger.Error("Unexpected type of \"msg_created_at_unix_milliseconds\" value found in content of \"smr_lead_task\" IOPub message",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("request_source", source.String()),
			zap.String("message_id", kernelMessage.GetHeader().MessageId),
			zap.String("type", reflect.TypeOf(val).Name()))

		trainingStartedAt = -1
	}

	if trainingStartedAt > 0 {
		delayMilliseconds := trainingStartedAt - c.lastTrainingSubmittedAt.UnixMilli()
		if delayMilliseconds < 0 {
			c.logger.Error("Computed invalid delay between training submission and training start...",
				zap.String("workload_id", c.Workload.GetId()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.String("session_id", c.SessionId),
				zap.Time("sent_execute_request_at", c.lastTrainingSubmittedAt),
				zap.String("request_source", source.String()),
				zap.Int64("training_started_at", trainingStartedAt),
				zap.Int64("computed_delay_millis", delayMilliseconds))

			delayMilliseconds = 0
		}

		c.Workload.UpdateStatistics(func(stats *Statistics) {
			stats.JupyterTrainingStartLatenciesDashboardMillis = append(
				stats.JupyterTrainingStartLatenciesDashboardMillis, float64(delayMilliseconds))

			stats.JupyterTrainingStartLatencyDashboardMillis += float64(delayMilliseconds)
		})

		c.logger.Debug("Computed training-started delay for session.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("request_source", source.String()),
			zap.Time("sent_execute_request_at", c.lastTrainingSubmittedAt),
			zap.Int64("training_started_at", trainingStartedAt),
			zap.Int64("computed_delay_milliseconds", delayMilliseconds))

		c.incurDelay(time.Millisecond * time.Duration(delayMilliseconds))
	}

	if source == zmq {
		c.TrainingStartedChannel <- trainingStartedAt
	}

	return trainingStartedAt
}

func (c *Client) handleIOPubStreamMessage(kernelMessage jupyter.KernelMessage) interface{} {
	content := kernelMessage.GetContent().(map[string]interface{})

	var (
		stream string
		text   string
		ok     bool
	)

	stream, ok = content["name"].(string)
	if !ok {
		c.logger.Warn("Content of IOPub message did not contain an entry with key \"name\" and value of type string.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.Any("content", content), zap.Any("message", kernelMessage),
			zap.String("session_id", c.SessionId))
		return nil
	}

	text, ok = content["text"].(string)
	if !ok {
		c.logger.Warn("Content of IOPub message did not contain an entry with key \"text\" and value of type string.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.Any("content", content),
			zap.Any("message", kernelMessage),
			zap.String("session_id", c.SessionId))
		return nil
	}

	return &parsedIoPubMessage{
		Stream: stream,
		Text:   text,
	}
}

// CreateExecuteRequestArguments creates the arguments for an "execute_request" from the given event.
//
// The event must be of type "training-started", or this will return nil.
func (c *Client) CreateExecuteRequestArguments(evt *domain.Event) (*jupyter.RequestExecuteArgs, error) {
	if evt.Name != domain.EventSessionTraining {
		c.logger.Error("Attempted to create \"execute_request\" arguments for event of invalid type.",
			zap.String("event_type", evt.Name.String()),
			zap.String("event_id", evt.Id()),
			zap.String("session_id", evt.SessionID()),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()))

		return nil, fmt.Errorf("invalid event type: %s", evt.Name)
	}

	sessionMetadata := evt.Data.(domain.SessionMetadata)

	if sessionMetadata == nil {
		c.logger.Error("Event has nil data.",
			zap.String("event_type", evt.Name.String()),
			zap.String("event_id", evt.Id()),
			zap.String("session_id", evt.SessionID()),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()))
		return nil, fmt.Errorf("event has nil data")
	}

	gpus := sessionMetadata.GetCurrentTrainingMaxGPUs()
	if gpus == 0 && sessionMetadata.HasGpus() && sessionMetadata.GetGPUs() > 0 {
		gpus = sessionMetadata.GetGPUs()
	}

	resourceRequest := &domain.ResourceRequest{
		Cpus:     sessionMetadata.GetCurrentTrainingMaxCPUs(),
		MemoryMB: sessionMetadata.GetCurrentTrainingMaxMemory(),
		VRAM:     sessionMetadata.GetVRAM(),
		Gpus:     gpus,
	}

	milliseconds := float64(evt.Duration.Milliseconds())
	if c.Workload.ShouldTimeCompressTrainingDurations() {
		milliseconds = milliseconds * c.timescaleAdjustmentFactor
		c.logger.Debug("Applied time-compression to training duration.",
			zap.String("session_id", evt.SessionID()),
			zap.Duration("original_duration", evt.Duration),
			zap.Float64("updated_duration_ms", milliseconds),
			zap.Float64("timescale_adjustment_factor", c.timescaleAdjustmentFactor),
			zap.String("event_id", evt.Id()),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()))
	}

	argsBuilder := jupyter.NewRequestExecuteArgsBuilder().
		Code(fmt.Sprintf("training_duration_millis = %d", evt.Duration.Milliseconds())).
		Silent(false).
		StoreHistory(true).
		UserExpressions(nil).
		AllowStdin(true).
		StopOnError(false).
		AwaitResponse(false).
		OnResponseCallback(c.OnReceiveExecuteReply).
		AddMetadata("resource_request", resourceRequest).
		AddMetadata("training_duration_millis", milliseconds).
		AddMetadata("execution_index", c.trainingEventsHandled.Load()+1)

	if c.AssignedModel != "" {
		argsBuilder = argsBuilder.AddMetadata("model", c.AssignedModel)
	}

	if c.AssignedDataset != "" {
		argsBuilder = argsBuilder.AddMetadata("dataset", c.AssignedDataset)
	}

	return argsBuilder.Build(), nil
}

// getAdjustedDuration returns the duration of the *domain.Event adjusted based on the timescale adjustment factor
// of the Client's Workload.
func (c *Client) getAdjustedDuration(evt *domain.Event) time.Duration {
	timescaleAdjustmentFactor := c.Workload.GetTimescaleAdjustmentFactor()
	duration := evt.Duration

	if duration == 0 {
		return 0
	}

	return time.Duration(timescaleAdjustmentFactor * float64(evt.Duration))
}

// getTimeoutInterval computes a "meaningful" timeout interval based on the scheduling policy, taking into account
// approximately how long the network I/O before/after training is expected to take and whatnot.
func (c *Client) getTimeoutInterval(evt *domain.Event) time.Duration {
	baseInterval := time.Second * 330

	// Load the scheduling policy.
	schedulingPolicy := c.schedulingPolicy
	if schedulingPolicy == "" {
		c.logger.Warn("Could not compute meaningful timeout interval because scheduling policy is invalid.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("event", evt.Name.String()))
		return baseInterval + c.getAdjustedDuration(evt)
	}

	if schedulingPolicy == "static" || schedulingPolicy == "dynamic-v3" || schedulingPolicy == "dynamic-v4" {
		// There's no network I/O on the critical path, so stopping the training should be quick.
		return baseInterval + c.getAdjustedDuration(evt)
	}

	// Get the remote storage definition of the workload.
	remoteStorageDefinition := c.Workload.GetRemoteStorageDefinition()
	if remoteStorageDefinition == nil {
		c.logger.Warn("Could not compute meaningful timeout interval because scheduling policy is invalid.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("event", evt.Name.String()))
		return baseInterval + c.getAdjustedDuration(evt) // We make it a bit higher since we know I/O is on the critical path.
	}

	// Load the session and subsequently its current resource request.
	// We already checked that this existed in handleTrainingEventEnded.
	resourceRequest := c.Session.GetCurrentResourceRequest()
	if resourceRequest == nil {
		c.logger.Warn("Could not compute meaningful timeout interval because scheduling policy is invalid.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("event", evt.Name.String()))
		return baseInterval + c.getAdjustedDuration(evt) // We make it a bit higher since we know I/O is on the critical path.
	}

	vramBytes := resourceRequest.VRAM * 1.0e9
	readTime := (vramBytes / float64(remoteStorageDefinition.DownloadRate)) * (1 + float64(remoteStorageDefinition.DownloadRateVariancePercentage))
	writeTime := (vramBytes / float64(remoteStorageDefinition.UploadRate)) * (1 + float64(remoteStorageDefinition.UploadRateVariancePercentage))
	expectedNetworkIoLatency := readTime + writeTime

	// Extra 30 seconds for whatever shenanigans need to occur.
	interval := baseInterval + (time.Second * time.Duration(expectedNetworkIoLatency)) + c.getAdjustedDuration(evt)

	c.logger.Debug("Computed timeout interval.",
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId),
		zap.Float64("vram_gb", resourceRequest.VRAM),
		zap.Float64("vram_bytes", vramBytes),
		zap.String("remote_storage_definition", remoteStorageDefinition.String()),
		zap.String("event", evt.Name.String()))

	return interval
}

// checkIfTrainingStartedViaGrpc is called when the Client times out waiting for a training to begin.
//
// Specifically, checkIfTrainingStartedViaGrpc attempts to retrieve the JupyterMessage that may
// have been dropped.
//
// checkIfTrainingStartedViaGrpc returns an error if it is unable to resolve the status of the training. So,
// if checkIfTrainingStartedViaGrpc returns an error, then the Client should keep waiting.
func (c *Client) checkIfTrainingStartedViaGrpc(execReqMsgId string) (int64, error) {
	// If our callback for retrieving a Jupyter message is nil, then just return.
	if c.getJupyterMessageCallback == nil {
		return -1, fmt.Errorf("no 'get jupyter message' callback configured for client")
	}

	c.logger.Debug("Attempting to retrieve \"smr_lead_task\" message via gRPC, in case the ZMQ version was dropped.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("execute_request_msg_id", execReqMsgId),
		zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)))

	resp, err := c.getJupyterMessageCallback(c.SessionId, execReqMsgId, "smr_lead_task")
	if err != nil {
		c.logger.Warn("Failed to retrieve \"smr_lead_task\" message via gRPC.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("execute_request_msg_id", execReqMsgId),
			zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)),
			zap.Error(err))
		return -1, err
	}

	return c.handleTrainingStartedViaGrpc(execReqMsgId, resp)
}

func (c *Client) claimTrainingStartedNotification(jupyterMsg jupyter.KernelMessage, execReqMsgId string, source requestSource) error {
	c.trainingStartedRequestMapMutex.Lock()
	receivedFlag, loaded := c.trainingStartedRequestMap[jupyterMsg.GetParentHeader().MessageId]
	c.trainingStartedRequestMapMutex.Unlock()

	if !loaded {
		c.logger.Error("No request flag associated with \"execute_request\" message upon receiving \"smr_lead_task\"",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("execute_request_id", jupyterMsg.GetHeader().MessageId),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("request_source", source.String()),
			zap.String("smr_lead_task_message", jupyterMsg.String()))

		title := fmt.Sprintf("No Request Flag for \"smr_lead_task\" Message \"%s\" For Execution \"%s\" Targeting Kernel \"%s\"",
			jupyterMsg.GetHeader().MessageId, jupyterMsg.GetParentHeader().MessageId, c.SessionId)
		message := fmt.Sprintf("Cannot properly handle 'training-started' event after receiving \"smr_lead_task\" message \"%s\" via %s.",
			jupyterMsg.GetHeader().MessageId, source.String())

		c.notifyCallback(&proto.Notification{
			Id:               uuid.NewString(),
			Title:            title,
			Message:          message,
			NotificationType: domain.ErrorNotification.Int32(),
			Panicked:         false,
		})

		return fmt.Errorf("missing request flag for \"smr_lead_task\" \"%s\"", execReqMsgId)
	}

	// In cases where a message is significantly delayed, we may resort to retrieving it via gRPC.
	// In this case, we don't want things to get messed up if we eventually receive the delayed ZMQ message here.
	// For example, we could misinterpret a delayed "execute_reply" as being for the current training and think that
	// the current training has ended when it hasn't.
	//
	// So, we check if we're the ones to atomically flip the associated 'received' flag from 0 to 1. If so,
	// then we can handle the message. If not, then we discard it.
	if !receivedFlag.CompareAndSwap(0, 1) {
		c.logger.Warn("Failed to flip 'received' flag from 0 to 1 for \"smr_lead_task\".",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("execute_request_id", jupyterMsg.GetHeader().MessageId),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("request_source", source.String()),
			zap.String("smr_lead_task_message", jupyterMsg.String()))

		return fmt.Errorf("failed to flip 'received' flag for \"smr_lead_task\"")
	}

	return nil
}

func (c *Client) handleTrainingStartedViaGrpc(execReqMsgId string, resp *proto.GetJupyterMessageResponse) (int64, error) {
	jupyterMsg, conversionErr := proto_utilities.ProtoToJupyterMessage(resp.Message)
	if conversionErr != nil {
		c.logger.Error("Failed to convert \"smr_lead_task\" proto JupyterMessage to standard JupyterMessage",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("execute_request_msg_id", execReqMsgId),
			zap.String("execute_reply_message_proto", resp.String()),
			zap.Error(conversionErr))

		return -1, conversionErr
	}

	// Claim ownership over handling the notification. We're "competing" with the goroutine listening
	// for "smr_lead_task" messages sent via ZMQ/WebSockets/whatever it is that we're using.
	err := c.claimTrainingStartedNotification(jupyterMsg, execReqMsgId, gRPC)
	if err != nil {
		return -1, err
	}

	trainingStartedAt := c.handleTrainingStartedNotification(jupyterMsg, gRPC)
	return trainingStartedAt, nil
}

// checkIfTrainingStoppedViaGrpc is called when the Client times out waiting for a training to end.
//
// Specifically, checkIfTrainingStoppedViaGrpc attempts to retrieve the JupyterMessage that may
// have been dropped.
//
// checkIfTrainingStoppedViaGrpc returns an error if it is unable to resolve the status of the training. So,
// if checkIfTrainingStoppedViaGrpc returns an error, then the Client should keep waiting.
func (c *Client) checkIfTrainingStoppedViaGrpc(evt *domain.Event, execReqMsgId string,
	cumulativeTimeoutInterval time.Duration) error {

	// If our callback for retrieving a Jupyter message is nil, then just return.
	if c.getJupyterMessageCallback == nil {
		return fmt.Errorf("no 'get jupyter message' callback configured for client")
	}

	c.logger.Debug("Attempting to retrieve \"execute_reply\" message via gRPC, in case the ZMQ version was dropped.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("execute_request_msg_id", execReqMsgId),
		zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)))

	resp, err := c.getJupyterMessageCallback(c.SessionId, execReqMsgId, "execute_reply")

	if err != nil {
		c.logger.Warn("Failed to retrieve \"execute_reply\" message via gRPC.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("execute_request_msg_id", execReqMsgId),
			zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)),
			zap.Error(err))
		return err
	}

	return c.handleTrainingStopViaGrpc(evt, execReqMsgId, resp, cumulativeTimeoutInterval)
}

func (c *Client) claimTrainingStoppedNotification(jupyterMsg jupyter.KernelMessage, execReqMsgId string, source requestSource) error {
	c.trainingEndedRequestMapMutex.Lock()
	receivedFlag, loaded := c.trainingEndedRequestMap[jupyterMsg.GetParentHeader().MessageId]
	c.trainingEndedRequestMapMutex.Unlock()

	if !loaded {
		c.logger.Error("No request flag associated with \"execute_request\" message upon receiving \"execute_reply\"",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("execute_request_id", jupyterMsg.GetHeader().MessageId),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("request_source", source.String()),
			zap.String("execute_reply_message", jupyterMsg.String()))

		title := fmt.Sprintf("No Request Flag for \"execute_request\" Message \"%s\" Targeting Kernel \"%s\"",
			jupyterMsg.GetParentHeader().MessageId, c.SessionId)
		message := fmt.Sprintf("Cannot properly handle 'training-stopped' event after receiving \"execute_reply\" message \"%s\" via %s.",
			jupyterMsg.GetHeader().MessageId, source.String())

		c.notifyCallback(&proto.Notification{
			Id:               uuid.NewString(),
			Title:            title,
			Message:          message,
			NotificationType: domain.ErrorNotification.Int32(),
			Panicked:         false,
		})

		return fmt.Errorf("missing request flag for \"execute_request\" \"%s\"", execReqMsgId)
	}

	// In cases where a message is significantly delayed, we may resort to retrieving it via gRPC.
	// In this case, we don't want things to get messed up if we eventually receive the delayed ZMQ message here.
	// For example, we could misinterpret a delayed "execute_reply" as being for the current training and think that
	// the current training has ended when it hasn't.
	//
	// So, we check if we're the ones to atomically flip the associated 'received' flag from 0 to 1. If so,
	// then we can handle the message. If not, then we discard it.
	if !receivedFlag.CompareAndSwap(0, 1) {
		c.logger.Warn("Failed to flip 'received' flag from 0 to 1 for \"execute_reply\". Must have already received message.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.String("execute_request_id", jupyterMsg.GetHeader().MessageId),
			zap.String("outstanding_execute_request_id", c.outstandingExecuteRequestId),
			zap.String("request_source", source.String()),
			zap.String("execute_reply_message", jupyterMsg.String()))

		return fmt.Errorf("failed to flip 'received' flag for \"execute_reply\"")
	}

	return nil
}

// handleTrainingStopViaGrpc is called by checkIfTrainingStoppedViaGrpc if the "execute_reply"
// message that the target Client has been waiting on was retrieved successfully via gRPC.
func (c *Client) handleTrainingStopViaGrpc(evt *domain.Event, execReqMsgId string, resp *proto.GetJupyterMessageResponse,
	cumulativeTimeoutInterval time.Duration) error {

	jupyterMsg, conversionErr := proto_utilities.ProtoToJupyterMessage(resp.Message)
	if conversionErr != nil {
		c.logger.Error("Failed to convert proto JupyterMessage to standard JupyterMessage",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("execute_request_msg_id", execReqMsgId),
			zap.String("execute_reply_message_proto", resp.String()),
			zap.Error(conversionErr))

		return conversionErr
	}

	// Claim ownership over handling the notification. We're "competing" with the goroutine listening
	// for "execute_reply" messages sent via ZMQ/WebSockets/whatever it is that we're using.
	err := c.claimTrainingStoppedNotification(jupyterMsg, execReqMsgId, gRPC)
	if err != nil {
		return err
	}

	// We'll estimate the e2e latency, because the messages being dropped is more likely a bug of some sort...
	content := jupyterMsg.GetContent().(map[string]interface{})
	val := content["execution_finished_unix_millis"]
	execEndedTimeUnixMillis := int64(val.(float64))
	execEndedTime := time.UnixMilli(execEndedTimeUnixMillis)

	estimatedReplyLatencyMillis := c.Workload.Statistics.GetAverageTotalReplyLatencyMillis()
	estimatedReplyLatency := time.Duration(float64(time.Millisecond) * estimatedReplyLatencyMillis)

	estimatedReceivedAt := execEndedTime.Add(estimatedReplyLatency)

	estimatedEndToEndLatency := estimatedReceivedAt.Sub(c.lastTrainingSubmittedAt)

	c.logger.Debug("Retrieved \"execute_reply\" message via gRPC.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("execute_request_msg_id", execReqMsgId),
		zap.Time("estimated_received_at", estimatedReceivedAt),
		zap.Float64("estimated_reply_latency_millis", estimatedReplyLatencyMillis),
		zap.Duration("estimated_reply_latency", estimatedReplyLatency),
		zap.Duration("estimated_end_to_end_latency", estimatedEndToEndLatency),
		zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)),
		zap.String("execute_reply_message", resp.String()))

	// If we estimated an invalid latency, then just use the average so far...
	if estimatedEndToEndLatency < 0 {
		estimatedEndToEndLatency = time.Duration(float64(time.Millisecond) * c.Workload.Statistics.GetAverageEndToEndExecuteRequestLatencyMillis())
	}

	notification := &trainingStoppedNotification{
		Response:   jupyterMsg,
		ReceivedAt: estimatedReceivedAt,
	}

	c.handleTrainingEnded(evt, notification, execReqMsgId, cumulativeTimeoutInterval, estimatedEndToEndLatency)
	return nil
}

// waitForTrainingToEnd waits for a training to begin being processed by a kernel replica.
//
// waitForTrainingToEnd is called by handleTrainingEvent after submitTrainingToKernel is called.
func (c *Client) waitForTrainingToEnd(initialContext context.Context, evt *domain.Event, execReqMsgId string,
	originalTimeoutInterval time.Duration) error {

	defer func() {
		// Reset this value regardless of whether we successfully stop training or not.
		c.lastTrainingSubmittedAt = time.UnixMilli(0)
	}()

	startedWaitingAt := time.Now()
	maximumAdditionalWaitTime := time.Minute * 10

	// Wait for the training to end.
	err := c.doWaitForTrainingToEnd(initialContext, evt, execReqMsgId, originalTimeoutInterval)
	if err == nil {
		// Training ended. We can return.
		return nil
	}

	// Log a message and send a warning notification.
	isTraining := c.trainingStoppedTimedOut(c.lastTrainingSubmittedAt, originalTimeoutInterval, execReqMsgId, err)
	if !isTraining {
		err = c.checkIfTrainingStoppedViaGrpc(evt, execReqMsgId, originalTimeoutInterval)
		if err == nil {
			return nil
		}

		c.logger.Debug("Failed to retrieve \"execute_reply\" message via gRPC.",
			zap.String("session_id", c.SessionId),
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("execute_request_msg_id", execReqMsgId),
			zap.Error(err))
	}

	cumulativeTimeoutInterval := originalTimeoutInterval
	timeoutInterval := time.Second * 60

	// Keep waiting for a while.
	// We'll start printing more frequent warnings.
	for time.Since(startedWaitingAt) < maximumAdditionalWaitTime {
		cumulativeTimeoutInterval = cumulativeTimeoutInterval + timeoutInterval
		ctx, cancel := context.WithTimeout(context.Background(), timeoutInterval)

		// Wait a little longer for the training to end.
		err = c.doWaitForTrainingToEnd(ctx, evt, execReqMsgId, timeoutInterval)
		if err == nil {
			// Training has finally ended. We can return.
			cancel()
			return nil
		}

		// Error related to timing out? If so, log a message, send a notification, and keep on waiting.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, jupyter.ErrRequestTimedOut) {
			isTraining = c.trainingStoppedTimedOut(c.lastTrainingSubmittedAt, cumulativeTimeoutInterval, execReqMsgId, err)
			cancel()

			// After the initial timeout, we'll have been waiting long enough that we'll check even if it says that
			// the kernel is still training -- though this is unlikely to succeed in that case. The gateway learns that
			// the kernel is no longer training by receiving the "execute_reply" message.
			err = c.checkIfTrainingStoppedViaGrpc(evt, execReqMsgId, cumulativeTimeoutInterval)
			if err == nil {
				return nil
			}

			c.logger.Debug("Failed to retrieve \"execute_reply\" message via gRPC.",
				zap.String("session_id", c.SessionId),
				zap.String("workload_id", c.Workload.GetId()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.Bool("is_actively_training", isTraining),
				zap.String("execute_request_msg_id", execReqMsgId),
				zap.Error(err))

			continue
		}

		// We received some other error. We'll just return it. Something is wrong, apparently.
		cancel()
		return err
	}

	c.logger.Error("Completely timed out waiting for training to end.",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("execute_request_msg_id", execReqMsgId),
		zap.Bool("is_actively_training", isTraining),
		zap.Duration("original_timeout_interval", originalTimeoutInterval),
		zap.Duration("time_elapsed", time.Since(c.lastTrainingSubmittedAt)))

	// If there's an existing error, then we'll join it with an errShutdownClient.
	// errShutdownClient will instruct the client to terminate early.
	if err != nil {
		err = errors.Join(errShutdownClient, err)
	}

	// errShutdownClient will instruct the client to terminate early.
	return errShutdownClient
}

// waitForTrainingToEnd waits until we receive an "execute_request" from the kernel.
func (c *Client) doWaitForTrainingToEnd(ctx context.Context, event *domain.Event, execReqMsgId string, timeoutInterval time.Duration) error {
	select {
	case v := <-c.TrainingStoppedChannel:
		{
			return c.handleNotificationOnTrainingStoppedChan(v, event, execReqMsgId, timeoutInterval, time.Now())
		}
	case <-ctx.Done():
		{
			err := ctx.Err()
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				return err // We'll check for context.DeadlineExceeded.
			}

			return err
		}
	}
}

// handleNotificationOnTrainingStoppedChan is called by doWaitForTrainingToEnd if a value is received on the target
// Client's TrainingStoppedChannel.
func (c *Client) handleNotificationOnTrainingStoppedChan(value interface{}, event *domain.Event,
	execReqMsgId string, timeoutInterval time.Duration, receivedAt time.Time) error {

	// e2eLatency := time.Since(time.UnixMilli(c.lastTrainingSubmittedAt.UnixMilli()))
	e2eLatency := receivedAt.Sub(time.UnixMilli(c.lastTrainingSubmittedAt.UnixMilli()))

	switch value.(type) {
	case error:
		{
			err := value.(error)
			c.logger.Warn("Session failed to stop training...",
				zap.String("workload_id", c.Workload.GetId()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.String("session_id", c.SessionId),
				zap.String("execute_request_msg_id", execReqMsgId),
				zap.Duration("timeout_interval", timeoutInterval),
				zap.Duration("e2e_latency", e2eLatency),
				zap.Error(err))

			return nil // to prevent workload from ending outright
		}
	case *trainingStoppedNotification:
		{
			notification := value.(*trainingStoppedNotification)
			c.handleTrainingEnded(event, notification, execReqMsgId, timeoutInterval, e2eLatency)

			return nil
		}
	default:
		{
			c.logger.Error("Received unexpected response via 'training-stopped' channel.",
				zap.String("workload_id", c.Workload.GetId()),
				zap.String("workload_name", c.Workload.WorkloadName()),
				zap.String("session_id", c.SessionId),
				zap.Duration("e2e_latency", e2eLatency),
				zap.Duration("timeout_interval", timeoutInterval),
				zap.String("execute_request_msg_id", execReqMsgId),
				zap.Any("response", value))

			return fmt.Errorf("unexpected response via 'training-stopped' channel")
		}
	}
}

// handleTrainingEnded is called by handleNotificationOnTrainingStoppedChan if the value received on the target
// Client's TrainingStoppedChannel is a jupyter.KernelMessage, which indicates that the training has successfully
// completed.
func (c *Client) handleTrainingEnded(event *domain.Event, notification *trainingStoppedNotification, execReqMsgId string,
	timeoutInterval time.Duration, e2eLatency time.Duration) {

	content := notification.Response.GetContent().(map[string]interface{})

	val := content["execution_start_unix_millis"]
	execStartedTimeUnixMillis := int64(val.(float64))

	val = content["execution_finished_unix_millis"]
	execEndedTimeUnixMillis := int64(val.(float64))

	execTimeMillis := execEndedTimeUnixMillis - execStartedTimeUnixMillis

	c.Workload.RecordSessionExecutionTime(c.SessionId, execTimeMillis)

	delay := notification.ReceivedAt.UnixMilli() - execEndedTimeUnixMillis

	c.Workload.UpdateStatistics(func(stats *Statistics) {
		stats.TotalReplyLatenciesMillis = append(stats.TotalReplyLatenciesMillis, delay)
		stats.TotalReplyLatencyMillis += delay

		stats.TotalExecuteRequestEndToEndLatencyMillis += e2eLatency.Milliseconds()
		stats.TotalExecuteRequestEndToEndLatenciesMillis = append(stats.TotalExecuteRequestEndToEndLatenciesMillis, e2eLatency.Milliseconds())
	})

	c.Workload.TrainingStopped(c.SessionId, event, c.convertCurrentTickTimestampToTickNumber())

	c.logger.Debug("Session stopped training",
		zap.String("session_id", c.SessionId),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.Int64("exec_time_millis", execTimeMillis),
		zap.Duration("e2e_latency", e2eLatency),
		zap.String("execute_request_msg_id", execReqMsgId),
		zap.Duration("timeout_interval", timeoutInterval),
		zap.Int32("training_events_handled", c.trainingEventsHandled.Load()),
		zap.Int("total_training_events_for_session", c.TotalNumTrainings()))
}

// stopSession instructs the Client to terminate its Jupyter kernel.
//
// stopSession is called while handling a "session-stopped" event and if the client times out while handling
// another event, such as "training-started" or "training-stopped".
func (c *Client) stopSession() {
	c.logger.Debug("Stopping session.",
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId))

	err := c.kernelSessionManager.StopKernel(c.SessionId)
	if err != nil {
		c.logger.Error("Error encountered while stopping session.",
			zap.String("workload_id", c.Workload.GetId()),
			zap.String("workload_name", c.Workload.WorkloadName()),
			zap.String("session_id", c.SessionId),
			zap.Error(err))

		// We won't return the error so it doesn't kill the whole workload
		// if this one Client/session has a problem.
	}

	c.logger.Debug(domain.ColorizeText("Successfully stopped session.", domain.LightGreen),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId))

	// Attempt to update the Prometheus metrics for Session lifetime duration (in seconds).
	sessionLifetimeDuration := time.Since(c.Session.GetCreatedAt())
	metrics.PrometheusMetricsWrapperInstance.WorkloadSessionLifetimeSeconds.
		With(prometheus.Labels{"workload_id": c.Workload.GetId()}).
		Observe(sessionLifetimeDuration.Seconds())

	c.Workload.SessionStopped(c.SessionId)
}

// handleSessionStoppedEvent handles a domain.EventSessionStopped *domain.Event.
func (c *Client) handleSessionStoppedEvent() error {
	defer c.handledStopEvent.Store(true) // Even if there was an error, this Client is done.

	c.stopSession()

	c.logger.Debug(
		fmt.Sprintf("Handled \"%s\" event successfully.",
			domain.ColorizeText("session-stopped", domain.LightOrange)),
		zap.String("workload_id", c.Workload.GetId()),
		zap.String("workload_name", c.Workload.WorkloadName()),
		zap.String("session_id", c.SessionId))

	return nil
}
