package workload_test

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/mock_domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/mock_workload"
	"github.com/scusemua/workload-driver-react/m/v2/internal/storage"
	"github.com/scusemua/workload-driver-react/m/v2/pkg/jupyter"
	mockjupyter "github.com/scusemua/workload-driver-react/m/v2/pkg/jupyter/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"

	"github.com/scusemua/workload-driver-react/m/v2/internal/server/workload"
)

var (
	workloadDriverOpts = &domain.Configuration{
		TraceStep:                               60,
		GPUTraceFile:                            "",
		GPUTraceStep:                            60,
		GPUMappingFile:                          "",
		MaxSessionCpuFile:                       "",
		MaxSessionMemFile:                       "",
		MaxSessionGpuFile:                       "",
		MaxTaskCpuFile:                          "",
		MaxTaskMemFile:                          "",
		MaxTaskGpuFile:                          "",
		CPUTraceFile:                            "",
		CPUTraceStep:                            60,
		CPUMappingFile:                          "",
		CPUDowntime:                             "",
		MemTraceFile:                            "",
		MemTraceStep:                            60,
		MemMappingFile:                          "",
		FromMonth:                               "",
		ToMonth:                                 "",
		Output:                                  "",
		ClusterStatsOutput:                      "",
		OutputSessions:                          "",
		OutputTasks:                             "",
		OutputTaskIntervals:                     "",
		OutputReschedIntervals:                  "",
		InspectPod:                              "",
		InspectGPU:                              false,
		InspectionClass:                         "",
		Debug:                                   false,
		Verbose:                                 false,
		Seed:                                    0,
		LastTimestamp:                           0,
		EvictHostOnLastContainerStop:            0,
		WorkloadPresetsFilepath:                 "configs/workload-presets-file.yaml",
		WorkloadTemplatesFilepath:               "configs/workload-templates-file.yaml",
		WorkloadJobConfigFilepath:               "configs/workload-job-config.yaml",
		ExpectedOriginPort:                      8001,
		ExpectedOriginAddresses:                 "http://localhost,http://127.0.0.1",
		ClusterDashboardHandlerPort:             8078,
		DriverTimescale:                         0,
		MaxTaskDurationSec:                      0,
		ContinueUntilExplicitlyTerminated:       false,
		UseResourceCredits:                      false,
		InitialCreditBalance:                    0,
		ResourceCreditCPU:                       0,
		ResourceCreditGPU:                       0,
		ResourceCreditMemMB:                     0,
		ResourceCreditCostInUSD:                 0,
		HostReclaimer:                           "",
		IdleHostReclaimerBaseIntervalSec:        0,
		VariableIdleHostReclaimerMaxIntervalSec: 0,
		VariableIdleHostReclaimerTargetResource: "",
		VariableIdleHostReclaimerBaseResourceAmount:    0,
		VariableIdleHostReclaimerAllowScalingDown:      false,
		PushUpdateInterval:                             1,
		WorkloadDriverKernelSpec:                       "distributed",
		ConnectToKernelTimeoutMillis:                   60000,
		InstanceTypeSelector:                           "",
		ServerlessInstanceTypeSelector:                 "",
		NewMostLeastCPU_GPUThresholdSelector_Threshold: 0,
		ServerfulUserCostMultiplier:                    "",
		ServerfulProviderCostMultiplier:                "",
		ServerlessUserCostMultiplier:                   "",
		ServerlessProviderCostMultiplier:               "",
		ServerfulCostModel:                             "",
		FaasCostModel:                                  "",
		AdjustGpuReservations:                          false,
		UseOneYearReservedPricingForMinimumCapacity:    false,
		UseThreeYearReservedPricingForMinimumCapacity:  false,
		SimulateDataTransferLatency:                    false,
		SimulateDataTransferCost:                       false,
		BillUsersForNonActiveReplicas:                  false,
		NonActiveReplicaCostMultiplier:                 "",
		OnlyChargeMixedContainersWhileTraining:         false,
		LogOutputFile:                                  "",
		ScalingPolicy:                                  "",
		NumTrainingReplicas:                            0,
		SubscribedRatioUpdateInterval:                  0,
		ScalingFactor:                                  0,
		ScalingInterval:                                0,
		ScalingLimit:                                   0,
		MaximumHostsToReleaseAtOnce:                    0,
		ScalingOutEnaled:                               false,
		ScalingBufferSize:                              0,
		InCluster:                                      false,
		KernelQueryInterval:                            "60s",
		NodeQueryInterval:                              "120s",
		KernelSpecQueryInterval:                        "600s",
		KubeConfig:                                     "",
		GatewayAddress:                                 "localhost:8079",
		FrontendJupyterServerAddress:                   "127.0.0.1:8888",
		InternalJupyterServerAddress:                   "127.0.0.1:8888",
		JupyterServerBasePath:                          "/",
		ServerPort:                                     8000,
		WebsocketProxyPort:                             8077,
		AdminUser:                                      "admin",
		AdminPassword:                                  "12345",
		TokenValidDurationSec:                          3600,
		TokenRefreshIntervalSec:                        5400,
		BaseUrl:                                        "/",
		PrometheusEndpoint:                             "/metrics",
		WorkloadOutputDirectory:                        "./workload-output",
		WorkloadOutputIntervalSec:                      2,
		TimeCompressTrainingDurations:                  true,
		MaxClientSleepDuringInitSeconds:                1,
	}
)

func notifyClientThatTrainingFailedToStart(client *workload.Client, ename string, evalue string) {
	time.Sleep(time.Millisecond * time.Duration(randRange(4, 64)))

	if ename == "" {
		ename = "ErrInsufficientHosts"
	}

	if evalue == "" {
		evalue = "insufficient hosts available"
	}

	executeReplyTime := time.Now()
	executeReplyHeader := jupyter.NewKernelMessageHeaderBuilder().
		WithSession(client.SessionId).
		WithUsername(client.SessionId).
		WithDate(executeReplyTime).
		WithVersion(jupyter.VERSION).
		WithMessageId(uuid.NewString()).
		WithMessageType(jupyter.ExecuteReply).
		Build()

	executeRequestHeader := jupyter.NewKernelMessageHeaderBuilder().
		WithSession(client.SessionId).
		WithUsername(client.SessionId).
		WithDate(time.Now().Add(time.Millisecond * -50)).
		WithVersion(jupyter.VERSION).
		WithMessageId(uuid.NewString()).
		WithMessageType(jupyter.ExecuteRequest).
		Build()

	executeReplyMessage := jupyter.NewMessageBuilder().
		WithContent(map[string]interface{}{
			"status": "error",
			"ename":  ename,
			"evalue": evalue,
		}).
		WithHeader(executeReplyHeader).
		WithParentHeader(executeRequestHeader).
		Build()

	go func() {
		client.OnReceiveExecuteReply(executeReplyMessage)
	}()
}

func notifyClientThatTrainingFinished(client *workload.Client, execStart float64, execStop float64) {
	time.Sleep(time.Millisecond * time.Duration(randRange(4, 64)))

	executeReplyTime := time.Now()
	executeReplyHeader := jupyter.NewKernelMessageHeaderBuilder().
		WithSession(client.SessionId).
		WithUsername(client.SessionId).
		WithDate(executeReplyTime).
		WithVersion(jupyter.VERSION).
		WithMessageId(uuid.NewString()).
		WithMessageType(jupyter.ExecuteReply).
		Build()

	executeRequestHeader := jupyter.NewKernelMessageHeaderBuilder().
		WithSession(client.SessionId).
		WithUsername(client.SessionId).
		WithDate(time.Now().Add(time.Millisecond * -50)).
		WithVersion(jupyter.VERSION).
		WithMessageId(uuid.NewString()).
		WithMessageType(jupyter.ExecuteRequest).
		Build()

	executeReplyMessage := jupyter.NewMessageBuilder().
		WithContent(map[string]interface{}{
			"status":                         "ok",
			"execution_start_unix_millis":    execStart,
			"execution_finished_unix_millis": execStop,
		}).
		WithHeader(executeReplyHeader).
		WithParentHeader(executeRequestHeader).
		Build()

	go func() {
		client.OnReceiveExecuteReply(executeReplyMessage)
	}()
}

func notifyClientThatTrainingStarted(client *workload.Client) {
	time.Sleep(time.Millisecond * time.Duration(randRange(4, 64)))

	smrLeadTaskCreationTimestamp := time.Now()
	smrLeadTaskMessageHeader := jupyter.NewKernelMessageHeaderBuilder().
		WithSession(client.SessionId).
		WithUsername(client.SessionId).
		WithDate(smrLeadTaskCreationTimestamp).
		WithVersion(jupyter.VERSION).
		WithMessageId(uuid.NewString()).
		WithMessageType(workload.SmrLeadTask).
		Build()

	executeRequestMessageHeader := jupyter.NewKernelMessageHeaderBuilder().
		WithSession(client.SessionId).
		WithUsername(client.SessionId).
		WithDate(time.Now().Add(time.Millisecond * -50)).
		WithVersion(jupyter.VERSION).
		WithMessageId(uuid.NewString()).
		WithMessageType(jupyter.ExecuteRequest).
		Build()

	smrLeadTaskMessage := jupyter.NewMessageBuilder().
		WithContent(map[string]interface{}{
			"status":                           "ok",
			"msg_created_at_unix_milliseconds": smrLeadTaskCreationTimestamp.UnixMilli(),
		}).
		WithHeader(smrLeadTaskMessageHeader).
		WithParentHeader(executeRequestMessageHeader).
		Build()

	go func() {
		client.HandleIOPubMessage(smrLeadTaskMessage)
	}()
}

var _ = Describe("Workload Driver Tests", func() {
	Context("Driving workloads", func() {
		var (
			controller           *gomock.Controller
			mockWebsocket        *mock_domain.MockConcurrentWebSocket
			mockCallbackProvider *mock_workload.MockCallbackProvider
			mockKernelManager    *mockjupyter.MockKernelSessionManager

			managerMetadata     map[string]interface{}
			managerMetadataLock sync.Mutex

			remoteStorageDefinition   *proto.RemoteStorageDefinition
			workloadDriver            *workload.Driver
			timescaleAdjustmentFactor float64
			atom                      zap.AtomicLevel
		)

		Context("Static scheduling", func() {
			// Create and register the workload that is used for the unit tests within this Context.
			createAndRegisterWorkload := func(sessionId string, sessionMetadata domain.SessionMetadata) {
				resourceRequest := domain.NewResourceRequest(128, 512, 1, 1, "AnyGPU")
				session := domain.NewWorkloadSession(sessionId, sessionMetadata, resourceRequest, time.UnixMilli(0), &atom)
				workloadTemplateSession := domain.NewWorkloadTemplateSession(session, 0, 8)
				workloadTemplateSession.AddTraining(1, 2, 128, 512, 1, []float64{100})

				workloadRegistrationRequest := &domain.WorkloadRegistrationRequest{
					AdjustGpuReservations:     false,
					WorkloadName:              "TestWorkload",
					DebugLogging:              true,
					Sessions:                  []*domain.WorkloadTemplateSession{workloadTemplateSession},
					TemplateFilePath:          "",
					Type:                      "template",
					Key:                       "TestWorkload",
					Seed:                      0,
					TimescaleAdjustmentFactor: timescaleAdjustmentFactor,
					RemoteStorageDefinition:   remoteStorageDefinition,
					SessionsSamplePercentage:  1.0,
				}

				currWorkload, err := workloadDriver.RegisterWorkload(workloadRegistrationRequest)
				Expect(err).To(BeNil())
				Expect(currWorkload).ToNot(BeNil())
				Expect(currWorkload.IsReady()).To(BeTrue())
				Expect(currWorkload.WorkloadName()).To(Equal("TestWorkload"))
				Expect(currWorkload.GetId()).To(Equal(workloadDriver.ID()))
			}

			BeforeEach(func() {
				managerMetadata = make(map[string]interface{})

				controller = gomock.NewController(GinkgoT())
				mockWebsocket = mock_domain.NewMockConcurrentWebSocket(controller)
				mockKernelManager = mockjupyter.NewMockKernelSessionManager(controller)

				mockCallbackProvider = mock_workload.NewMockCallbackProvider(controller)
				mockCallbackProvider.EXPECT().GetSchedulingPolicy().AnyTimes().Return("static", true)
				mockCallbackProvider.EXPECT().HandleWorkloadError(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(workloadId string, err error) {
					fmt.Printf("[ERROR] Workload '%s' encountered error: %v\n", workloadId, err)
				})

				timescaleAdjustmentFactor = 1 / 90 // 0.01667

				atom = zap.NewAtomicLevelAt(zap.DebugLevel)

				var err error
				workloadDriver, err = workload.NewBasicWorkloadDriver(workloadDriverOpts, true, timescaleAdjustmentFactor,
					mockWebsocket, &atom, mockCallbackProvider, &domain.WorkloadJobConfiguration{
						Models: []*domain.ModelConfig{
							{
								Type: "Computer Vision (CV)",
								Name: "ResNet-18",
							},
						},
						Datasets: []*domain.DatasetConfig{
							{
								Type: "Computer Vision (CV)",
								Name: "CIFAR-10",
							},
						},
					})
				Expect(err).To(BeNil())

				workloadDriver.OutputCsvDisabled = true
				workloadDriver.KernelManager = mockKernelManager

				remoteStorageDefinition = storage.NewRemoteStorageBuilder().
					WithName("AWS S3").
					WithDownloadRate(200_000_000).
					WithUploadRate(50_000_000).
					WithDownloadVariancePercent(0).
					WithUploadVariancePercent(0).
					WithWriteFailureChancePercentage(0).
					WithReadFailureChancePercentage(0).
					Build()

				Expect(workloadDriver).ToNot(BeNil())

				mockKernelManager.EXPECT().AddMetadata(gomock.Any(), gomock.Any()).AnyTimes().Do(func(key string, val interface{}) {
					managerMetadataLock.Lock()
					defer managerMetadataLock.Unlock()

					managerMetadata[key] = val
				})

				mockKernelManager.EXPECT().GetMetadata(gomock.Any()).AnyTimes().Do(func(key string) interface{} {
					managerMetadataLock.Lock()
					defer managerMetadataLock.Unlock()

					return managerMetadata[key]
				})
			})

			Context("Resubmitting failed events", func() {
				It("Will correctly resubmit a failed 'session-started' event", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
					mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

					var firstCreateSessionAttemptWg1, firstCreateSessionAttemptWg2 sync.WaitGroup
					firstCreateSessionAttemptWg1.Add(1)
					firstCreateSessionAttemptWg2.Add(1)

					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							fmt.Println("\nKernelSessionManager::CreateSession has been called for the first time.")

							// Tell the main goroutine that we've called KernelSessionManager::CreateSession.
							firstCreateSessionAttemptWg1.Done()

							fmt.Println("\nWaiting for next mocked call to KernelSessionManager::CreateSession to be set up before returning from first call.")

							// Wait for the main goroutine to set up the next expected call to KernelSessionManager::CreateSession.
							firstCreateSessionAttemptWg2.Wait()

							fmt.Println("\nReturning from KernelSessionManager::CreateSession for the first time with an error.")

							return nil, fmt.Errorf("insufficient hosts available")
						})

					var kernelStoppedWg sync.WaitGroup
					kernelStoppedWg.Add(1)

					mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
						kernelStoppedWg.Done()
						return nil
					})

					createAndRegisterWorkload(sessionId, sessionMetadata)

					var execStartedTimeUnixMillis int64
					clientChan := make(chan *workload.Client, 1)
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						client, loaded := workloadDriver.Clients[sessionId]
						Expect(loaded).To(BeTrue())
						Expect(client).ToNot(BeNil())

						clientChan <- client
						execStartedTimeUnixMillis = time.Now().UnixMilli()

						return nil, nil
					})

					err := workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg1.Wait()

					fmt.Println("\nSetting up second mocked call to KernelSessionManager::CreateSession now.")

					var secondCreateSessionAttemptWg sync.WaitGroup
					secondCreateSessionAttemptWg.Add(1)

					// Set up the next expected call.
					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							// This time, we'll return successfully.
							sessionConn := &jupyter.SessionConnection{
								Kernel: mockKernelConnection,
							}

							secondCreateSessionAttemptWg.Done()

							fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

							return sessionConn, nil
						})

					firstCreateSessionAttemptWg2.Done()

					secondCreateSessionAttemptWg.Wait()

					client := <-clientChan
					Expect(client).ToNot(BeNil())

					Expect(workloadDriver.GetWorkload().Statistics.NumActiveTrainings).To(Equal(int64(0)))

					notifyClientThatTrainingStarted(client)

					// This could theoretically fail in a race, but 5 seconds
					// should be plenty long enough for the condition to become true.
					Eventually(func() int64 {
						return workloadDriver.GetWorkload().Statistics.NumActiveTrainings
					}).
						WithTimeout(time.Second * 2).
						WithPolling(time.Millisecond * 250).
						Should(Equal(int64(1)))

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					//client.TrainingStoppedChannel <- &jupyter.Message{
					//	Header: &jupyter.KernelMessageHeader{
					//		MessageId:   uuid.NewString(),
					//		MessageType: jupyter.ExecuteReply,
					//		Date:        time.Now().String(),
					//	},
					//	Content: map[string]interface{}{
					//		"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
					//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
					//	},
					//}
					notifyClientThatTrainingFinished(client, float64(execStartedTimeUnixMillis), float64(execStopTimeUnixMillis))

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(2)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
				})

				It("Will correctly resubmit failed a 'training-started' event", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
					mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

					var kernelStartedWg sync.WaitGroup
					kernelStartedWg.Add(1)

					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							sessionConn := &jupyter.SessionConnection{
								Kernel: mockKernelConnection,
							}

							kernelStartedWg.Done()

							return sessionConn, nil
						})

					var kernelStoppedWg sync.WaitGroup
					kernelStoppedWg.Add(1)

					mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
						kernelStoppedWg.Done()
						return nil
					})

					createAndRegisterWorkload(sessionId, sessionMetadata)

					clientChan := make(chan *workload.Client, 1)
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						client, loaded := workloadDriver.Clients[sessionId]
						Expect(loaded).To(BeTrue())
						Expect(client).ToNot(BeNil())

						clientChan <- client

						return nil, nil
					})

					err := workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					kernelStartedWg.Wait()

					client := <-clientChan
					Expect(client).ToNot(BeNil())

					notifyClientThatTrainingFailedToStart(client, "", "")

					var trainingResubmittedWg sync.WaitGroup
					trainingResubmittedWg.Add(1)

					var execStartedTimeUnixMillis int64
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						execStartedTimeUnixMillis = time.Now().UnixMilli()
						trainingResubmittedWg.Done()
						return nil, nil
					})

					trainingResubmittedWg.Wait()

					Expect(workloadDriver.GetWorkload().Statistics.NumActiveTrainings).To(Equal(int64(0)))

					notifyClientThatTrainingStarted(client)

					// This could theoretically fail in a race, but 5 seconds
					// should be plenty long enough for the condition to become true.
					Eventually(func() int64 {
						return workloadDriver.GetWorkload().Statistics.NumActiveTrainings
					}).
						WithTimeout(time.Second * 2).
						WithPolling(time.Millisecond * 250).
						Should(Equal(int64(1)))

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					//client.TrainingStoppedChannel <- &jupyter.Message{
					//	Header: &jupyter.KernelMessageHeader{
					//		MessageId:   uuid.NewString(),
					//		MessageType: jupyter.ExecuteReply,
					//		Date:        time.Now().String(),
					//	},
					//	Content: map[string]interface{}{
					//		"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
					//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
					//	},
					//}
					notifyClientThatTrainingFinished(client, float64(execStartedTimeUnixMillis), float64(execStopTimeUnixMillis))

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(1)))
				})

				It("Will correctly resubmit both a failed 'session-started' event and a failed 'training' event", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
					mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

					var firstCreateSessionAttemptWg1, firstCreateSessionAttemptWg2 sync.WaitGroup
					firstCreateSessionAttemptWg1.Add(1)
					firstCreateSessionAttemptWg2.Add(1)

					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							fmt.Println("\nKernelSessionManager::CreateSession has been called for the first time.")

							// Tell the main goroutine that we've called KernelSessionManager::CreateSession.
							firstCreateSessionAttemptWg1.Done()

							fmt.Println("\nWaiting for next mocked call to KernelSessionManager::CreateSession to be set up before returning from first call.")

							// Wait for the main goroutine to set up the next expected call to KernelSessionManager::CreateSession.
							firstCreateSessionAttemptWg2.Wait()

							fmt.Println("\nReturning from KernelSessionManager::CreateSession for the first time with an error.")

							return nil, fmt.Errorf("insufficient hosts available")
						})

					var kernelStoppedWg sync.WaitGroup
					kernelStoppedWg.Add(1)

					mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
						kernelStoppedWg.Done()
						return nil
					})

					createAndRegisterWorkload(sessionId, sessionMetadata)

					clientChan := make(chan *workload.Client, 1)
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						client, loaded := workloadDriver.Clients[sessionId]
						Expect(loaded).To(BeTrue())
						Expect(client).ToNot(BeNil())

						clientChan <- client

						return nil, nil
					})

					err := workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg1.Wait()

					fmt.Println("\nSetting up second mocked call to KernelSessionManager::CreateSession now.")

					var secondCreateSessionAttemptWg sync.WaitGroup
					secondCreateSessionAttemptWg.Add(1)

					// Set up the next expected call.
					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							// This time, we'll return successfully.
							sessionConn := &jupyter.SessionConnection{
								Kernel: mockKernelConnection,
							}

							secondCreateSessionAttemptWg.Done()

							fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

							return sessionConn, nil
						})

					firstCreateSessionAttemptWg2.Done()

					secondCreateSessionAttemptWg.Wait()

					client := <-clientChan
					Expect(client).ToNot(BeNil())

					notifyClientThatTrainingFailedToStart(client, "", "")

					var trainingResubmittedWg sync.WaitGroup
					trainingResubmittedWg.Add(1)

					var execStartedTimeUnixMillis int64
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						execStartedTimeUnixMillis = time.Now().UnixMilli()
						trainingResubmittedWg.Done()
						return nil, nil
					})

					trainingResubmittedWg.Wait()

					Expect(workloadDriver.GetWorkload().Statistics.NumActiveTrainings).To(Equal(int64(0)))

					notifyClientThatTrainingStarted(client)

					// This could theoretically fail in a race, but 5 seconds
					// should be plenty long enough for the condition to become true.
					Eventually(func() int64 {
						return workloadDriver.GetWorkload().Statistics.NumActiveTrainings
					}).
						WithTimeout(time.Second * 2).
						WithPolling(time.Millisecond * 250).
						Should(Equal(int64(1)))

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					//client.TrainingStoppedChannel <- &jupyter.Message{
					//	Header: &jupyter.KernelMessageHeader{
					//		MessageId:   uuid.NewString(),
					//		MessageType: jupyter.ExecuteReply,
					//		Date:        time.Now().String(),
					//	},
					//	Content: map[string]interface{}{
					//		"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
					//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
					//	},
					//}
					notifyClientThatTrainingFinished(client, float64(execStartedTimeUnixMillis), float64(execStopTimeUnixMillis))

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(2)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(1)))
				})

				It("Will repeatedly resubmit a failed 'session-started' event multiple times", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
					mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

					var kernelStoppedWg sync.WaitGroup
					kernelStoppedWg.Add(1)

					mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
						kernelStoppedWg.Done()
						return nil
					})

					var execStartedTimeUnixMillis int64
					clientChan := make(chan *workload.Client, 1)
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						client, loaded := workloadDriver.Clients[sessionId]
						Expect(loaded).To(BeTrue())
						Expect(client).ToNot(BeNil())

						clientChan <- client
						execStartedTimeUnixMillis = time.Now().UnixMilli()

						return nil, nil
					})

					createAndRegisterWorkload(sessionId, sessionMetadata)

					var firstCreateSessionAttemptWg1, firstCreateSessionAttemptWg2 sync.WaitGroup
					firstCreateSessionAttemptWg1.Add(1)
					firstCreateSessionAttemptWg2.Add(1)

					setupFailedCreateAttempt := func(attemptNumber int) {
						mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								fmt.Printf("\nKernelSessionManager::CreateSession has been called (and will fail) on attempt #%d\n", attemptNumber)

								// Tell the main goroutine that we've called KernelSessionManager::CreateSession.
								firstCreateSessionAttemptWg1.Done()

								fmt.Printf("\nWaiting for next mocked call to KernelSessionManager::CreateSession to be set up before returning on attempt #%d\n", attemptNumber)

								// Wait for the main goroutine to set up the next expected call to KernelSessionManager::CreateSession.
								firstCreateSessionAttemptWg2.Wait()

								// Reset the barrier for the next time we call this.
								firstCreateSessionAttemptWg2.Add(1)

								fmt.Printf("\nReturning from KernelSessionManager::CreateSession with an error on attempt #%d\n", attemptNumber)

								return nil, fmt.Errorf("insufficient hosts available")
							})
					}

					// Set up the first failed attempt.
					setupFailedCreateAttempt(1)

					// Start the workload.
					err := workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					for i := 1; i < 5; i++ {
						// Wait for KernelSessionManager::CreateSession to be called.
						firstCreateSessionAttemptWg1.Wait()

						// Set up the next failed attempt.
						setupFailedCreateAttempt(i + 1)

						// Reset this barrier.
						firstCreateSessionAttemptWg1.Add(1)

						// Let the caller of KernelSessionManager::CreateSession return.
						firstCreateSessionAttemptWg2.Done()
					}

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg1.Wait()

					// Let the caller of KernelSessionManager::CreateSession return.
					firstCreateSessionAttemptWg2.Done()

					fmt.Println("\nSetting up successful mocked call to KernelSessionManager::CreateSession now.")

					var secondCreateSessionAttemptWg sync.WaitGroup
					secondCreateSessionAttemptWg.Add(1)

					// Set up the next expected call.
					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							// This time, we'll return successfully.
							sessionConn := &jupyter.SessionConnection{
								Kernel: mockKernelConnection,
							}

							secondCreateSessionAttemptWg.Done()

							fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

							return sessionConn, nil
						})

					secondCreateSessionAttemptWg.Wait()

					client := <-clientChan
					Expect(client).ToNot(BeNil())

					notifyClientThatTrainingStarted(client)

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					//client.TrainingStoppedChannel <- &jupyter.Message{
					//	Header: &jupyter.KernelMessageHeader{
					//		MessageId:   uuid.NewString(),
					//		MessageType: jupyter.ExecuteReply,
					//		Date:        time.Now().String(),
					//	},
					//	Content: map[string]interface{}{
					//		"status":                         "ok",
					//		"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
					//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
					//	},
					//}
					notifyClientThatTrainingFinished(client, float64(execStartedTimeUnixMillis), float64(execStopTimeUnixMillis))

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(6)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
				})

				It("Will correctly resubmit a failed 'training-started' event multiple times", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
					mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

					var kernelStartedWg sync.WaitGroup
					kernelStartedWg.Add(1)

					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							sessionConn := &jupyter.SessionConnection{
								Kernel: mockKernelConnection,
							}

							kernelStartedWg.Done()

							return sessionConn, nil
						})

					var kernelStoppedWg sync.WaitGroup
					kernelStoppedWg.Add(1)

					mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
						kernelStoppedWg.Done()
						return nil
					})

					createAndRegisterWorkload(sessionId, sessionMetadata)

					clientChan := make(chan *workload.Client, 1)
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						client, loaded := workloadDriver.Clients[sessionId]
						Expect(loaded).To(BeTrue())
						Expect(client).ToNot(BeNil())

						clientChan <- client

						return nil, nil
					})

					err := workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					kernelStartedWg.Wait()

					client := <-clientChan
					Expect(client).ToNot(BeNil())

					notifyClientThatTrainingFailedToStart(client, "", "")

					var trainingResubmittedWg sync.WaitGroup
					trainingResubmittedWg.Add(1)

					var execStartedTimeUnixMillis int64
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(6).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						execStartedTimeUnixMillis = time.Now().UnixMilli()
						trainingResubmittedWg.Done()
						return nil, nil
					})
					for i := 0; i < 5; i++ {
						// Wait for RequestExecute to be called.
						trainingResubmittedWg.Wait()

						// Reset the WaitGroup.
						trainingResubmittedWg.Add(1)

						// Send the notification that the training failed to start.
						notifyClientThatTrainingFailedToStart(client, "", "")
					}

					// Wait for RequestExecute to be called.
					trainingResubmittedWg.Wait()

					// Send the notification that the training started correctly.
					notifyClientThatTrainingStarted(client)

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					//client.TrainingStoppedChannel <- &jupyter.Message{
					//	Header: &jupyter.KernelMessageHeader{
					//		MessageId:   uuid.NewString(),
					//		MessageType: jupyter.ExecuteReply,
					//		Date:        time.Now().String(),
					//	},
					//	Content: map[string]interface{}{
					//		"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
					//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
					//	},
					//}
					notifyClientThatTrainingFinished(client, float64(execStartedTimeUnixMillis), float64(execStopTimeUnixMillis))

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(6)))
				})

				It("Will successfully handle a workload with 16 sessions in which 8 fail to be created initially", func() {
					numSessions := 16
					numFailFirstTime := numSessions / 2

					sessionIds := make([]string, 0, numSessions)
					sessionMetadatas := make([]domain.SessionMetadata, 0, numSessions)
					mockKernelConnections := make([]*mockjupyter.MockKernelConnection, 0, numSessions)
					workloadTemplateSessions := make([]*domain.WorkloadTemplateSession, 0, numSessions)

					var firstCreateSessionAttemptWg, secondCreateSessionAttemptWg sync.WaitGroup
					var kernelStoppedWg sync.WaitGroup

					clientChannel := make(chan *workload.Client, numSessions)

					execStartTimes := make(map[string]int64)
					var execStartTimeMutex sync.Mutex

					firstFailClients := make([]*workload.Client, 0, numFailFirstTime)
					firstFailClientsMutex := sync.Mutex{}

					firstPassClients := make([]*workload.Client, 0, numSessions-numFailFirstTime)
					firstPassClientsMutex := sync.Mutex{}

					for i := 0; i < numFailFirstTime; i++ {
						sessionId := uuid.NewString()
						sessionIds = append(sessionIds, sessionId)
						sessionMetadata := getBasicSessionMetadata(sessionId, controller)
						sessionMetadatas = append(sessionMetadatas, sessionMetadata)

						mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
						mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
						mockKernelConnection.EXPECT().KernelId().AnyTimes().Return(sessionId)

						mockKernelConnections = append(mockKernelConnections, mockKernelConnection)

						kernelStoppedWg.Add(1)
						firstCreateSessionAttemptWg.Add(1)
						secondCreateSessionAttemptWg.Add(1)

						failedCall := mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								fmt.Printf("\nKernelSessionManager::CreateSession has been called (and will fail) on attempt #%d\n", 1)

								// Tell the main goroutine that we've called KernelSessionManager::CreateSession.
								firstCreateSessionAttemptWg.Done()

								fmt.Printf("\nWaiting for next mocked call to KernelSessionManager::CreateSession to be set up before returning on attempt #%d\n", 1)

								fmt.Printf("\nReturning from KernelSessionManager::CreateSession with an error on attempt #%d\n", 1)

								return nil, fmt.Errorf("insufficient hosts available")
							})

						// Set up the next expected call.
						mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								// This time, we'll return successfully.
								sessionConn := &jupyter.SessionConnection{
									Kernel: mockKernelConnection,
								}

								secondCreateSessionAttemptWg.Done()

								fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

								return sessionConn, nil
							}).After(failedCall)

						resourceRequest := domain.NewResourceRequest(128, 512, 1, 1, "AnyGPU")
						session := domain.NewWorkloadSession(sessionId, sessionMetadata, resourceRequest, time.UnixMilli(0), &atom)
						workloadTemplateSession := domain.NewWorkloadTemplateSession(session, 0, 8)
						workloadTemplateSession.AddTraining(1, 2, 128, 512, 1, []float64{100})
						workloadTemplateSessions = append(workloadTemplateSessions, workloadTemplateSession)

						mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
							kernelStoppedWg.Done()
							return nil
						})

						mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
							client, loaded := workloadDriver.Clients[sessionId]
							Expect(loaded).To(BeTrue())
							Expect(client).ToNot(BeNil())

							clientChannel <- client

							execStartTimeMutex.Lock()
							execStartTimes[sessionId] = time.Now().UnixMilli()
							execStartTimeMutex.Unlock()

							firstFailClientsMutex.Lock()
							firstFailClients = append(firstFailClients, client)
							firstFailClientsMutex.Unlock()

							return nil, nil
						})
					}

					for i := numFailFirstTime; i < numSessions; i++ {
						sessionId := uuid.NewString()
						sessionIds = append(sessionIds, sessionId)
						sessionMetadata := getBasicSessionMetadata(sessionId, controller)
						sessionMetadatas = append(sessionMetadatas, sessionMetadata)

						mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
						mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
						mockKernelConnection.EXPECT().KernelId().AnyTimes().Return(sessionId)

						mockKernelConnections = append(mockKernelConnections, mockKernelConnection)

						kernelStoppedWg.Add(1)
						firstCreateSessionAttemptWg.Add(1)

						mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								// This time, we'll return successfully.
								sessionConn := &jupyter.SessionConnection{
									Kernel: mockKernelConnection,
								}

								firstCreateSessionAttemptWg.Done()

								fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

								return sessionConn, nil
							})

						resourceRequest := domain.NewResourceRequest(128, 512, 1, 1, "AnyGPU")
						session := domain.NewWorkloadSession(sessionId, sessionMetadata, resourceRequest, time.UnixMilli(0), &atom)
						workloadTemplateSession := domain.NewWorkloadTemplateSession(session, 0, 8)
						workloadTemplateSession.AddTraining(1, 2, 128, 512, 1, []float64{100})
						workloadTemplateSessions = append(workloadTemplateSessions, workloadTemplateSession)

						mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
							kernelStoppedWg.Done()
							return nil
						})

						mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
							client, loaded := workloadDriver.Clients[sessionId]
							Expect(loaded).To(BeTrue())
							Expect(client).ToNot(BeNil())

							clientChannel <- client

							execStartTimeMutex.Lock()
							execStartTimes[sessionId] = time.Now().UnixMilli()
							execStartTimeMutex.Unlock()

							firstPassClientsMutex.Lock()
							firstPassClients = append(firstPassClients, client)
							firstPassClientsMutex.Unlock()

							return nil, nil
						})
					}

					workloadRegistrationRequest := &domain.WorkloadRegistrationRequest{
						AdjustGpuReservations:     false,
						WorkloadName:              "TestWorkload",
						DebugLogging:              true,
						Sessions:                  workloadTemplateSessions,
						TemplateFilePath:          "",
						Type:                      "template",
						Key:                       "TestWorkload",
						Seed:                      0,
						TimescaleAdjustmentFactor: timescaleAdjustmentFactor,
						RemoteStorageDefinition:   remoteStorageDefinition,
						SessionsSamplePercentage:  1.0,
					}

					currWorkload, err := workloadDriver.RegisterWorkload(workloadRegistrationRequest)
					Expect(err).To(BeNil())
					Expect(currWorkload).ToNot(BeNil())
					Expect(currWorkload.IsReady()).To(BeTrue())
					Expect(currWorkload.WorkloadName()).To(Equal("TestWorkload"))
					Expect(currWorkload.GetId()).To(Equal(workloadDriver.ID()))

					err = workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					targetWorkload := workloadDriver.GetWorkload()
					Expect(targetWorkload).ToNot(BeNil())
					GinkgoWriter.Printf("State of workload %s (ID=%s): %s\n",
						targetWorkload.Name, targetWorkload.Id, targetWorkload.GetState().String())
					Expect(targetWorkload.IsRunning()).To(BeTrue())
					Expect(targetWorkload == currWorkload).To(BeTrue())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg.Wait()

					secondCreateSessionAttemptWg.Wait()

					clients := make([]*workload.Client, 0, numSessions)
					for i := 0; i < numSessions; i++ {
						client := <-clientChannel
						Expect(client).ToNot(BeNil())
						clients = append(clients, client)

						notifyClientThatTrainingStarted(client)
					}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					Expect(len(execStartTimes)).To(Equal(numSessions))

					for _, client := range clients {
						execStartTimeMutex.Lock()
						execStartTime := execStartTimes[client.SessionId]
						execStartTimeMutex.Unlock()

						//client.TrainingStoppedChannel <- &jupyter.Message{
						//	Header: &jupyter.KernelMessageHeader{
						//		MessageId:   uuid.NewString(),
						//		MessageType: jupyter.ExecuteReply,
						//		Date:        time.Now().String(),
						//	},
						//	Content: map[string]interface{}{
						//		"execution_start_unix_millis":    float64(execStartTime),
						//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						//	},
						//}
						notifyClientThatTrainingFinished(client, float64(execStartTime), float64(execStopTimeUnixMillis))
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					for _, client := range firstFailClients {
						Expect(client.NumSessionStartAttempts()).To(Equal(int32(2)))
						Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
						Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
					}

					for _, client := range firstPassClients {
						Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
						Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
						Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
					}
				})
			})

			Context("Basic workload execution", func() {
				It("Will successfully attempt to create a session", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)
					mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
					mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

					var firstCreateSessionAttemptWg sync.WaitGroup
					firstCreateSessionAttemptWg.Add(1)

					mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
						func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
							// This time, we'll return successfully.
							sessionConn := &jupyter.SessionConnection{
								Kernel: mockKernelConnection,
							}

							firstCreateSessionAttemptWg.Done()

							fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

							return sessionConn, nil
						})

					var kernelStoppedWg sync.WaitGroup
					kernelStoppedWg.Add(1)

					mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
						kernelStoppedWg.Done()
						return nil
					})

					createAndRegisterWorkload(sessionId, sessionMetadata)

					var execStartedTimeUnixMillis int64
					clientChan := make(chan *workload.Client, 1)
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						client, loaded := workloadDriver.Clients[sessionId]
						Expect(loaded).To(BeTrue())
						Expect(client).ToNot(BeNil())

						clientChan <- client
						execStartedTimeUnixMillis = time.Now().UnixMilli()

						return nil, nil
					})

					err := workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg.Wait()

					client := <-clientChan
					Expect(client).ToNot(BeNil())

					notifyClientThatTrainingStarted(client)

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					//client.TrainingStoppedChannel <- &jupyter.Message{
					//	Header: &jupyter.KernelMessageHeader{
					//		MessageId:   uuid.NewString(),
					//		MessageType: jupyter.ExecuteReply,
					//		Date:        time.Now().String(),
					//	},
					//	Content: map[string]interface{}{
					//		"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
					//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
					//	},
					//}
					notifyClientThatTrainingFinished(client, float64(execStartedTimeUnixMillis), float64(execStopTimeUnixMillis))

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
				})

				It("Will successfully handle a workload with 16 sessions that each train a single time", func() {
					numSessions := 16

					sessionIds := make([]string, 0, numSessions)
					sessionMetadatas := make([]domain.SessionMetadata, 0, numSessions)
					mockKernelConnections := make([]*mockjupyter.MockKernelConnection, 0, numSessions)
					workloadTemplateSessions := make([]*domain.WorkloadTemplateSession, 0, numSessions)

					var firstCreateSessionAttemptWg sync.WaitGroup
					var kernelStoppedWg sync.WaitGroup

					clientChannel := make(chan *workload.Client, numSessions)

					execStartTimes := make(map[string]int64)
					var execStartTimeMutex sync.Mutex

					for i := 0; i < numSessions; i++ {
						sessionId := uuid.NewString()
						sessionIds = append(sessionIds, sessionId)
						sessionMetadata := getBasicSessionMetadata(sessionId, controller)
						sessionMetadatas = append(sessionMetadatas, sessionMetadata)

						mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
						mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
						mockKernelConnection.EXPECT().KernelId().AnyTimes().Return(sessionId)

						mockKernelConnections = append(mockKernelConnections, mockKernelConnection)

						kernelStoppedWg.Add(1)
						firstCreateSessionAttemptWg.Add(1)

						mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								// This time, we'll return successfully.
								sessionConn := &jupyter.SessionConnection{
									Kernel: mockKernelConnection,
								}

								firstCreateSessionAttemptWg.Done()

								fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

								return sessionConn, nil
							})

						resourceRequest := domain.NewResourceRequest(128, 512, 1, 1, "AnyGPU")
						session := domain.NewWorkloadSession(sessionId, sessionMetadata, resourceRequest, time.UnixMilli(0), &atom)
						workloadTemplateSession := domain.NewWorkloadTemplateSession(session, 0, 8)
						workloadTemplateSession.AddTraining(1, 2, 128, 512, 1, []float64{100})
						workloadTemplateSessions = append(workloadTemplateSessions, workloadTemplateSession)

						mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
							kernelStoppedWg.Done()
							return nil
						})

						mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
							client, loaded := workloadDriver.Clients[sessionId]
							Expect(loaded).To(BeTrue())
							Expect(client).ToNot(BeNil())

							clientChannel <- client

							execStartTimeMutex.Lock()
							execStartTimes[sessionId] = time.Now().UnixMilli()
							execStartTimeMutex.Unlock()

							return nil, nil
						})
					}

					timescaleAdjustmentFactor = 1 / 90

					workloadRegistrationRequest := &domain.WorkloadRegistrationRequest{
						AdjustGpuReservations:     false,
						WorkloadName:              "TestWorkload",
						DebugLogging:              true,
						Sessions:                  workloadTemplateSessions,
						TemplateFilePath:          "",
						Type:                      "template",
						Key:                       "TestWorkload",
						Seed:                      0,
						TimescaleAdjustmentFactor: timescaleAdjustmentFactor,
						RemoteStorageDefinition:   remoteStorageDefinition,
						SessionsSamplePercentage:  1.0,
					}

					currWorkload, err := workloadDriver.RegisterWorkload(workloadRegistrationRequest)
					Expect(err).To(BeNil())
					Expect(currWorkload).ToNot(BeNil())
					Expect(currWorkload.IsReady()).To(BeTrue())
					Expect(currWorkload.WorkloadName()).To(Equal("TestWorkload"))
					Expect(currWorkload.GetId()).To(Equal(workloadDriver.ID()))
					Expect(currWorkload.TotalNumSessions()).To(Equal(numSessions))

					err = workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					targetWorkload := workloadDriver.GetWorkload()
					Expect(targetWorkload).ToNot(BeNil())
					GinkgoWriter.Printf("State of workload %s (ID=%s): %s\n",
						targetWorkload.Name, targetWorkload.Id, targetWorkload.GetState().String())
					Expect(targetWorkload.IsRunning()).To(BeTrue())
					Expect(targetWorkload == currWorkload).To(BeTrue())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg.Wait()
					GinkgoWriter.Printf("State of workload %s (ID=%s): %s\n",
						targetWorkload.Name, targetWorkload.Id, targetWorkload.GetState().String())
					Expect(targetWorkload.IsRunning()).To(BeTrue())

					clients := make([]*workload.Client, 0, numSessions)
					for i := 0; i < numSessions; i++ {
						client := <-clientChannel
						Expect(client).ToNot(BeNil())
						clients = append(clients, client)

						Expect(workloadDriver.GetWorkload().Statistics.NumActiveTrainings).To(Equal(int64(i)))

						notifyClientThatTrainingStarted(client)

						// This could theoretically fail in a race, but 5 seconds
						// should be plenty long enough for the condition to become true.
						Eventually(func() int64 {
							return targetWorkload.Statistics.NumActiveTrainings
						}).
							WithTimeout(time.Second * 2).
							WithPolling(time.Millisecond * 250).
							Should(Equal(int64(i + 1)))
					}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					Expect(len(execStartTimes)).To(Equal(numSessions))

					for _, client := range clients {
						execStartTimeMutex.Lock()
						execStartTime := execStartTimes[client.SessionId]
						execStartTimeMutex.Unlock()

						//client.TrainingStoppedChannel <- &jupyter.Message{
						//	Header: &jupyter.KernelMessageHeader{
						//		MessageId:   uuid.NewString(),
						//		MessageType: jupyter.ExecuteReply,
						//		Date:        time.Now().String(),
						//	},
						//	Content: map[string]interface{}{
						//		"execution_start_unix_millis":    float64(execStartTime),
						//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						//	},
						//}
						notifyClientThatTrainingFinished(client, float64(execStartTime), float64(execStopTimeUnixMillis))
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					for _, client := range clients {
						Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
						Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
						Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
					}

					Expect(targetWorkload.WorkloadName()).To(Equal("TestWorkload"))
					Expect(targetWorkload.GetId()).To(Equal(workloadDriver.ID()))
					GinkgoWriter.Printf("State of workload %s (ID=%s): %s\n",
						targetWorkload.Name, targetWorkload.Id, targetWorkload.GetState().String())
					Expect(targetWorkload.IsFinished()).To(BeTrue())

					sessions := targetWorkload.Sessions
					Expect(len(sessions)).To(Equal(numSessions))
					Expect(targetWorkload.TotalNumSessions()).To(Equal(numSessions))

					statistics := targetWorkload.GetStatistics()
					Expect(statistics).ToNot(BeNil())
				})

				It("Will correctly discard sessions with no training events when instructed to do so", func() {

				})

				It("Will successfully handle a workload with 16 sessions that each train multiple times", func() {
					numSessions := 16
					numTrainingsPerSession := []int{3, 2, 5, 4, 8, 5, 3, 9, 7, 6, 8, 2, 5, 3, 4, 7}
					totalNumTrainings := 0
					for _, numTrainings := range numTrainingsPerSession {
						totalNumTrainings += numTrainings
					}
					firstTrainingTicks := []int{1, 5, 1, 6, 2, 3, 4, 6, 3, 4, 2, 2, 5, 1, 4, 3}

					sessionIds := make([]string, 0, numSessions)
					sessionMetadatas := make([]domain.SessionMetadata, 0, numSessions)
					mockKernelConnections := make([]*mockjupyter.MockKernelConnection, 0, numSessions)
					workloadTemplateSessions := make([]*domain.WorkloadTemplateSession, 0, numSessions)

					var firstCreateSessionAttemptWg sync.WaitGroup
					var kernelStoppedWg sync.WaitGroup

					clientChannels := make([]chan *workload.Client, 0, numSessions)
					clients := make([]*workload.Client, numSessions)
					for i := 0; i < numSessions; i++ {
						channel := make(chan *workload.Client)
						clientChannels = append(clientChannels, channel)
					}

					clientsMutex := sync.Mutex{}

					for i := 0; i < numSessions; i++ {
						sessionId := fmt.Sprintf("TestSession%d", i)
						sessionIds = append(sessionIds, sessionId)
						sessionMetadata := getBasicSessionMetadata(sessionId, controller)
						sessionMetadatas = append(sessionMetadatas, sessionMetadata)

						mockKernelConnection := mockjupyter.NewMockKernelConnection(controller)
						mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
						mockKernelConnection.EXPECT().KernelId().AnyTimes().Return(sessionId)

						mockKernelConnections = append(mockKernelConnections, mockKernelConnection)

						kernelStoppedWg.Add(1)
						firstCreateSessionAttemptWg.Add(1)

						mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								// This time, we'll return successfully.
								sessionConn := &jupyter.SessionConnection{
									Kernel: mockKernelConnection,
								}

								firstCreateSessionAttemptWg.Done()

								fmt.Println("Returning from KernelSessionManager::CreateSession for the second time.")

								return sessionConn, nil
							})

						maxCpu := rand.Intn(63800) + 100
						maxMemMb := rand.Intn(127800) + 1
						maxGpus := rand.Intn(7) + 1
						maxVram := rand.Intn(31) + 1

						resourceRequest := domain.NewResourceRequest(float64(maxCpu), float64(maxMemMb), maxGpus, float64(maxVram), "AnyGPU")
						session := domain.NewWorkloadSession(sessionId, sessionMetadata, resourceRequest, time.UnixMilli(0), &atom)

						firstTrainingTick := firstTrainingTicks[i]
						startTick := rand.Intn(firstTrainingTick)
						workloadTemplateSession := domain.NewWorkloadTemplateSession(session, startTick, 8)

						numTrainings := numTrainingsPerSession[i]
						tick := firstTrainingTick
						for j := 0; j < numTrainings; j++ {
							cpu := rand.Intn(maxCpu)
							memMb := rand.Intn(maxMemMb)
							gpus := rand.Intn(maxGpus)
							vram := rand.Intn(maxVram)

							gpuUtilizations := make([]float64, 0, gpus)
							for k := 0; k < gpus; k++ {
								gpuUtilization := rand.Intn(100)
								gpuUtilizations = append(gpuUtilizations, float64(gpuUtilization))
							}

							duration := rand.Intn(6) + 1
							workloadTemplateSession.AddTraining(tick, duration, float64(cpu), float64(memMb), float64(vram), gpuUtilizations)
							tick += duration + 1
						}

						workloadTemplateSession.StopTick = tick + 1

						fmt.Printf("Session '%s' will finish at tick %d.\n", sessionId, tick)

						workloadTemplateSessions = append(workloadTemplateSessions, workloadTemplateSession)

						mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
							kernelStoppedWg.Done()
							fmt.Printf("Session \"%s\" is stopping.\n", sessionId)
							return nil
						})

						workloadDriverClientsMutex := sync.Mutex{}
						firstCall := mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
							workloadDriverClientsMutex.Lock()
							client, loaded := workloadDriver.Clients[sessionId]
							workloadDriverClientsMutex.Unlock()

							Expect(loaded).To(BeTrue())
							Expect(client).ToNot(BeNil())

							clientsMutex.Lock()
							channel := clientChannels[i]
							clientsMutex.Unlock()

							channel <- client

							return nil, nil
						})

						mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(numTrainings - 1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
							return nil, nil
						}).After(firstCall)
					}

					workloadRegistrationRequest := &domain.WorkloadRegistrationRequest{
						AdjustGpuReservations:     false,
						WorkloadName:              "TestWorkload",
						DebugLogging:              true,
						Sessions:                  workloadTemplateSessions,
						TemplateFilePath:          "",
						Type:                      "template",
						Key:                       "TestWorkload",
						Seed:                      0,
						TimescaleAdjustmentFactor: timescaleAdjustmentFactor,
						RemoteStorageDefinition:   remoteStorageDefinition,
						SessionsSamplePercentage:  1.0,
					}

					currWorkload, err := workloadDriver.RegisterWorkload(workloadRegistrationRequest)
					Expect(err).To(BeNil())
					Expect(currWorkload).ToNot(BeNil())
					Expect(currWorkload.IsReady()).To(BeTrue())
					Expect(currWorkload.WorkloadName()).To(Equal("TestWorkload"))
					Expect(currWorkload.GetId()).To(Equal(workloadDriver.ID()))

					err = workloadDriver.StartWorkload()
					Expect(err).To(BeNil())

					targetWorkload := workloadDriver.GetWorkload()
					Expect(targetWorkload).ToNot(BeNil())
					GinkgoWriter.Printf("State of workload %s (ID=%s): %s\n",
						targetWorkload.Name, targetWorkload.Id, targetWorkload.GetState().String())
					Expect(targetWorkload.IsRunning()).To(BeTrue())
					Expect(targetWorkload == currWorkload).To(BeTrue())

					go workloadDriver.ProcessWorkloadEvents()
					go workloadDriver.DriveWorkload()

					// Wait for KernelSessionManager::CreateSession to be called.
					firstCreateSessionAttemptWg.Wait()

					trainingStartTimeChannels := make([]chan time.Time, 0, numSessions)
					for i := 0; i < numSessions; i++ {
						trainingStartTimeChannels = append(trainingStartTimeChannels, make(chan time.Time))
					}

					// Training start.
					for i := 0; i < numSessions; i++ {
						channel := clientChannels[i]
						Expect(channel).ToNot(BeNil())

						client := <-channel
						Expect(client).ToNot(BeNil())

						clients[i] = client

						numTrainings := numTrainingsPerSession[i]
						go func(sessionIndex int) {
							trainingStartTimeChannel := trainingStartTimeChannels[sessionIndex]
							workloadTemplateSession := workloadTemplateSessions[sessionIndex]
							for j := 0; j < numTrainings; j++ {
								training := workloadTemplateSession.TrainingEvents[j]
								trainingDuration := training.DurationInTicks

								notifyClientThatTrainingStarted(client)

								trainingStartTimeChannel <- time.Now()
								fmt.Printf("Submitted training %d/%d (duration=%d ticks) for session \"%s\"\n", j+1, numTrainings, trainingDuration, workloadTemplateSession.Id)
								time.Sleep(time.Duration(float64(trainingDuration) * float64(time.Minute) * timescaleAdjustmentFactor))
							}
						}(i)
					}

					// Training end.
					for i, client := range clients {
						numTrainings := numTrainingsPerSession[i]
						go func(sessionIndex int) {
							trainingStartTimeChannel := trainingStartTimeChannels[sessionIndex]

							for j := 0; j < numTrainings; j++ {
								workloadTemplateSession := workloadTemplateSessions[sessionIndex]
								training := workloadTemplateSession.TrainingEvents[j]
								trainingDuration := training.DurationInTicks

								trainingStartTime := <-trainingStartTimeChannel

								time.Sleep(time.Duration(float64(trainingDuration) * float64(time.Minute) * timescaleAdjustmentFactor))
								execStopTimeUnixMillis := time.Now().UnixMilli()
								//client.TrainingStoppedChannel <- &jupyter.Message{
								//	Header: &jupyter.KernelMessageHeader{
								//		MessageId:   uuid.NewString(),
								//		MessageType: jupyter.ExecuteReply,
								//		Date:        time.Now().String(),
								//	},
								//	Content: map[string]interface{}{
								//		"execution_start_unix_millis":    float64(trainingStartTime.UnixMilli()),
								//		"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
								//	},
								//}
								notifyClientThatTrainingFinished(client, float64(trainingStartTime.UnixMilli()), float64(execStopTimeUnixMillis))
								fmt.Printf("Submitted 'training-stopped' %d/%d for session \"%s\"\n", j+1, numTrainings, workloadTemplateSession.Id)
								time.Sleep(time.Millisecond * 10)
							}
						}(i)
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					for idx, client := range clients {
						Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
						Expect(client.TrainingEventsHandled()).To(Equal(int32(numTrainingsPerSession[idx])))
						Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
					}

					targetWorkload = workloadDriver.GetWorkload()
					Expect(targetWorkload).ToNot(BeNil())
					GinkgoWriter.Printf("State of workload %s (ID=%s): %s\n",
						targetWorkload.Name, targetWorkload.Id, targetWorkload.GetState().String())
					Expect(targetWorkload.IsFinished()).To(BeTrue())
					Expect(targetWorkload == currWorkload).To(BeTrue())
					Expect(targetWorkload.Statistics.NumSubmittedTrainings).To(Equal(totalNumTrainings))
					Expect(targetWorkload.Statistics.NumTasksExecuted).To(Equal(totalNumTrainings))
					Expect(targetWorkload.Statistics.NumSessionsCreated).To(Equal(numSessions))
				})
			})
		})
	})
})
