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
	mock_jupyter "github.com/scusemua/workload-driver-react/m/v2/pkg/jupyter/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
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
	}
)

var _ = Describe("Driver", func() {
	Context("Driving workloads", func() {
		var (
			controller           *gomock.Controller
			mockWebsocket        *mock_domain.MockConcurrentWebSocket
			mockCallbackProvider *mock_workload.MockCallbackProvider
			mockKernelManager    *mock_jupyter.MockKernelSessionManager

			managerMetadata     map[string]interface{}
			managerMetadataLock sync.Mutex

			remoteStorageDefinition   *proto.RemoteStorageDefinition
			workloadDriver            *workload.BasicWorkloadDriver
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
			}

			BeforeEach(func() {
				managerMetadata = make(map[string]interface{})

				controller = gomock.NewController(GinkgoT())
				mockWebsocket = mock_domain.NewMockConcurrentWebSocket(controller)
				mockKernelManager = mock_jupyter.NewMockKernelSessionManager(controller)

				mockCallbackProvider = mock_workload.NewMockCallbackProvider(controller)
				mockCallbackProvider.EXPECT().GetSchedulingPolicy().AnyTimes().Return("static", true)
				mockCallbackProvider.EXPECT().HandleWorkloadError(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(workloadId string, err error) {
					fmt.Printf("[ERROR] Workload '%s' encountered error: %v\n", workloadId, err)
				})

				timescaleAdjustmentFactor = 0.01667

				atom = zap.NewAtomicLevelAt(zap.DebugLevel)
				workloadDriver = workload.NewBasicWorkloadDriver(workloadDriverOpts, true, timescaleAdjustmentFactor,
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

					mockKernelConnection := mock_jupyter.NewMockKernelConnection(controller)
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

					client.TrainingStartedChannel <- struct{}{}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					client.TrainingStoppedChannel <- &jupyter.BaseKernelMessage{
						Header: &jupyter.KernelMessageHeader{
							MessageId:   uuid.NewString(),
							MessageType: jupyter.ExecuteReply,
							Date:        time.Now().String(),
						},
						Content: map[string]interface{}{
							"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
							"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						},
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(2)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
				})

				It("Will correctly resubmit failed a 'training-started' event", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mock_jupyter.NewMockKernelConnection(controller)
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

					client.TrainingStartedChannel <- fmt.Errorf("insufficient hosts available")

					var trainingResubmittedWg sync.WaitGroup
					trainingResubmittedWg.Add(1)

					var execStartedTimeUnixMillis int64
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						execStartedTimeUnixMillis = time.Now().UnixMilli()
						trainingResubmittedWg.Done()
						return nil, nil
					})

					//requestExecArgs := <-requestExecArgsChan
					trainingResubmittedWg.Wait()
					client.TrainingStartedChannel <- struct{}{}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					client.TrainingStoppedChannel <- &jupyter.BaseKernelMessage{
						Header: &jupyter.KernelMessageHeader{
							MessageId:   uuid.NewString(),
							MessageType: jupyter.ExecuteReply,
							Date:        time.Now().String(),
						},
						Content: map[string]interface{}{
							"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
							"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						},
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(1)))
				})

				It("Will correctly resubmit both a failed 'session-started' event and a failed 'training' event", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mock_jupyter.NewMockKernelConnection(controller)
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

					client.TrainingStartedChannel <- fmt.Errorf("insufficient hosts available")

					var trainingResubmittedWg sync.WaitGroup
					trainingResubmittedWg.Add(1)

					var execStartedTimeUnixMillis int64
					mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
						execStartedTimeUnixMillis = time.Now().UnixMilli()
						trainingResubmittedWg.Done()
						return nil, nil
					})

					//requestExecArgs := <-requestExecArgsChan
					trainingResubmittedWg.Wait()
					client.TrainingStartedChannel <- struct{}{}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					client.TrainingStoppedChannel <- &jupyter.BaseKernelMessage{
						Header: &jupyter.KernelMessageHeader{
							MessageId:   uuid.NewString(),
							MessageType: jupyter.ExecuteReply,
							Date:        time.Now().String(),
						},
						Content: map[string]interface{}{
							"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
							"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						},
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(2)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(1)))
				})

				It("Will correctly resubmit a failed 'session-started' event multiple times", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mock_jupyter.NewMockKernelConnection(controller)
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

					setupFailedCreateAttempt := func(atttemptNumber int) {
						mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
							func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
								fmt.Printf("\nKernelSessionManager::CreateSession has been called (and will fail) on attempt #%d\n", atttemptNumber)

								// Tell the main goroutine that we've called KernelSessionManager::CreateSession.
								firstCreateSessionAttemptWg1.Done()

								fmt.Printf("\nWaiting for next mocked call to KernelSessionManager::CreateSession to be set up before returning on attempt #%d\n", atttemptNumber)

								// Wait for the main goroutine to set up the next expected call to KernelSessionManager::CreateSession.
								firstCreateSessionAttemptWg2.Wait()

								// Reset the barrier for the next time we call this.
								firstCreateSessionAttemptWg2.Add(1)

								fmt.Printf("\nReturning from KernelSessionManager::CreateSession with an error on attempt #%d\n", atttemptNumber)

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

					client.TrainingStartedChannel <- struct{}{}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					client.TrainingStoppedChannel <- &jupyter.BaseKernelMessage{
						Header: &jupyter.KernelMessageHeader{
							MessageId:   uuid.NewString(),
							MessageType: jupyter.ExecuteReply,
							Date:        time.Now().String(),
						},
						Content: map[string]interface{}{
							"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
							"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						},
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(6)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(0)))
				})

				It("Will correctly resubmit a failed 'training-started' event multiple times", func() {
					sessionId := "TestSession"

					sessionMetadata := getBasicSessionMetadata(sessionId, controller)

					mockKernelConnection := mock_jupyter.NewMockKernelConnection(controller)
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

					client.TrainingStartedChannel <- fmt.Errorf("insufficient hosts available")

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
						client.TrainingStartedChannel <- fmt.Errorf("insufficient hosts available")
					}

					// Wait for RequestExecute to be called.
					trainingResubmittedWg.Wait()

					// Send the notification that the training started correctly.
					client.TrainingStartedChannel <- struct{}{}

					time.Sleep(time.Second * time.Duration(8*timescaleAdjustmentFactor))
					execStopTimeUnixMillis := time.Now().UnixMilli()

					client.TrainingStoppedChannel <- &jupyter.BaseKernelMessage{
						Header: &jupyter.KernelMessageHeader{
							MessageId:   uuid.NewString(),
							MessageType: jupyter.ExecuteReply,
							Date:        time.Now().String(),
						},
						Content: map[string]interface{}{
							"execution_start_unix_millis":    float64(execStartedTimeUnixMillis),
							"execution_finished_unix_millis": float64(execStopTimeUnixMillis),
						},
					}

					kernelStoppedWg.Wait()

					time.Sleep(time.Second * 1)

					Expect(client.NumSessionStartAttempts()).To(Equal(int32(1)))
					Expect(client.TrainingEventsHandled()).To(Equal(int32(1)))
					Expect(client.TrainingEventsDelayed()).To(Equal(int32(6)))
				})
			})
		})
	})
})
