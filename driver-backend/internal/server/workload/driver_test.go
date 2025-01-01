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
					mockWebsocket, &atom, mockCallbackProvider)

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

			It("Will correctly resubmit failed 'training-started' events", func() {
				sessionId := "TestSession"

				sessionMetadata := mock_domain.NewMockSessionMetadata(controller)

				sessionMetadata.EXPECT().GetPod().AnyTimes().Return(sessionId)
				sessionMetadata.EXPECT().GetVRAM().AnyTimes().Return(1.0)

				sessionMetadata.EXPECT().GetMaxSessionCPUs().AnyTimes().Return(128.0)
				sessionMetadata.EXPECT().GetMaxSessionMemory().AnyTimes().Return(512.0)
				sessionMetadata.EXPECT().GetMaxSessionGPUs().AnyTimes().Return(1)
				sessionMetadata.EXPECT().GetMaxSessionVRAM().AnyTimes().Return(1.0)

				sessionMetadata.EXPECT().GetCurrentTrainingMaxGPUs().AnyTimes().Return(1)
				sessionMetadata.EXPECT().GetCurrentTrainingMaxCPUs().AnyTimes().Return(128.0)
				sessionMetadata.EXPECT().GetCurrentTrainingMaxMemory().AnyTimes().Return(512.0)
				sessionMetadata.EXPECT().GetCurrentTrainingMaxVRAM().AnyTimes().Return(1.0)

				mockKernelConnection := mock_jupyter.NewMockKernelConnection(controller)
				mockKernelConnection.EXPECT().RegisterIoPubHandler(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

				mockKernelManager.EXPECT().CreateSession(sessionId, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
					func(sessionId string, sessionPath string, sessionType string, kernelSpecName string, resourceSpec *jupyter.ResourceSpec) (*jupyter.SessionConnection, error) {
						sessionConn := &jupyter.SessionConnection{
							Kernel: mockKernelConnection,
						}

						return sessionConn, nil
					})

				var kernelStoppedWg sync.WaitGroup
				kernelStoppedWg.Add(1)

				mockKernelManager.EXPECT().StopKernel(sessionId).Times(1).DoAndReturn(func(sessionId string) error {
					kernelStoppedWg.Done()
					return nil
				})

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

				clientChan := make(chan *workload.Client, 1)
				mockKernelConnection.EXPECT().RequestExecute(gomock.Any()).Times(1).DoAndReturn(func(args *jupyter.RequestExecuteArgs) (jupyter.KernelMessage, error) {
					client, loaded := workloadDriver.Clients[sessionId]
					Expect(loaded).To(BeTrue())
					Expect(client).ToNot(BeNil())

					clientChan <- client

					return nil, nil
				})

				err = workloadDriver.StartWorkload()
				Expect(err).To(BeNil())

				go workloadDriver.ProcessWorkloadEvents()
				go workloadDriver.DriveWorkload()

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

				time.Sleep(time.Second * 5)
			})
		})
	})
})
