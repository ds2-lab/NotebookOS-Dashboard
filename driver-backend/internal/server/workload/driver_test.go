package workload_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/mock_domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/mock_workload"
	"github.com/scusemua/workload-driver-react/m/v2/internal/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
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
			controller                *gomock.Controller
			mockWebsocket             *mock_domain.MockConcurrentWebSocket
			mockCallbackProvider      *mock_workload.MockCallbackProvider
			remoteStorageDefinition   *proto.RemoteStorageDefinition
			timescaleAdjustmentFactor float64
			driver                    *workload.BasicWorkloadDriver
			atom                      zap.AtomicLevel
		)

		BeforeEach(func() {
			controller = gomock.NewController(GinkgoT())
			mockWebsocket = mock_domain.NewMockConcurrentWebSocket(controller)
			mockCallbackProvider = mock_workload.NewMockCallbackProvider(controller)

			timescaleAdjustmentFactor = 0.1

			atom = zap.NewAtomicLevelAt(zap.DebugLevel)
			driver = workload.NewBasicWorkloadDriver(workloadDriverOpts, true, timescaleAdjustmentFactor,
				mockWebsocket, &atom, mockCallbackProvider)
			driver.OutputCsvDisabled = true

			remoteStorageDefinition = storage.NewRemoteStorageBuilder().
				WithName("AWS S3").
				WithDownloadRate(200_000_000).
				WithUploadRate(50_000_000).
				WithDownloadVariancePercent(0).
				WithUploadVariancePercent(0).
				WithWriteFailureChancePercentage(0).
				WithReadFailureChancePercentage(0).
				Build()

			Expect(driver).ToNot(BeNil())
		})

		It("Will correctly resubmit failed 'training-started' events", func() {
			eventQueue := driver.EventQueue()
			Expect(eventQueue).ToNot(BeNil())

			sessionMetadata := mock_domain.NewMockSessionMetadata(controller)
			resourceRequest := domain.NewResourceRequest(128, 512, 1, 1, "AnyGPU")
			session := domain.NewWorkloadSession("TestSession", sessionMetadata, resourceRequest, time.Now(), &atom)
			workloadTemplateSession := domain.NewWorkloadTemplateSession(session, 0, 4)

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

			workload, err := driver.RegisterWorkload(workloadRegistrationRequest)
			Expect(err).To(BeNil())
			Expect(workload).ToNot(BeNil())

			go driver.DriveWorkload()
		})
	})
})
