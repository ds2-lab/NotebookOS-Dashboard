package workload_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/workload"
	"github.com/scusemua/workload-driver-react/m/v2/internal/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"time"
)

var _ = Describe("Workload Client Tests", func() {
	var (
		mockCtrl                *gomock.Controller
		remoteStorageDefinition *proto.RemoteStorageDefinition
		atom                    zap.AtomicLevel
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())

		remoteStorageDefinition = storage.NewRemoteStorageBuilder().
			WithName("AWS S3").
			WithDownloadRate(200_000_000).
			WithUploadRate(50_000_000).
			WithDownloadVariancePercent(0).
			WithUploadVariancePercent(0).
			WithWriteFailureChancePercentage(0).
			WithReadFailureChancePercentage(0).
			Build()

		atom = zap.NewAtomicLevelAt(zap.DebugLevel)
	})

	It("Will embed its assigned model and dataset in the metadata of an 'execute_request' message.", func() {
		sessionId := uuid.NewString()
		workloadId := uuid.NewString()

		basicWorkload := workload.NewBuilder(&atom).
			SetID(workloadId).
			SetWorkloadName("Dummy Workload").
			SetSeed(1).
			EnableDebugLogging(true).
			SetTimescaleAdjustmentFactor(1.0).
			SetTimeCompressTrainingDurations(true).
			SetRemoteStorageDefinition(remoteStorageDefinition).
			SetSessionsSamplePercentage(1.0).
			Build()
		Expect(basicWorkload).ToNot(BeNil())

		workloadFromTemplate, err := workload.NewWorkloadFromTemplate(basicWorkload, make([]*domain.WorkloadTemplateSession, 0))
		Expect(err).To(BeNil())
		Expect(workloadFromTemplate).ToNot(BeNil())

		sessionReadyEvent := &domain.Event{
			Name:        domain.EventSessionReady,
			GlobalIndex: 0,
			LocalIndex:  0,
			ID:          uuid.NewString(),
			SessionId:   sessionId,
			Timestamp:   time.Now(),
			Data:        getBasicSessionMetadata(sessionId, mockCtrl),
		}

		client := workload.NewClientBuilder().
			WithSessionId(sessionId).
			WithAtom(&atom).
			WithSessionReadyEvent(sessionReadyEvent).
			WithDeepLearningModel("ResNet-18").
			WithDataset("CIFAR-10").
			WithWorkload(workloadFromTemplate).
			Build()
		Expect(client).ToNot(BeNil())

		By("Correctly creating the client")

		Expect(client).ToNot(BeNil())
		Expect(client.SessionId).To(Equal(sessionId))
		Expect(client.AssignedModel).To(Equal("ResNet-18"))
		Expect(client.AssignedDataset).To(Equal("CIFAR-10"))

		By("Correctly creating 'execute_request' arguments.")

		startTime := time.Now()
		trainingEvent := &domain.Event{
			Name:        domain.EventSessionTraining,
			GlobalIndex: 0,
			LocalIndex:  0,
			ID:          uuid.NewString(),
			SessionId:   sessionId,
			Timestamp:   startTime,
			Data:        getBasicSessionMetadata(sessionId, mockCtrl),
			Duration:    time.Millisecond * 500,
			EndTime:     startTime.Add(time.Millisecond * 500),
		}

		args, err := client.CreateExecuteRequestArguments(trainingEvent)
		Expect(err).To(BeNil())
		Expect(args).ToNot(BeNil())

		metadata := args.RequestMetadata()
		Expect(metadata).ToNot(BeNil())
		Expect(len(metadata)).To(Equal(4))

		_, loadedResourceReq := metadata["resource_request"]
		Expect(loadedResourceReq).To(BeTrue())

		trainingDurationMillis, loadedTrainingDuration := metadata["training_duration_millis"]
		Expect(loadedTrainingDuration).To(BeTrue())
		Expect(trainingDurationMillis).To(Equal(500.0))

		model, loadedModel := metadata["model"]
		Expect(loadedModel).To(BeTrue())
		Expect(model).To(Equal("ResNet-18"))

		dataset, loadedDataset := metadata["dataset"]
		Expect(loadedDataset).To(BeTrue())
		Expect(dataset).To(Equal("CIFAR-10"))
	})
})
