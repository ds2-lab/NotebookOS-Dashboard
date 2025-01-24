package workload_test

import (
	"encoding/json"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/workload"
	"github.com/scusemua/workload-driver-react/m/v2/internal/storage"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"os"
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

		timescaleAdjustmentFactor := 1.0

		basicWorkload := workload.NewBuilder(&atom).
			SetID(workloadId).
			SetWorkloadName("Dummy Workload").
			SetSeed(1).
			EnableDebugLogging(true).
			SetTimescaleAdjustmentFactor(timescaleAdjustmentFactor).
			SetTimeCompressTrainingDurations(true).
			SetRemoteStorageDefinition(remoteStorageDefinition).
			SetSessionsSamplePercentage(1.0).
			Build()
		Expect(basicWorkload).ToNot(BeNil())

		err := basicWorkload.InitializeFromTemplate(make([]*domain.WorkloadTemplateSession, 0))
		Expect(err).To(BeNil())

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
			WithWorkload(basicWorkload).
			WithTimescaleAdjustmentFactor(timescaleAdjustmentFactor).
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

		GinkgoWriter.Printf("metadata: %v\n", metadata)
		milliseconds := float64(trainingEvent.Duration.Milliseconds())
		GinkgoWriter.Printf("evt.Duration.Milliseconds(): %v\n", milliseconds)
		GinkgoWriter.Printf("milliseconds * basicWorkload.TimescaleAdjustmentFactor: %v\n", milliseconds*basicWorkload.TimescaleAdjustmentFactor)

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

	It("Will correctly write its logs to a file", func() {
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

		err := basicWorkload.InitializeFromTemplate(make([]*domain.WorkloadTemplateSession, 0))
		Expect(err).To(BeNil())

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
			WithWorkload(basicWorkload).
			WithFileOutput("test_client_output.json").
			Build()
		Expect(client).ToNot(BeNil())

		By("Correctly creating the client")

		Expect(client).ToNot(BeNil())
		Expect(client.SessionId).To(Equal(sessionId))
		Expect(client.AssignedModel).To(Equal("ResNet-18"))
		Expect(client.AssignedDataset).To(Equal("CIFAR-10"))

		err = client.Stop()
		Expect(err).To(BeNil())

		time.Sleep(time.Millisecond * 250)

		data, err := os.ReadFile("test_client_output.json")
		Expect(err).To(BeNil())
		Expect(data).ToNot(BeNil())
		Expect(len(data) > 0).To(BeTrue())

		// Example:
		// {"L":"WARN","T":"2025-01-20T12:56:50.462-0500","M":"Explicitly instructed to stop."}
		type LogMessage struct {
			Level     string `json:"L"`
			Timestamp string `json:"T"`
			Message   string `json:"M"`
		}

		var msg *LogMessage
		err = json.Unmarshal(data, &msg)
		Expect(err).To(BeNil())
		Expect(msg).ToNot(BeNil())
		Expect(msg.Level).To(Equal("WARN"))
		Expect(len(msg.Timestamp) > 0).To(BeTrue())

		Expect(msg.Message).To(Equal("Explicitly instructed to stop."))

		time.Sleep(time.Millisecond * 250)

		// Remove the file.
		err = os.Remove("test_client_output.json")
		Expect(err).To(BeNil())
	})
})
