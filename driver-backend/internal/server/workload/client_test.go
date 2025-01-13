package workload_test

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/mock_domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/workload"
	"go.uber.org/mock/gomock"
	"time"
)

var _ = Describe("Client Tests", func() {
	var mockCtrl *gomock.Controller

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
	})

	It("Will embed its assigned model and dataset in the metadata of an 'execute_request' message.", func() {
		sessionId := uuid.NewString()
		builder := workload.NewClientBuilder().
			WithSessionId(sessionId).
			WithDeepLearningModel("ResNet-18").
			WithDataset("CIFAR-10")

		client := builder.Build()

		By("Correctly creating the client")

		Expect(client).ToNot(BeNil())
		Expect(client.SessionId).To(Equal(sessionId))
		Expect(client.AssignedModel).To(Equal("ResNet-18"))
		Expect(client.AssignedDataset).To(Equal("CIFAR-10"))

		By("Correctly creating 'execute_request' arguments.")

		data := mock_domain.NewMockSessionMetadata(mockCtrl)
		data.EXPECT().GetPod().AnyTimes().Return(sessionId)

		trainingEvent := &domain.Event{
			Name:        domain.EventSessionTraining,
			GlobalIndex: 0,
			LocalIndex:  0,
			ID:          uuid.NewString(),
			SessionId:   sessionId,
			Timestamp:   time.Now(),
			Data:        data,
		}

		args, err := client.CreateExecuteRequestArguments(trainingEvent)
		Expect(err).To(BeNil())
		Expect(args).ToNot(BeNil())
	})
})
