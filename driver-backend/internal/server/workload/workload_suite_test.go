package workload_test

import (
	"github.com/scusemua/workload-driver-react/m/v2/internal/domain"
	"github.com/scusemua/workload-driver-react/m/v2/internal/mock_domain"
	"go.uber.org/mock/gomock"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Create the domain.SessionMetadata struct that is used for the unit tests within this Context.
func getBasicSessionMetadata(sessionId string, controller *gomock.Controller) domain.SessionMetadata {
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

	return sessionMetadata
}

func TestWorkload(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Workload Suite")
}
