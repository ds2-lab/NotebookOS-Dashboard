package workload

import (
	"encoding/json"
	"github.com/mattn/go-colorable"
	"github.com/scusemua/workload-driver-react/m/v2/internal/server/api/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
)

// Builder is the builder for the Workload struct.
type Builder struct {
	Id                            string                         `json:"workload_id"`
	WorkloadName                  string                         `json:"workload_name"`
	Seed                          int64                          `json:"seed"`
	DebugLoggingEnabled           bool                           `json:"debug_logging"`
	TimescaleAdjustmentFactor     float64                        `json:"timescale_adjustment_factor"`
	SessionsSamplePercentage      float64                        `json:"sessions_sample_percentage"`
	TimeCompressTrainingDurations bool                           `json:"time_compress_training_durations"`
	RemoteStorageDefinition       *proto.RemoteStorageDefinition `json:"remote-storage-definition"`
	FileOutputPath                string                         `json:"file_output_path"`

	atom   *zap.AtomicLevel
	logger *zap.Logger
}

// NewBuilder creates a new Builder instance.
func NewBuilder(atom *zap.AtomicLevel) *Builder {
	builder := &Builder{
		atom:                      atom,
		Seed:                      -1,
		DebugLoggingEnabled:       true,
		SessionsSamplePercentage:  1.0,
		TimescaleAdjustmentFactor: 1.0,
	}

	zapConfig := zap.NewDevelopmentEncoderConfig()
	zapConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(zapConfig), zapcore.AddSync(colorable.NewColorableStdout()), atom)
	logger := zap.New(core, zap.Development())
	if logger == nil {
		panic("failed to create logger for workload builder")
	}

	builder.logger = logger

	return builder
}

func (b *Builder) String() string {
	m, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}

	return string(m)
}

// SetID sets the ID for the workload.
func (b *Builder) SetID(id string) *Builder {
	b.Id = id
	return b
}

// SetWorkloadName sets the name for the workload.
func (b *Builder) SetWorkloadName(workloadName string) *Builder {
	b.WorkloadName = workloadName
	return b
}

// SetSeed sets the seed value for the workload.
func (b *Builder) SetSeed(seed int64) *Builder {
	b.Seed = seed
	return b
}

// EnableDebugLogging enables or disables debug logging.
func (b *Builder) EnableDebugLogging(enabled bool) *Builder {
	b.DebugLoggingEnabled = enabled
	return b
}

// SetTimescaleAdjustmentFactor sets the timescale adjustment factor.
func (b *Builder) SetTimescaleAdjustmentFactor(factor float64) *Builder {
	b.TimescaleAdjustmentFactor = factor
	return b
}

// SetSessionsSamplePercentage sets the sessions sample percentage.
func (b *Builder) SetSessionsSamplePercentage(percentage float64) *Builder {
	b.SessionsSamplePercentage = percentage
	return b
}

// SetTimeCompressTrainingDurations sets the timeCompressTrainingDurations flag.
func (b *Builder) SetTimeCompressTrainingDurations(timeCompressTrainingDurations bool) *Builder {
	b.TimeCompressTrainingDurations = timeCompressTrainingDurations
	return b
}

// SetRemoteStorageDefinition sets the remote storage definition.
func (b *Builder) SetRemoteStorageDefinition(def *proto.RemoteStorageDefinition) *Builder {
	b.RemoteStorageDefinition = def
	return b
}

// WithFileOutput will instruct the Workload [that is to be built] to also output its logs to a file (at the specified
// path) in addition to outputting its logs to the console/terminal (stdout).
func (b *Builder) WithFileOutput(path string) *Builder {
	b.FileOutputPath = path
	return b
}

// Build creates a Workload instance with the specified values.
func (b *Builder) Build() *BasicWorkload {
	b.logger.Debug("Building workload.",
		zap.String("workload_id", b.Id),
		zap.String("workload_config", b.String()))

	workload := &BasicWorkload{
		Id:                            b.Id, // Same ID as the driver.
		Name:                          b.WorkloadName,
		Seed:                          b.Seed,
		DebugLoggingEnabled:           b.DebugLoggingEnabled,
		TimescaleAdjustmentFactor:     b.TimescaleAdjustmentFactor,
		WorkloadType:                  UnspecifiedWorkload,
		atom:                          b.atom,
		sessionsMap:                   make(map[string]interface{}),
		trainingStartedTimes:          make(map[string]time.Time),
		trainingStartedTimesTicks:     make(map[string]int64),
		RemoteStorageDefinition:       b.RemoteStorageDefinition,
		SampledSessions:               make(map[string]interface{}),
		UnsampledSessions:             make(map[string]interface{}),
		Statistics:                    NewStatistics(b.SessionsSamplePercentage),
		TimeCompressTrainingDurations: b.TimeCompressTrainingDurations,
	}

	zapEncoderConfig := zap.NewDevelopmentEncoderConfig()
	zapEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(zapEncoderConfig), zapcore.AddSync(colorable.NewColorableStdout()), b.atom)

	var core zapcore.Core
	if b.FileOutputPath == "" {
		// No filepath specified. Will just output to the console.
		core = zapcore.NewTee(consoleCore)
	} else {
		// Create file output as well.
		logFile, err := os.Create(b.FileOutputPath)
		if err != nil {
			panic(err)
		}

		writer := zapcore.AddSync(logFile)

		zapFileEncoderConfig := zap.NewDevelopmentEncoderConfig()
		zapFileEncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
		fileCore := zapcore.NewCore(zapcore.NewJSONEncoder(zapFileEncoderConfig), writer, b.atom)
		core = zapcore.NewTee(consoleCore, fileCore)
	}

	logger := zap.New(core, zap.Development())
	if logger == nil {
		panic("failed to create logger for workload driver")
	}

	workload.logger = logger
	workload.sugaredLogger = logger.Sugar()

	return workload
}
