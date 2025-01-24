package workload

import (
	"encoding/json"
)

type PreloadedWorkloadTemplate struct {
	// DisplayName is the display name of the preloaded workload template.
	DisplayName string `json:"display_name" yaml:"display_name" name:"display_name" mapstructure:"display_name"`

	// Key uniquely identifies the PreloadedWorkloadTemplate.
	Key string `json:"key" yaml:"key" mapstructure:"key" name:"key"`

	// Filepath is the file path of the .JSON workload template file.
	Filepath string `json:"filepath" yaml:"filepath" name:"filepath" mapstructure:"filepath"`

	// NumSessions is the number of sessions that will be created by/in the workload.
	NumSessions int `json:"num_sessions" yaml:"num_sessions" name:"num_sessions" mapstructure:"num_sessions"`

	// NumTrainings is the total number of training events in the workload (for all sessions).
	NumTrainings int `json:"num_training_events" yaml:"num_training_events" name:"num_training_events" mapstructure:"num_training_events"`

	// IsLarge indicates if the workload is "arbitrarily" large, as in it is up to the creator of the template
	// (or whoever creates the configuration file with all the preloaded workload templates in it) to designate
	// a workload as "large".
	IsLarge bool `json:"large" yaml:"large" name:"large" mapstructure:"large"`
}

func (t *PreloadedWorkloadTemplate) String() string {
	m, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}

	return string(m)
}
