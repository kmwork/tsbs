package devops

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

type commonDevopsSimulatorConfig struct {
	// Start is the beginning time for the Simulator
	Start time.Time
	// End is the ending time for the Simulator
	End time.Time
}

func calculateEpochs(c commonDevopsSimulatorConfig, interval time.Duration) uint64 {
	return uint64(c.End.Sub(c.Start).Nanoseconds() / interval.Nanoseconds())
}

type commonDevopsSimulator struct {
	madePoints uint64
	maxPoints  uint64

	epoch      uint64
	epochs     uint64
	epochHosts uint64
	initHosts  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration
}

// Finished tells whether we have simulated all the necessary points
func (s *commonDevopsSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *commonDevopsSimulator) fields(measurements []common.SimulatedMeasurement) map[string][][]byte {
	data := make(map[string][][]byte)
	for _, sm := range measurements {
		point := serialize.NewPoint()
		sm.ToPoint(point)
		data[string(point.MeasurementName())] = point.FieldKeys()
	}

	return data
}
