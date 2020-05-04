package devops

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
	"github.com/timescale/tsbs/internal/utils"
)

var labelCPU = []byte("cpu") // heap optimization
var cpuFields []common.LabeledDistributionMaker

func PreConstructor() {
	cpuFields = make([]common.LabeledDistributionMaker, utils.KostyaColumnCounter())
	var i int64
	for i = 0; i < utils.KostyaColumnCounter(); i++ {
		var fieldName = "f" + strconv.FormatInt(i, 10)
		var item = common.LabeledDistributionMaker{
			Label: []byte(fieldName), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) },
		}
		cpuFields[i] = item
	}
}

// Reuse NormalDistributions as arguments to other distributions. This is
// safe to do because the higher-level distribution advances the ND and
// immediately uses its value and saves the state
var cpuND = common.ND(0.0, 1.0)

type CPUMeasurement struct {
	*common.SubsystemMeasurement
}

func NewCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, len(cpuFields))
}

func newSingleCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, 1)
}

func newCPUMeasurementNumDistributions(start time.Time, numDistributions int) *CPUMeasurement {
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, cpuFields[:numDistributions])
	return &CPUMeasurement{sub}
}

func (m *CPUMeasurement) ToPoint(p *serialize.Point) {
	m.ToPointAllInt64(p, labelCPU, cpuFields)
}
