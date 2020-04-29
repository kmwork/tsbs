package devops

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

var labelCPU = []byte("cpu") // heap optimization
var cpuFields []common.LabeledDistributionMaker = nil

// for cpuFields generate (c) kostya
func init() {
	log.Print("[kostya-start2] kostyaGenerateLabels start")
	cpuFields = make([]common.LabeledDistributionMaker, common.KostyaNumFields)
	var i int64
	for i = 0; i < common.KostyaNumFields; i++ {
		var fieldName = "kostya_" + strconv.FormatInt(i, 10)
		var item = common.LabeledDistributionMaker{
			Label: []byte(fieldName), DistributionMaker: func() common.Distribution { return common.CWD(cpuND, 0.0, 100.0, rand.Float64()*100.0) },
		}
		cpuFields = append(cpuFields, item)
	}
	log.Printf("[kostya-done2] kostyaGenerateLabels done, len = %d", len(r))
}

// Reuse NormalDistributions as arguments to other distributions. This is
// safe to do because the higher-level distribution advances the ND and
// immediately uses its value and saves the state
var cpuND = common.ND(0.0, 1.0)

type CPUMeasurement struct {
	*common.SubsystemMeasurement
}

func NewCPUMeasurement(start time.Time) *CPUMeasurement {
	fmt.Printf("[kostya-start3] NewCPUMeasurement start")
	return newCPUMeasurementNumDistributions(start, len(cpuFields))
}

func newSingleCPUMeasurement(start time.Time) *CPUMeasurement {
	return newCPUMeasurementNumDistributions(start, 1)
}

func newCPUMeasurementNumDistributions(start time.Time, numDistributions int) *CPUMeasurement {
	fmt.Printf("[kostya-start4] newCPUMeasurementNumDistributions start")
	sub := common.NewSubsystemMeasurementWithDistributionMakers(start, cpuFields[:numDistributions])
	return &CPUMeasurement{sub}
}

func (m *CPUMeasurement) ToPoint(p *serialize.Point) {
	m.ToPointAllInt64(p, labelCPU, cpuFields)
}
