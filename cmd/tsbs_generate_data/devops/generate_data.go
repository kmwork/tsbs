package devops

// DevopsSimulator generates data similar to telemetry, with metrics from a variety of device systems.
// It fulfills the Simulator interface.
type DevopsSimulator struct {
	*commonDevopsSimulator
	simulatedMeasurementIndex int
}

// DevopsSimulatorConfig is used to create a DevopsSimulator.
type DevopsSimulatorConfig commonDevopsSimulatorConfig
