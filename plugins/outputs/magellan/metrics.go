package magellan

import (
	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
)

func (w *Magellan) loadMetricDefs() error {
	if w.MetricDefDir == "" {
		w.MetricDefs = map[string]*info.MetricDef{}
		return nil
	}
	err := info.Scan(w.MetricDefDir, w.MetricDefs)
	return err
}

func (w *Magellan) metricDef(metric string) (*info.MetricDef, bool) {
	m, ok := w.MetricDefs[metric]
	return m, ok
}
