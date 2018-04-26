package magellan

import (
	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
	"github.com/influxdata/telegraf"
)

type ResultDef struct {
	Name    string
	Enabled bool
	Def     *info.MetricDef
}

func newResultDef(defName string, metricDef *info.MetricDef) *ResultDef {
	if metricDef == nil {
		metricDef = &info.MetricDef{
			Name: defName,
		}
	}
	return &ResultDef{
		Name:    defName,
		Enabled: true,
		Def:     metricDef,
	}
}

func updateResultDef(r *ResultDef, metric telegraf.Metric) (bool, error) {
	updated := false

	tags := metric.Tags()
	for n := range tags {
		found := false
		for i := range r.Def.Dimensions {
			if r.Def.Dimensions[i].Name == n {
				found = true
				break
			}
		}
		if !found {
			r.Def.Dimensions = append(r.Def.Dimensions, info.Dim{
				Name:  n,
				Field: n,
			})
			updated = true
		}
	}

	fields := metric.Fields()
	for n := range fields {
		found := false
		for i := range r.Def.Fields {
			if r.Def.Fields[i].Name == n {
				found = true
				break
			}
		}
		if !found {
			r.Def.Fields = append(r.Def.Fields, info.Field{
				Name:  n,
				Field: n,
				Type:  valueType(fields[n]),
			})
			updated = true
		}
	}
	return updated, nil
}

func valueType(value interface{}) string {
	var datatype string
	switch value.(type) {
	case int64:
		datatype = "integer"
	case float64:
		datatype = "number"
	case string:
		datatype = "string"
	default:
		datatype = "string"
	}
	return datatype
}
