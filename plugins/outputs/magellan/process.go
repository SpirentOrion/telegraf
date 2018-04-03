package magellan

import (
	"context"

	xv1 "github.com/SpirentOrion/orion-api/res/xfer/v1"
	"github.com/influxdata/telegraf"
)

const (
	timestampName = "timestamp"
)

func (w *Magellan) process(ctx context.Context, metrics []telegraf.Metric) error {
	dbw := &xv1.DatabaseWrite{
		ResultSets: make([]xv1.ResultSetWrite, 0, len(metrics)),
	}
	for _, metric := range metrics {
		defName := metric.Name()
		resultDef, ok := w.ResultDefs[defName]
		if !ok {
			resultDef, err := w.createResultDef(metric)
			w.ResultDefs[defName] = resultDef
			err = w.writeResultDef(ctx, resultDef)
			if err != nil {
				continue
			}
		}
		if !resultDef.Enabled {
			continue
		}
		rs := w.processMetric(metric)
		if rs != nil {
			continue
		}
		dbw.ResultSets = append(dbw.ResultSets, *rs)
	}
	if len(dbw.ResultSets) > 0 || len(dbw.DimensionSets) > 0 {
		return w.Client.WriteDB(ctx, dbw)
	}
	return nil
}

func (w *Magellan) processMetric(metric telegraf.Metric) *xv1.ResultSetWrite {
	rs := &xv1.ResultSetWrite{
		Name: w.SetPrefix + metric.Name(),
	}
	var values []interface{}

	rs.Columns = append(rs.Columns, timestampName)
	values = append(values, metric.Time())

	// convert tags
	for c, v := range metric.Tags() {
		rs.Columns = append(rs.Columns, c)
		values = append(values, v)
	}

	// convert fields
	for c, v := range metric.Fields() {
		rs.Columns = append(rs.Columns, c)
		values = append(values, v)
	}
	rs.Rows = append(rs.Rows, values)
	return rs
}

func (w *Magellan) createResultDef(metric telegraf.Metric) (*ResultDef, error) {
	r := &ResultDef{
		Name:    metric.Name(),
		Enabled: true,
	}

	// create def from metric values
	tags := metric.Tags()
	fields := metric.Fields()
	r.Def = &MetricsDef{
		Dimensions: make([]string, 0, len(tags)),
		Fields:     make([]Metric, 0, len(fields)),
	}
	for _, v := range tags {
		r.Def.Dimensions = append(r.Def.Dimensions, v)
	}
	for n, v := range fields {
		r.Def.Fields = append(r.Def.Fields,
			Metric{
				Name: n,
				Type: valueType(v),
			})
	}
	return r, nil
}

func (w *Magellan) writeResultDef(ctx context.Context, r *ResultDef) error {

	rs := xv1.ResultSet{
		Name: w.SetPrefix + r.Name,
	}

	var dsList []xv1.DimensionSet
	var rsList []xv1.ResultSet

	for _, v := range r.Def.Dimensions {
		f := xv1.FieldDefinition{
			Name:        v,
			DisplayName: v,
			Type:        "string",
		}
		rs.Facts = append(rs.Facts, f)
	}
	for _, v := range r.Def.Fields {
		f := xv1.FieldDefinition{
			Name:        v.Name,
			DisplayName: v.Name,
			Type:        v.Type,
		}
		rs.Facts = append(rs.Facts, f)
	}
	rsList = append(rsList, rs)

	var err error
	if len(dsList) > 0 || len(rs.Facts) > 0 {
		err = w.Client.UpdateDB(ctx, dsList, rsList)
	}
	r.Enabled = (err == nil)
	return err
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
