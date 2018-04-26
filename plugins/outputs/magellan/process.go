package magellan

import (
	"context"
	"log"

	xv1 "github.com/SpirentOrion/orion-api/res/xfer/v1"
	"github.com/influxdata/telegraf"
)

const (
	timestampName = "timestamp"
	counterSuffix = "_cnt"
)

func (w *Magellan) process(ctx context.Context, metrics []telegraf.Metric) error {
	dbw := &xv1.DatabaseWrite{
		ResultSets: make([]xv1.ResultSetWrite, 0, len(metrics)),
	}
	var err error
	updatedDefs := map[*ResultDef]bool{}
	for m := range metrics {
		metric := metrics[m]
		defName := metric.Name()
		t := metric.Type()
		if t == telegraf.Counter {
			// counters and gauges are seperate metrics, use separate resultdef to avoid timestamp merges
			defName += counterSuffix
		}
		resultDef, ok := w.ResultDefs[defName]
		if !ok {
			log.Printf("D! Creating result def %s", defName)
			metricDef, _ := w.metricDef(defName)
			resultDef = newResultDef(defName, metricDef)
			w.ResultDefs[defName] = resultDef
			updatedDefs[resultDef] = true
		}
		if !resultDef.Enabled {
			continue
		}
		if _, ok = updatedDefs[resultDef]; ok {
			updateResultDef(resultDef, metric)
		}
		rs := w.processMetric(resultDef, metric)
		if rs == nil {
			continue
		}
		dbw.ResultSets = append(dbw.ResultSets, *rs)
	}
	if len(updatedDefs) > 0 {
		dsList, rsList := w.processResultDefs(updatedDefs)
		log.Printf("D! Writing %d result defs", len(updatedDefs))
		err := w.Client.UpdateDB(ctx, dsList, rsList)
		if err != nil {
			enableResultDefs(updatedDefs, false)
			log.Printf("D! result def dbwrite error %s", err.Error())
			return err
		}
	}
	if len(dbw.ResultSets) > 0 || len(dbw.DimensionSets) > 0 {
		if err = w.Client.WriteDB(ctx, dbw); err != nil {
			log.Printf("D! result dbwrite error %s", err.Error())
			return err
		}
	}
	return nil
}

func (w *Magellan) processMetric(r *ResultDef, metric telegraf.Metric) *xv1.ResultSetWrite {
	rs := &xv1.ResultSetWrite{
		Name: w.ResultPrefix + r.Name,
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

func (w *Magellan) processResultDefs(defs map[*ResultDef]bool) ([]xv1.DimensionSet, []xv1.ResultSet) {
	var dsList []xv1.DimensionSet
	rsList := make([]xv1.ResultSet, 0, len(defs))
	for r := range defs {
		rs := xv1.ResultSet{
			Name: w.ResultPrefix + r.Name,
		}
		for _, v := range r.Def.Dimensions {
			f := xv1.FieldDefinition{
				Name:        v.Name,
				DisplayName: v.Field,
				Type:        "string",
			}
			rs.Facts = append(rs.Facts, f)
		}
		for _, v := range r.Def.Fields {
			f := xv1.FieldDefinition{
				Name:        v.Name,
				DisplayName: v.Field,
				Type:        v.Type,
			}
			rs.Facts = append(rs.Facts, f)
		}
		rsList = append(rsList, rs)
	}
	return dsList, rsList
}

func enableResultDefs(defs map[*ResultDef]bool, enable bool) {
	for r := range defs {
		r.Enabled = enable
	}
}
