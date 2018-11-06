package magellan

import (
	"context"
	"errors"
	"log"
	"strings"

	xv1 "github.com/SpirentOrion/orion-api/res/xfer/v1"
	"github.com/influxdata/telegraf"
)

const (
	timestampName = "timestamp"
	// counterSuffix = "_cnt"
)

func (m *Magellan) process(ctx context.Context, metrics []telegraf.Metric) error {
	dbw := &xv1.DatabaseWrite{
		ResultSets: make([]xv1.ResultSetWrite, 0, len(metrics)),
	}
	var err error
	updatedDefs := map[*ResultDef]bool{}
	for i := range metrics {
		metric := metrics[i]
		defName := metric.Name()
		//  t := metric.Type()
		//  if t == telegraf.Counter {
		//      counters and gauges are seperate metrics, use separate resultdef to avoid timestamp merges
		//	defName += counterSuffix
		//  }
		resultDef, ok := m.ResultDefs[defName]
		if !ok {
			log.Printf("D! Creating result def %s", defName)
			resultDef = newResultDef(defName, &m.MetricDefs)
			m.ResultDefs[defName] = resultDef
			updatedDefs[resultDef] = true
		}
		if !resultDef.Enabled {
			continue
		}
		if _, ok = updatedDefs[resultDef]; ok {
			updateResultDef(resultDef, metric)
		}
		ds, rs := m.processMetric(resultDef, metric)
		if rs != nil {
			dbw.ResultSets = append(dbw.ResultSets, *rs)
		}
		for i := range ds {
			dbw.DimensionSets = append(dbw.DimensionSets, *ds[i])
		}
	}
	if len(updatedDefs) > 0 {
		dsList, rsList := m.processResultDefs(updatedDefs)
		err := m.Client.UpdateDB(ctx, dsList, rsList)
		if err != nil {
			log.Printf("E! result def dbwrite error %s", err.Error())
			enableResultDefs(updatedDefs, false)
			return err
		}
	}
	if len(dbw.ResultSets) > 0 || len(dbw.DimensionSets) > 0 {
		if err = m.Client.WriteDB(ctx, dbw); err != nil {
			log.Printf("D! result dbwrite error %s", err.Error())
			return err
		}
	}
	return nil
}

func (m *Magellan) processMetric(r *ResultDef, metric telegraf.Metric) ([]*xv1.DimensionSetWrite, *xv1.ResultSetWrite) {
	var ds []*xv1.DimensionSetWrite
	rs := &xv1.ResultSetWrite{
		Name: m.ResultPrefix + r.Name,
	}
	var values []interface{}

	rs.Columns = append(rs.Columns, timestampName)
	values = append(values, metric.Time())

	// convert tags to dimensions
	tags := metric.Tags()
	for _, dim := range r.Dims {
		// identifiers
		idList, err := buildValueList(dim.IdNames, tags)
		if err != nil {
			continue
		}
		dimName := dim.Dim.Name
		dimStore := m.dimStore(dimName)
		idKey := strings.Join(idList, ":")
		dimObj := dimStore.Find(idKey)
		var objUpdated bool
		if dimObj == nil {
			attribs := map[string]interface{}{}
			for i := range idList {
				attribs[dim.IdNames[i]] = idList[i]
			}
			for _, n := range dim.AttribNames {
				if v, ok := tags[n]; ok {
					attribs[n] = v
				}
			}
			dimObj = dimStore.Create(idKey, attribs)
			objUpdated = true
		}
		// ToDo: Add code here to update non id attribute fields
		// Possibly:
		// else {
		//   objUpdated = dimStore.Update(dimObj, dim.AttribNames, tags)
		// }
		if objUpdated {
			var dim_values []interface{}
			s := &xv1.DimensionSetWrite{
				Name: dimName,
			}
			s.Columns = append(s.Columns, "key")
			dim_values = append(dim_values, dimObj.Key)
			for c, v := range dimObj.Attributes {
				s.Columns = append(s.Columns, c)
				dim_values = append(dim_values, v)
			}
			s.Rows = append(s.Rows, dim_values)
			ds = append(ds, s)
		}
		// add dimensioin keys to result set
		rs.Columns = append(rs.Columns, dimName)
		values = append(values, dimObj.Key)
	}
	if len(m.TestKey) > 0 {
		rs.Columns = append(rs.Columns, "test")
		values = append(values, m.TestKey)
	}
	for c, v := range metric.Fields() {
		rs.Columns = append(rs.Columns, c)
		values = append(values, v)
	}
	rs.Rows = append(rs.Rows, values)
	return ds, rs
}

func (m *Magellan) processResultDefs(defs map[*ResultDef]bool) ([]xv1.DimensionSet, []xv1.ResultSet) {
	var dsList []xv1.DimensionSet
	rsList := make([]xv1.ResultSet, 0, len(defs))
	dimNames := make(map[string]bool)
	for r := range defs {
		var dimSetNames []string
		if len(m.TestKey) > 0 {
			dimSetNames = append(dimSetNames, "test")
		}
		for _, d := range r.Dims {
			dimSetNames = append(dimSetNames, d.Dim.Name)
			if _, ok := dimNames[d.Dim.Name]; ok {
				continue
			}
			ds := xv1.DimensionSet{
				Name: d.Dim.Name,
			}
			for _, v := range d.Dim.Attributes {
				f := xv1.FieldDefinition{
					Name:        v.Name,
					DisplayName: v.DisplayName,
					Type:        "string",
				}
				ds.Attributes = append(ds.Attributes, f)
			}
			dsList = append(dsList, ds)
			dimNames[d.Dim.Name] = true
		}
		rs := xv1.ResultSet{
			Name:          m.ResultPrefix + r.Name,
			DimensionSets: dimSetNames,
		}
		if len(dimSetNames) > 0 {
			rs.PrimaryDimensionSet = &dimSetNames[0]
		}
		for _, v := range r.ResFacts {
			f := xv1.FieldDefinition{
				Name:        v.Name,
				DisplayName: v.DisplayName,
				Type:        v.Db.DataType,
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

func buildValueList(names []string, data map[string]string) ([]string, error) {
	vals := make([]string, 0, len(names))
	for _, n := range names {
		v, ok := data[n]
		if !ok {
			return vals, errors.New("buildValueList missing name in map data")
		}
		vals = append(vals, v)
	}
	return vals, nil
}
