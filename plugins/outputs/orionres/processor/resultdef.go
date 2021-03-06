package processor

import (
	"log"

	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
	"github.com/influxdata/telegraf"
)

type ResultDefDim struct {
	IdNames     []string
	AttribNames []string
	Dim         *info.DimSet
}

type ResultDef struct {
	Name                string
	MetricServiceName   string
	Enabled             bool
	RemapResFacts       bool
	TestDim             bool
	PrimaryDimensionSet string
	Dims                []*ResultDefDim
	DimAttribs          map[string]string
	ResFacts            map[string]*info.ResSetFact
}

func newResultDef(defName string, setDefs *info.MetricDefs) *ResultDef {
	resFacts := make(map[string]*info.ResSetFact)
	dimAttribs := make(map[string]string)

	res, ok := setDefs.MetricsServiceRes[defName]
	if !ok {
		return &ResultDef{
			Name:                defName,
			MetricServiceName:   defName,
			Enabled:             true,
			DimAttribs:          dimAttribs,
			ResFacts:            resFacts,
			TestDim:             true,
			PrimaryDimensionSet: "test",
		}
	}

	dimSets := make([]*ResultDefDim, 0, len(res.DimensionSets))
	testDim := false
	for _, d := range res.DimensionSets {
		log.Printf("D! adding dimension %s", d)
		if d == "test" {
			testDim = true
			continue
		}
		dim, ok := setDefs.Dim[d]
		if !ok {
			log.Printf("E! dimension %s not found", d)
			continue
		}
		if len(dim.Attributes) == 0 {
			log.Printf("E! skipping dimemsiom %s, no attributes defined", d)
			continue
		}
		idNames := dim.MetricsService.Identifiers
		if len(idNames) == 0 {
			idNames = append(idNames, dim.Attributes[0].Name)
			log.Printf("D! identifiers not specified for %s dimension, dimension won't be filled in from metrics data", d)
			continue
		}
		var attribNames []string
		for _, a := range dim.Attributes {
			dimAttribs[a.Name] = dim.Name
			var isId bool
			for _, id := range idNames {
				if a.Name == id {
					isId = true
					break
				}
			}
			if isId {
				continue
			}
			attribNames = append(attribNames, a.Name)
		}
		dimSets = append(dimSets, &ResultDefDim{
			IdNames:     idNames,
			AttribNames: attribNames,
			Dim:         dim,
		})
	}

	for _, f := range res.Facts {
		log.Printf("D! adding fact %s", f.Name)
		resFacts[f.Name] = f
	}
	return &ResultDef{
		Name:                res.Name,
		MetricServiceName:   res.MetricsServiceName(),
		Dims:                dimSets,
		DimAttribs:          dimAttribs,
		RemapResFacts:       res.MetricsService.RemapFacts,
		ResFacts:            resFacts,
		Enabled:             true,
		TestDim:             testDim,
		PrimaryDimensionSet: res.PrimaryDimensionSet,
	}
}

func updateResultDef(r *ResultDef, metric telegraf.Metric) (bool, error) {
	updated := false
	if r.RemapResFacts {
		return updated, nil
	}

	tags := metric.Tags()
	for n := range tags {
		if _, ok := r.DimAttribs[n]; ok {
			continue
		}
		if _, ok := r.ResFacts[n]; ok {
			continue
		}
		// add unknown dims into result facts
		r.ResFacts[n] = &info.ResSetFact{
			Name:        n,
			DisplayName: n,
			Description: n,
			Db: info.ResSetFactDb{
				BaseUnit: "none",
				DataType: "string",
			},
		}
		updated = true
	}

	fields := metric.Fields()
	for n := range fields {
		if _, ok := r.ResFacts[n]; ok {
			continue
		}
		r.ResFacts[n] = &info.ResSetFact{
			Name:        n,
			DisplayName: n,
			Description: n,
			Db: info.ResSetFactDb{
				BaseUnit: "none",
				DataType: valueType(fields[n]),
			},
		}
		updated = true
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
