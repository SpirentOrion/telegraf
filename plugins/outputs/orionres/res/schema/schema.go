package schema

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/SpirentOrion/luddite"
	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
	"github.com/influxdata/telegraf/plugins/outputs/orionres/processor"
)

const (
	EcodeLookupFailure = "LOOKUP_FAILURE"
)

var errorDefs = map[string]string{
	EcodeLookupFailure: "Lookup failure: %s",
}

type schemaResource struct {
	schemaType int
	processor  *processor.Processor
}

func NewResource(schemaType int, p *processor.Processor) *schemaResource {
	if schemaType != info.SetTypeDim && schemaType != info.SetTypeRes {
		defSchemaType := info.SetTypeDim
		log.Printf("E! Invalid schema type %d specified, defaulting to %d", schemaType, defSchemaType)
		schemaType = defSchemaType
	}

	return &schemaResource{
		schemaType: schemaType,
		processor:  p,
	}
}

func (r *schemaResource) New() interface{} {
	if r.schemaType == info.SetTypeRes {
		return &info.ResSet{}
	}
	return &info.DimSet{}
}

func (r *schemaResource) Id(value interface{}) string {
	if r.schemaType == info.SetTypeRes {
		return value.(*info.ResSet).Name
	}
	return value.(*info.DimSet).Name
}

func (r *schemaResource) List(req *http.Request) (int, interface{}) {
	// Get any name filters specified (this allows for comma delimited
	// lists and it allows a filter key to be specified more than once)
	var nameFilters []string
	for _, qpFilter := range req.URL.Query()["name"] {
		nameFilters = append(nameFilters, strings.Split(qpFilter, ",")...)
	}

	// List the ResSet objects - limited to the specified names if filters have been
	// specified (all name filters specified must match for the request to succeed)
	if r.schemaType == info.SetTypeRes {
		resSets, nonMatchedFilters := r.listResSet(nameFilters)
		if len(nonMatchedFilters) > 0 {
			err := fmt.Errorf("The following name filter(s) did not match anything: %s",
				strings.Join(nonMatchedFilters, ", "))
			return http.StatusBadRequest, luddite.NewError(errorDefs, EcodeLookupFailure, err)
		}
		return http.StatusOK, resSets
	}

	// List the DimSet objects - limited to the specified names if filters have been
	// specified (all name filters specified must match for the request to succeed)
	dimSets, nonMatchedFilters := r.listDimSet(nameFilters)
	if len(nonMatchedFilters) > 0 {
		err := fmt.Errorf("The following name filter(s) did not match anything: %s",
			strings.Join(nonMatchedFilters, ", "))
		return http.StatusBadRequest, luddite.NewError(errorDefs, EcodeLookupFailure, err)
	}
	return http.StatusOK, dimSets
}

func (r *schemaResource) Count(req *http.Request) (int, interface{}) {
	return http.StatusOK, 0
}

func (r *schemaResource) Get(req *http.Request, name string) (int, interface{}) {
	// Attempt to get the requested ResSet definition
	if r.schemaType == info.SetTypeRes {
		resSets := r.processor.MetricDefs.Res
		if resSet, ok := resSets[name]; ok {
			return http.StatusOK, resSet
		}
		return http.StatusNotFound, nil
	}

	// Attempt to get the requested DimSet definition
	dimSets := r.processor.MetricDefs.Dim
	if dimSet, ok := dimSets[name]; ok {
		return http.StatusOK, dimSet
	}
	return http.StatusNotFound, nil
}

func (r *schemaResource) Create(req *http.Request, value interface{}) (int, interface{}) {
	return http.StatusNotImplemented, nil
}

func (r *schemaResource) Delete(req *http.Request, id string) (int, interface{}) {
	return http.StatusNotImplemented, nil
}

func (r *schemaResource) Update(req *http.Request, id string, value interface{}) (int, interface{}) {
	return http.StatusNotImplemented, nil
}

func (r *schemaResource) Action(req *http.Request, id string, action string) (int, interface{}) {
	return http.StatusNotFound, nil
}

func (r *schemaResource) listResSet(filters []string) ([]*info.ResSet, []string) {
	resSets := r.processor.MetricDefs.Res

	var ret []*info.ResSet
	var resSet *info.ResSet
	var nonMatchedFilters []string

	// If filters are supplied, only report ResSet's matched
	filterCount := len(filters)
	if filterCount > 0 {
		var ok bool
		var filter string
		ret = make([]*info.ResSet, 0, filterCount)
		for _, filter = range filters {
			if resSet, ok = resSets[filter]; ok {
				ret = append(ret, resSet)
			} else {
				nonMatchedFilters = append(nonMatchedFilters, filter)
			}
		}
		return ret, nonMatchedFilters
	}

	// Report all ResSets available
	ret = make([]*info.ResSet, 0, len(resSets))
	for _, resSet = range resSets {
		ret = append(ret, resSet)
	}
	return ret, nonMatchedFilters
}

func (r *schemaResource) listDimSet(filters []string) ([]*info.DimSet, []string) {
	dimSets := r.processor.MetricDefs.Dim

	var ret []*info.DimSet
	var dimSet *info.DimSet
	var nonMatchedFilters []string

	// If filters are supplied, only report DimSet's matched
	filterCount := len(filters)
	if filterCount > 0 {
		var ok bool
		var filter string
		ret = make([]*info.DimSet, 0, filterCount)
		for _, filter = range filters {
			if dimSet, ok = dimSets[filter]; ok {
				ret = append(ret, dimSet)
			} else {
				nonMatchedFilters = append(nonMatchedFilters, filter)
			}
		}
		return ret, nonMatchedFilters
	}

	// Report all DimSets available
	ret = make([]*info.DimSet, 0, len(dimSets))
	for _, dimSet = range dimSets {
		ret = append(ret, dimSet)
	}
	return ret, nonMatchedFilters
}
