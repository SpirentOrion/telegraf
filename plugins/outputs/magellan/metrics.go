package magellan

type Metric struct {
	Name string
	Type string
}

type MetricsDef struct {
	Name       string
	Fields     []Metric
	Dimensions []string
}
