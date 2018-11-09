package orionres

import (
	"context"
	"log"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/outputs/orionres/processor"
	"github.com/influxdata/telegraf/plugins/outputs/orionres/res/session"
	"github.com/influxdata/telegraf/spirent/service"
)

type OrionRes struct {
	MetricsDefDir string `toml:"metrics_dir"`
	URL           string `toml:"url"`
	DbId          string `toml:"db_id"`
	DbName        string `toml:"db_name"`
	TestKey       string `toml:"test_key"`

	Processor *processor.Processor
}

func (m *OrionRes) Write(metrics []telegraf.Metric) error {
	if m.Processor == nil {
		return nil
	}
	ctx := context.Background()
	return m.Processor.Process(ctx, metrics)
}

func (m *OrionRes) SampleConfig() string {
	return sampleConfig
}

func (m *OrionRes) Description() string {
	return "Configuration for orionres output"
}

func (m *OrionRes) Connect() error {
	m.loadMetricDefs()
	if len(m.DbId) > 0 || len(m.DbName) > 0 {
		c, err := processor.NewClient(m.URL, m.DbId, m.DbName, m.TestKey)
		if err != nil {
			log.Printf("E! orionres client error: %s", err)
			return err
		}
		m.Processor.SetClient(c)
		log.Printf("I! Using database DbId=%s", c.DbId)
	}
	service.Service().Start()
	return nil
}

func (w *OrionRes) Close() error {
	return nil
}

var sampleConfig = `
  ## orionres URL
  url = "http://localhost:9002"
`

func init() {
	m := &OrionRes{
		Processor: processor.New(),
	}
	m.initResources()
	outputs.Add("orionres", func() telegraf.Output {
		return m
	})
}

func (m *OrionRes) loadMetricDefs() error {
	log.Printf("I: load metrics definition directory: %s", m.MetricsDefDir)
	if m.MetricsDefDir == "" {
		return nil
	}
	err := m.Processor.MetricDefs.ScanFiles(m.MetricsDefDir, nil)
	if err != nil {
		log.Printf("E! ScanFiles error: %s", err)
	}
	log.Printf("I! Dimension sets loaded %d", len(m.Processor.MetricDefs.Dim))
	log.Printf("I! Result sets loaded %d", len(m.Processor.MetricDefs.Res))
	return err
}

func (m *OrionRes) initResources() {
	s := service.Service()
	s.Service.AddCollectionResource("/telegraf/output/orionres/sessions", session.NewResource(m.Processor))
}
