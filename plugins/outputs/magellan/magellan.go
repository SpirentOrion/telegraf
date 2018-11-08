package magellan

import (
	"context"
	"log"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/outputs/magellan/processor"
	"github.com/influxdata/telegraf/plugins/outputs/magellan/res/session"
	"github.com/influxdata/telegraf/spirent/service"
)

type Magellan struct {
	MetricsDefDir string `toml:"metrics_dir"`
	URL           string `toml:"url"`
	DbId          string `toml:"db_id"`
	DbName        string `toml:"db_name"`
	TestKey       string `toml:"test_key"`

	Processor *processor.Processor
}

func (m *Magellan) Write(metrics []telegraf.Metric) error {
	if m.Processor == nil {
		return nil
	}
	ctx := context.Background()
	return m.Processor.Process(ctx, metrics)
}

func (m *Magellan) SampleConfig() string {
	return sampleConfig
}

func (m *Magellan) Description() string {
	return "Configuration for magellan output"
}

func (m *Magellan) Connect() error {
	m.loadMetricDefs()
	if len(m.DbId) > 0 || len(m.DbName) > 0 {
		c, err := processor.NewClient(m.URL, m.DbId, m.DbName, m.TestKey)
		if err != nil {
			log.Printf("E! Magellan client error: %s", err)
			return err
		}
		m.Processor.SetClient(c)
		log.Printf("I! Using database DbId=%s", c.DbId)
	}
	service.Service().Start()
	return nil
}

func (w *Magellan) Close() error {
	return nil
}

var sampleConfig = `
  ## magellan URL
  url = "http://localhost:9002"
`

func init() {
	m := &Magellan{
		Processor: processor.New(),
	}
	m.initResources()
	outputs.Add("magellan", func() telegraf.Output {
		return m
	})
}

func (m *Magellan) loadMetricDefs() error {
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

func (m *Magellan) initResources() {
	s := service.Service()
	s.Service.AddCollectionResource("/telegraf/output/magellan/sessions", session.NewResource(m.Processor))
}
