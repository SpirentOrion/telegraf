package magellan

import (
	"context"
	"fmt"
	"log"

	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/outputs/magellan/client"
)

// Magellan output plugin
type Magellan struct {
	URL           string `toml:"url"`
	DbId          string `toml:"db_id"`
	DbName        string `toml:"db_name"`
	TestKey       string `toml:"test_key"`
	ResultPrefix  string `toml:"result_prefix"`
	MetricsDefDir string `toml:"metrics_dir"`

	Client     client.Client
	ResultDefs map[string]*ResultDef
	SetDefs    info.SetDefs
	DimStores  map[string]*DimStore
}

func (m *Magellan) Write(metrics []telegraf.Metric) error {
	if m.Client == nil {
		return nil
	}
	ctx := context.Background()
	return m.process(ctx, metrics)
}

func (m *Magellan) SampleConfig() string {
	return sampleConfig
}

func (m *Magellan) Description() string {
	return "Configuration for magellan output"
}

func (m *Magellan) Connect() error {
	var err error
	m.ResultDefs = make(map[string]*ResultDef)
	m.loadMetricDefs()
	m.Client, err = createClient(m.URL, m.DbId, m.DbName)
	if err != nil {
		log.Printf("E! Magellan client error: %s", err)
		return err
	}
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
	outputs.Add("magellan", func() telegraf.Output {
		return &Magellan{}
	})
}

func createClient(url string, dbId string, dbName string) (client.Client, error) {
	log.Printf("I! Creating connection to magellan %s:", url)
	c := client.NewOrionResClient(url)
	ctx := context.Background()
	var err error
	if len(dbId) == 0 && len(dbName) > 0 {
		log.Printf("I! Creating database with name %s", dbName)
		dbId, err = client.CreateDB(ctx, c, dbName)
		if err != nil {
			return nil, err
		}
	}
	log.Printf("I! Using database DbId=%s", dbId)
	db, err := client.FindDbId(ctx, c, dbId)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("Failed to find DbId=%s", dbId)
	}
	return client.New(c, db.Id, db.Name), nil
}

func (m *Magellan) loadMetricDefs() error {
	log.Printf("I: load metrics definition directory: %s", m.MetricsDefDir)
	if m.MetricsDefDir == "" {
		return nil
	}
	err := m.SetDefs.ScanFiles(m.MetricsDefDir, nil)
	if err != nil {
		log.Printf("E! ScanFiles error: %s", err)
	}
	log.Printf("I! Dimension sets loaded %d", len(m.SetDefs.Dim))
	log.Printf("I! Result sets loaded %d", len(m.SetDefs.Res))
	return err
}

func (m *Magellan) dimStore(dimName string) *DimStore {
	if m.DimStores == nil {
		m.DimStores = make(map[string]*DimStore)
	}
	if s, ok := m.DimStores[dimName]; ok {
		return s
	}
	s := NewDimStore()
	m.DimStores[dimName] = s
	return s
}
