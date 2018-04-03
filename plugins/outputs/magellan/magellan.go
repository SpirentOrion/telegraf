package magellan

import (
	"context"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/outputs/magellan/client"
)

// Magellan output plugin
type Magellan struct {
	URL       string
	DbName    string
	SetPrefix string

	Client     client.Client
	ResultDefs map[string]*ResultDef
}

func (w *Magellan) Write(metrics []telegraf.Metric) error {
	if !w.Client.ValidDB() {
		return nil
	}
	ctx := context.Background()
	return w.process(ctx, metrics)
}

func (w *Magellan) SampleConfig() string {
	return sampleConfig
}

func (w *Magellan) Description() string {
	return "Configuration for magellan output"
}

func (w *Magellan) Connect() error {
	w.ResultDefs = make(map[string]*ResultDef)
	w.Client = client.New(w.URL, w.DbName)
	ctx := context.Background()
	found, err := w.Client.FindDB(ctx)
	if err != nil {
		return err
	}
	if !found {
		err = w.Client.CreateDB(ctx)
		if err != nil {
			return err
		}
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
