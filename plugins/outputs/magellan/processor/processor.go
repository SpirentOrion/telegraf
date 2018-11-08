package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
	"github.com/influxdata/telegraf/plugins/outputs/magellan/client"
)

type TestClient struct {
	Client  client.Client
	TestKey string
	DbId    string
	Url     string
}

type Processor struct {
	sync.RWMutex
	MetricDefs info.MetricDefs
	ResultDefs map[string]*ResultDef
	DimStores  map[string]*DimStore
	client     *TestClient
}

func New() *Processor {
	return &Processor{
		ResultDefs: make(map[string]*ResultDef),
		DimStores:  make(map[string]*DimStore),
	}
}

func NewClient(url string, dbId string, dbName string, testKey string) (*TestClient, error) {
	c := client.NewOrionResClient(url)
	ctx := context.Background()
	var err error
	if len(dbId) == 0 && len(dbName) > 0 {
		dbId, err = client.CreateDB(ctx, c, dbName)
		if err != nil {
			return nil, err
		}
	}
	db, err := client.FindDbId(ctx, c, dbId)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("Failed to find DbId=%s", dbId)
	}
	return &TestClient{
		Client:  client.New(c, db.Id, db.Name),
		TestKey: testKey,
		DbId:    db.Id,
	}, nil
}

func (p *Processor) dimStore(dimName string) *DimStore {
	if s, ok := p.DimStores[dimName]; ok {
		return s
	}
	s := NewDimStore()
	p.DimStores[dimName] = s
	return s
}

func (p *Processor) SetClient(c *TestClient) {
	p.Lock()
	defer p.Unlock()
	p.client = c
}

func (p *Processor) Client() *TestClient {
	p.Lock()
	defer p.Unlock()
	return p.client
}
