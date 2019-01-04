package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/SpirentOrion/metrics-service/pkg/metrics/info"
	"github.com/influxdata/telegraf/plugins/outputs/orionres/client"
)

// Client session definition used to write into an orion results database
type SessionClient struct {
	Client     client.Client
	TestKey    string
	DbId       string
	Url        string
	ResultDefs map[string]*ResultDef
	DimStores  map[string]*DimStore
}

type Processor struct {
	sync.RWMutex
	MetricDefs    info.MetricDefs
	AddNewMetrics bool
	client        *SessionClient
}

func New() *Processor {
	return &Processor{}
}

func NewClient(url, dbId, dbName, testKey string) (*SessionClient, error) {
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
	return &SessionClient{
		Client:     client.New(c, db.Id, db.Name),
		TestKey:    testKey,
		DbId:       db.Id,
		ResultDefs: make(map[string]*ResultDef),
		DimStores:  make(map[string]*DimStore),
	}, nil
}

func (p *Processor) SetClient(c *SessionClient) {
	p.Lock()
	defer p.Unlock()
	p.client = c
}

func (p *Processor) Client() *SessionClient {
	p.Lock()
	defer p.Unlock()
	return p.client
}

func (c *SessionClient) dimStore(dimName string) *DimStore {
	if s, ok := c.DimStores[dimName]; ok {
		return s
	}
	s := NewDimStore()
	c.DimStores[dimName] = s
	return s
}
