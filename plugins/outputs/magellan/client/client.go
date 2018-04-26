package client

import (
	"context"
	"log"
	"net/url"

	cv1 "github.com/SpirentOrion/orion-api/res/client/v1"
	xv1 "github.com/SpirentOrion/orion-api/res/xfer/v1"
	// "github.com/SpirentOrion/orion-client/go/client"
)

// Client magellan output interface
type Client interface {
	CreateDB(ctx context.Context) error
	UpdateDB(ctx context.Context, ds []xv1.DimensionSet, rs []xv1.ResultSet) error
	WriteDB(ctx context.Context, dbw *xv1.DatabaseWrite) error
	ValidDB() bool
	ListDB(ctx context.Context) ([]xv1.Database, error)
	FindDB(ctx context.Context) (bool, error)
}

type client struct {
	Client *cv1.Client
	DbName string
	dbId   string
}

// NewClient creates a new client struct
func New(baseUrl string, dbName string) Client {
	u, _ := url.Parse(baseUrl)
	return &client{
		Client: cv1.New(u),
		DbName: dbName,
	}
}

func (c *client) CreateDB(ctx context.Context) error {
	db := &xv1.Database{
		Name:          c.DbName,
		Description:   c.DbName,
		Datastore:     datastore(),
		DimensionSets: []xv1.DimensionSet{},
		ResultSets: []xv1.ResultSet{
			agentCollectorResultSetDef(),
		},
	}
	db, ctx, err := c.Client.CreateDatabase(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	c.dbId = db.Id
	return err
}

func (c *client) UpdateDB(ctx context.Context, ds []xv1.DimensionSet, rs []xv1.ResultSet) error {
	db := &xv1.Database{
		Name:          c.DbName,
		Description:   c.DbName,
		Id:            c.dbId,
		Datastore:     datastore(),
		DimensionSets: ds,
		ResultSets:    rs,
	}

	db, ctx, err := c.Client.UpdateDatabase(ctx, c.dbId, db)
	return err
}

func (c *client) WriteDB(ctx context.Context, dbw *xv1.DatabaseWrite) error {
	ctx, err := c.Client.WriteDatabase(ctx, c.dbId, dbw)
	return err
}

func (c *client) ListDB(ctx context.Context) ([]xv1.Database, error) {
	dbs, ctx, err := c.Client.ListDatabases(ctx, nil)
	return dbs, err

}

func (c *client) ValidDB() bool {
	return (c.dbId != "")
}

func (c *client) FindDB(ctx context.Context) (bool, error) {
	dbs, err := c.ListDB(ctx)
	if err != nil {
		return false, err
	}
	for _, db := range dbs {
		if db.Name == c.DbName {
			c.dbId = db.Id
			return true, nil
		}
	}
	return false, nil
}

func datastore() xv1.DatabaseDatastore {
	return xv1.DatabaseDatastore{
		Id:       "default",
		Provider: "postgres",
	}
}

func agentCollectorResultSetDef() xv1.ResultSet {
	return xv1.ResultSet{
		Name: "agent_collector",
		Facts: []xv1.FieldDefinition{
			xv1.FieldDefinition{
				Name:        "Name",
				DisplayName: "Name",
				Type:        "string",
			},
		},
	}
}
