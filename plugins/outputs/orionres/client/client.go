package client

import (
	"context"
	"net/url"

	cv1 "github.com/SpirentOrion/orion-api/res/client/v1"
	xv1 "github.com/SpirentOrion/orion-api/res/xfer/v1"
	// "github.com/SpirentOrion/orion-client/go/client"
)

// Client orionres output interface
type Client interface {
	UpdateDB(ctx context.Context, ds []xv1.DimensionSet, rs []xv1.ResultSet) error
	WriteDB(ctx context.Context, dbw *xv1.DatabaseWrite) error
}

type client struct {
	Client *cv1.Client
	DbId   string
	DbName string
}

func NewOrionResClient(baseUrl string) *cv1.Client {
	u, _ := url.Parse(baseUrl)
	return cv1.New(u)
}

func CreateDB(ctx context.Context, c *cv1.Client, dbName string) (string, error) {
	db := &xv1.Database{
		Name:          dbName,
		Description:   dbName,
		Datastore:     datastore(),
		DimensionSets: []xv1.DimensionSet{},
		ResultSets: []xv1.ResultSet{
			agentCollectorResultSetDef(),
		},
	}
	newDb, ctx, err := c.CreateDatabase(ctx, db)
	if err != nil {
		return "", err
	}
	return newDb.Id, nil
}

func ListDB(ctx context.Context, c *cv1.Client) ([]xv1.Database, error) {
	dbs, _, err := c.ListDatabases(ctx, nil)
	return dbs, err
}

func FindDbId(ctx context.Context, c *cv1.Client, dbId string) (*xv1.Database, error) {
	dbs, err := ListDB(ctx, c)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db.Id == dbId {
			return &db, nil
		}
	}
	return nil, nil
}

// NewClient creates a new client struct
func New(c *cv1.Client, dbId, dbName string) Client {
	return &client{
		Client: c,
		DbId:   dbId,
		DbName: dbName,
	}
}

func (c *client) UpdateDB(ctx context.Context, ds []xv1.DimensionSet, rs []xv1.ResultSet) error {
	db := &xv1.Database{
		Name:          c.DbName,
		Id:            c.DbId,
		Datastore:     datastore(),
		DimensionSets: ds,
		ResultSets:    rs,
	}
	db, _, err := c.Client.UpdateDatabase(ctx, c.DbId, db)
	return err
}

func (c *client) WriteDB(ctx context.Context, dbw *xv1.DatabaseWrite) error {
	_, err := c.Client.WriteDatabase(ctx, c.DbId, dbw)
	return err
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
