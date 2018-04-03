package magellan

import (
	"context"
	"testing"

	xv1 "github.com/SpirentOrion/orion-api/res/xfer/v1"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	valid bool
	db    []xv1.Database

	params struct {
		ds  []xv1.DimensionSet
		rs  []xv1.ResultSet
		dbw *xv1.DatabaseWrite
	}
}

func (c *mockClient) reset() {
	c.params.ds = []xv1.DimensionSet{}
	c.params.rs = []xv1.ResultSet{}
	c.params.dbw = nil
}

func (c *mockClient) CreateDB(ctx context.Context) error {
	return nil
}

func (c *mockClient) UpdateDB(ctx context.Context, ds []xv1.DimensionSet, rs []xv1.ResultSet) error {
	c.params.ds = ds
	c.params.rs = rs
	return nil
}

func (c *mockClient) WriteDB(ctx context.Context, dbw *xv1.DatabaseWrite) error {
	c.params.dbw = dbw
	return nil
}

func (c *mockClient) ValidDB() bool {
	return true
}

func (c *mockClient) ListDB(ctx context.Context) ([]xv1.Database, error) {
	dbs := make([]xv1.Database, 0, 0)
	return dbs, nil
}

func (c *mockClient) FindDB(ctx context.Context) (bool, error) {
	return true, nil
}

func TestProcessResultDef(t *testing.T) {
	tm := testutil.MockMetrics()

	client := &mockClient{
		valid: true,
	}
	m := &Magellan{
		Client: client,
	}

	r, err := m.createResultDef(tm[0])
	require.NoError(t, err)
	assert.NotEqual(t, r, nil)

	ctx := context.Background()
	client.reset()
	err = m.writeResultDef(ctx, r)
	require.NoError(t, err)
	assert.Equal(t, len(client.params.rs), 1)
}
