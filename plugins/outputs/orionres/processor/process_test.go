package processor

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

func (c *mockClient) UpdateDB(ctx context.Context, ds []xv1.DimensionSet, rs []xv1.ResultSet) error {
	c.params.ds = ds
	c.params.rs = rs
	return nil
}

func (c *mockClient) WriteDB(ctx context.Context, dbw *xv1.DatabaseWrite) error {
	c.params.dbw = dbw
	return nil
}

func newMockClient() *SessionClient {
	mockClient := &mockClient{
		valid: true,
	}
	return &SessionClient{
		Client:     mockClient,
		resultDefs: make(map[string]*ResultDef),
		dimStores:  make(map[string]*DimStore),
	}
}

func TestProcessResultDef(t *testing.T) {
	tm := testutil.MockMetrics()

	m := New()
	m.AddNewMetrics = true
	m.SetClient(newMockClient())
	client := m.Client().Client.(*mockClient)

	ctx := context.Background()
	client.reset()
	err := m.Process(ctx, tm)
	require.NoError(t, err)
	assert.Equal(t, 1, len(m.Client().resultDefs))
	assert.Equal(t, 1, len(client.params.rs))
	assert.Equal(t, 1, len(client.params.dbw.ResultSets))
}

func TestSetDefProcessResultDef(t *testing.T) {
	tm := testutil.MockMetrics()

	m := New()
	m.AddNewMetrics = true
	m.SetClient(newMockClient())
	client := m.Client().Client.(*mockClient)

	err := m.MetricDefs.ScanFiles("./testdata/results", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(m.MetricDefs.Dim))
	assert.Equal(t, 1, len(m.MetricDefs.Res))
	dim, ok := m.MetricDefs.Dim["mock_agent"]
	assert.Equal(t, true, ok)
	assert.Equal(t, "mock_agent", dim.Name)

	ctx := context.Background()
	client.reset()
	err = m.Process(ctx, tm)
	require.NoError(t, err)
	assert.Equal(t, 1, len(m.Client().resultDefs))
	r, ok := m.Client().resultDefs["test1"]
	assert.Equal(t, true, ok)
	assert.Equal(t, 1, len(r.Dims))

	assert.Equal(t, 1, len(client.params.rs))
	assert.Equal(t, 1, len(client.params.dbw.ResultSets))
	assert.Equal(t, 1, len(client.params.dbw.ResultSets[0].Rows))
	assert.Equal(t, 3, len(client.params.dbw.ResultSets[0].Rows[0]))

}

func BenchmarkProcessResultDef(b *testing.B) {
	tm := testutil.MockMetrics()

	m := New()
	m.AddNewMetrics = true
	m.SetClient(newMockClient())
	client := m.Client().Client.(*mockClient)

	err := m.MetricDefs.ScanFiles("./testdata/results", nil)
	require.NoError(b, err)
	assert.Equal(b, 1, len(m.MetricDefs.Dim))
	assert.Equal(b, 1, len(m.MetricDefs.Res))
	dim, ok := m.MetricDefs.Dim["mock_agent"]
	assert.Equal(b, true, ok)
	assert.Equal(b, "mock_agent", dim.Name)

	ctx := context.Background()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err = m.Process(ctx, tm)
		require.NoError(b, err)
		client.reset()
	}
}
