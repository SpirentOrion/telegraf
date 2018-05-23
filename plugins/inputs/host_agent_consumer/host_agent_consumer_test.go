package host_agent_consumer

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/influxdata/telegraf/proto/metrics"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/assert"
)

const (
	seed                     int64   = 1411689600
	avsVswitchIfcCount       int     = 22
	avsVswitchIfcRatio       float64 = 1.0
	avsVswitchPortCount      int     = 14
	avsVswitchPortRatio      float64 = 1.0
	avsVswitchPortQueueCount int     = 8
	avsVswitchPortQueueRatio float64 = 1.0
	hostProcCount            int     = 51
	hostProcRatio            float64 = 27 / 51
	intelPcmCoreCount        int     = 19
	intelPcmCoreRatio        float64 = 1.0
	intelRdtCoreCount        int     = 5
	intelRdtCoreRatio        float64 = 1.0
	libvirtDomainBlockCount  int     = 13
	libvirtDomainBlockRatio  float64 = 5 / 13
	libvirtDomainCoreCount   int     = 4
	libvirtDomainCoreRatio   float64 = 1.0
	libvirtDomainIfcCount    int     = 16
	libvirtDomainIfcRatio    float64 = 8 / 16
	libvirtDomainCount       int     = 26
	libvirtDomainRatio       float64 = 16 / 26
	vswitchDpdkIfcCount      int     = 48
	vswitchDpdkIfcRatio      float64 = 1.0
	vswitchIfcCount          int     = 24
	vswitchIfcRatio          float64 = 1.0
)

var measurementToDims = map[string][]string{
	"avs_vswitch_interface_metrics": []string{
		"host_ip",
		"hostname",
		"if_index",
		"if_name",
		"if_uuid",
		"instance_name",
		"libvirt_uuid",
		"mac_addr",
		"network_name",
	},
	"avs_vswitch_port_metrics": []string{
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_uuid",
		"mac_addr",
		"network_name",
		"port_id",
		"port_name",
		"port_uuid",
		"socket_id",
	},
	"avs_vswitch_port_queue_metrics": []string{
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_uuid",
		"mac_addr",
		"network_name",
		"port_id",
		"port_name",
		"port_uuid",
		"queue_id",
		"queue_type",
	},
	"host_proc_metrics": []string{
		"comm",
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_domain",
		"libvirt_uuid",
		"pid",
		"parent_pid",
	},
	"intel_pcm_core_metrics": []string{
		"core_id",
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_uuid",
	},
	"intel_rdt_core_metrics": []string{
		"core_id",
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_uuid",
	},
	"libvirt_domain_block_metrics": []string{
		"domain_name",
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_domain",
		"libvirt_uuid",
	},
	"libvirt_domain_core_metrics": []string{
		"core_id",
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_domain",
		"libvirt_uuid",
	},
	"libvirt_domain_interface_metrics": []string{
		"bridge_name",
		"dev_name",
		"host_ip",
		"hostname",
		"if_type",
		"instance_name",
		"libvirt_domain",
		"libvirt_uuid",
		"mac_addr",
		"network_name",
	},
	"libvirt_domain_metrics": []string{
		"host_ip",
		"hostname",
		"instance_name",
		"libvirt_domain",
		"libvirt_uuid",
	},
	"vswitch_dpdk_interface_metrics": []string{
		"host_ip",
		"hostname",
		"if_name",
		"if_type",
		"instance_name",
		"libvirt_uuid",
		"mac_addr",
		"network_name",
		"tag_name",
	},
	"vswitch_interface_metrics": []string{
		"host_ip",
		"hostname",
		"if_name",
		"if_type",
		"instance_name",
		"libvirt_uuid",
		"mac_addr",
		"network_name",
		"tag_name",
	},
}

func TestGlimpseArgs(t *testing.T) {
	h := &HostAgent{}
	c := CloudProvider{
		AuthUrl:  "http://myurl",
		User:     "username",
		Password: "mypassword",
		Tenant:   "admin",
		Region:   "RegionOne",
		Provider: "Openstack",
		Addr:     "1.0.0.1",
		isValid:  true,
	}
	a, err := h.glimpseArgs(c, "list", "hypervisors")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(a), 16)

	c = CloudProvider{
		Provider: "Host-Metrics-Agent",
		Addr:     "1.0.0.1",
		isValid:  true,
	}
	a, err = h.glimpseArgs(c, "list", "hypervisors")
	assert.Equal(t, err, nil)
	assert.Equal(t, a, []string{
		"-provider", "Host-Metrics-Agent",
		"-addr", "1.0.0.1",
		"list", "hypervisors",
	})
}

func BenchmarkMacAddrNetworkMapNoLock(b *testing.B) {
	portMapRef := createNetPortMap()
	for i := 0; i < b.N; i++ {
		portMap := portMapRef
		p, ok := portMap["0:0:0:0:0:1"]
		if !ok {
			b.Fatalf("portMap find failed")
		}
		if p.MacAddress != "0:0:0:0:0:1" {
			b.Fatalf("portMap port failed")
		}
	}
}

func BenchmarkMacAddrNetworkMap(b *testing.B) {
	h := &HostAgent{}
	portMap := createNetPortMap()
	h.cloudMacAddrNetworkMap.Store(portMap)
	for i := 0; i < b.N; i++ {
		portMap := h.cloudMacAddrNetworkMapLoad()
		p, ok := portMap["0:0:0:0:0:1"]
		if !ok {
			b.Fatalf("portMap find failed")
		}
		if p.MacAddress != "0:0:0:0:0:1" {
			b.Fatalf("portMap port check failed")
		}
	}
}

func BenchmarkCloudInstances(b *testing.B) {
	h := &HostAgent{
		cloudInstances: make(map[string]*CloudInstance),
	}
	for i := 4; i < 6; i++ {
		c := &CloudInstance{
			Name: fmt.Sprintf("instance-%d", i),
			Id:   fmt.Sprintf("%d", i),
		}
		h.setCloudInstance(c.Id, c)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c, ok := h.cloudInstance("5")
		if !ok {
			b.Fatalf("cloud instance find failed")
		}
		if c.Id != "5" {
			b.Fatalf("cloud instance check failed")
		}
	}
}

func createNetPortMap() CloudMacAddrNetworkMap {
	portMap := make(CloudMacAddrNetworkMap)
	ports := []CloudNetworkPort{
		CloudNetworkPort{
			MacAddress:  "0:0:0:0:0:1",
			NetworkName: "br-eth0",
		},
		CloudNetworkPort{
			MacAddress:  "0:0:0:0:0:2",
			NetworkName: "br-eth1",
		},
	}
	for p := range ports {
		portMap[ports[p].MacAddress] = &ports[p]
	}
	return portMap
}

func TestParseMetricValues(t *testing.T) {
	t.Parallel()

	h := &HostAgent{}
	fKey := "foo"
	iKey := "bar"
	mv := []*metrics.MetricValue{
		&metrics.MetricValue{
			Name:  &fKey,
			Value: &metrics.MetricValue_DoubleValue{float64(3.14159265359)},
		},
		&metrics.MetricValue{
			Name:  &iKey,
			Value: &metrics.MetricValue_Int64Value{int64(1234567890)},
		},
	}
	values, err := h.parseMetricValues(mv)
	if err != nil {
		t.Fatal("unexpected error parsing metric values", err)
	}
	if len(mv) != 2 {
		t.Fatal("expected 2 items, got", len(mv))
	}
	v, found := values[fKey]
	if !found {
		t.Fatal("cannot find expected metric of type float64")
	}
	fVal, ok := v.(float64)
	if !ok {
		t.Fatal("expected float64 value")
	}
	if fVal != float64(3.14159265359) {
		t.Fatal("unexpected float64 value")
	}
	v, found = values[iKey]
	if !found {
		t.Fatal("cannot find expected metric of type int64")
	}
	iVal, ok := v.(int64)
	if !ok {
		t.Fatal("expected int64 value")
	}
	if iVal != int64(1234567890) {
		t.Fatal("unexpected int64 value")
	}
}

func TestParseMetricDimensions(t *testing.T) {
	t.Parallel()

	// init the host agent and its internals
	h := &HostAgent{}
	h.cloudHypervisors = make(map[string]CloudHypervisor)
	h.cloudInstances = make(map[string]*CloudInstance)
	netPortMap := make(CloudMacAddrNetworkMap)
	h.updateCloudNetworkPorts(netPortMap, []string{})
	h.cloudMacAddrNetworkMapStore(netPortMap)
	h.cloudMacAddrNetworkUpdated = make(CloudUpdateTime)

	// set up test inputs and expected output
	measurement := "libvirt_domain_metrics"
	inputDims := map[string]string{
		"hostname":      "compute-1",
		"host_ip":       "192.168.200.2",
		"libvirt_uuid":  "12345",
		"instance_name": "my-instance",
		"mac_addr":      "00:01:02:03:04:05",
		"foo":           "bar",
	}
	expDims := map[string]string{
		"hostname":      "compute-1",
		"host_ip":       "0.0.0.0",
		"libvirt_uuid":  "12345",
		"instance_name": "unknown",
		"mac_addr":      "00:01:02:03:04:05",
		"network_name":  "unknown",
		"foo":           "bar",
	}
	md := make([]*metrics.MetricDimension, 0, len(inputDims))
	for k, v := range inputDims {
		// must instantiate new string values
		key := k
		val := v
		md = append(md, &metrics.MetricDimension{
			Name:  &key,
			Value: &val,
		})
	}
	values, err := h.parseMetricDimensions(measurement, md)
	if err != nil {
		t.Fatal("unexpected error parsing metric dimensions", err)
	}
	if !reflect.DeepEqual(values, expDims) {
		t.Fatalf("expected dimensions %+v, got %+v", expDims, values)
	}
}

// buildMetricsValues creates a list of metrics values used as test
// input for benchmarking.
//
// metricCount indicates how many total metrics to generate.
// dblToIntRatio is a ratio between 0 and 1 that indicates the ratio of double
// type metrics versus integer type metrics.
func buildMetricsValues(metricCount int, dblToIntRatio float64) []*metrics.MetricValue {
	dblCount := int(math.Ceil(float64(metricCount) * dblToIntRatio))
	intCount := metricCount - dblCount
	var dIdx, iIdx int
	r := rand.New(rand.NewSource(seed))
	mv := make([]*metrics.MetricValue, metricCount)
	for i := 0; i < metricCount; i++ {
		name := fmt.Sprintf("metric_%d", i)
		if dIdx < dblCount {
			val := r.Float64() * math.Pow10(r.Intn(6))
			mv[i] = &metrics.MetricValue{
				Name:  &name,
				Value: &metrics.MetricValue_DoubleValue{val},
			}
		} else if iIdx < intCount {
			val := r.Int63()
			mv[i] = &metrics.MetricValue{
				Name:  &name,
				Value: &metrics.MetricValue_Int64Value{val},
			}
		}
	}
	return mv
}

// buildMetricsDimensions creates a list of metrics dimensions used as test
// input for benchmarking.
//
// measurement indicates which dimensions to generate.
func buildMetricsDimensions(measurement string) []*metrics.MetricDimension {
	var dims []*metrics.MetricDimension
	dimensions, found := measurementToDims[measurement]
	if found {
		for i := 0; i < len(dimensions); i++ {
			name := dimensions[i]
			value := fmt.Sprintf("value_%d", i)
			dims = append(dims, &metrics.MetricDimension{
				Name:  &name,
				Value: &value,
			})
		}
	}
	return dims
}

func benchmarkProcessMetrics(measurement string, metricCount int, dblToIntRatio float64, b *testing.B) {
	// init the host agent and its internals
	h := &HostAgent{
		acc: &testutil.Accumulator{},
	}
	h.cloudHypervisors = make(map[string]CloudHypervisor)
	h.cloudInstances = make(map[string]*CloudInstance)
	netPortMap := make(CloudMacAddrNetworkMap)
	h.updateCloudNetworkPorts(netPortMap, []string{})
	h.cloudMacAddrNetworkMapStore(netPortMap)
	h.cloudMacAddrNetworkUpdated = make(CloudUpdateTime)

	// build the test metric values and dimensions
	mv := buildMetricsValues(metricCount, dblToIntRatio)
	dims := buildMetricsDimensions(measurement)
	if len(dims) == 0 {
		b.Fatal("missing dimensions")
	}

	// build the real-time metric
	name := "test-data"
	ts := int64(12345)
	testData := []*metrics.Metric{
		&metrics.Metric{
			Name:       &name,
			Timestamp:  &ts,
			Values:     mv,
			Dimensions: dims,
		},
	}

	// run the processMetrics function b.N times
	for n := 0; n < b.N; n++ {
		h.processMetrics(testData)
	}
	if h.currValue < 1 {
		b.Fatal("no metrics were processed")
	}
}

func BenchmarkMetricsAVSVswitchIfc(b *testing.B) {
	benchmarkProcessMetrics("avs_vswitch_interface_metrics", avsVswitchIfcCount, avsVswitchIfcRatio, b)
}
func BenchmarkMetricsAVSVswitchPort(b *testing.B) {
	benchmarkProcessMetrics("avs_vswitch_port_metrics", avsVswitchPortCount, avsVswitchPortRatio, b)
}
func BenchmarkMetricsAVSVswitchPortQueue(b *testing.B) {
	benchmarkProcessMetrics("avs_vswitch_port_queue_metrics", avsVswitchPortQueueCount, avsVswitchPortQueueRatio, b)
}
func BenchmarkMetricsHostProc(b *testing.B) {
	benchmarkProcessMetrics("host_proc_metrics", hostProcCount, hostProcRatio, b)
}
func BenchmarkMetricsIntelPCMCore(b *testing.B) {
	benchmarkProcessMetrics("intel_pcm_core_metrics", intelPcmCoreCount, intelPcmCoreRatio, b)
}
func BenchmarkMetricsIntelRDTCore(b *testing.B) {
	benchmarkProcessMetrics("intel_rdt_core_metrics", intelRdtCoreCount, intelRdtCoreRatio, b)
}
func BenchmarkMetricsLibvirtDomain(b *testing.B) {
	benchmarkProcessMetrics("libvirt_domain_metrics", libvirtDomainCount, libvirtDomainRatio, b)
}
func BenchmarkMetricsLibvirtDomainBlock(b *testing.B) {
	benchmarkProcessMetrics("libvirt_domain_block_metrics", libvirtDomainBlockCount, libvirtDomainBlockRatio, b)
}
func BenchmarkMetricsLibvirtDomainCore(b *testing.B) {
	benchmarkProcessMetrics("libvirt_domain_core_metrics", libvirtDomainCoreCount, libvirtDomainCoreRatio, b)
}
func BenchmarkMetricsLibvirtDomainIfc(b *testing.B) {
	benchmarkProcessMetrics("libvirt_domain_interface_metrics", libvirtDomainIfcCount, libvirtDomainIfcRatio, b)
}
func BenchmarkMetricsVswitchDpdkIfc(b *testing.B) {
	benchmarkProcessMetrics("vswitch_dpdk_interface_metrics", vswitchDpdkIfcCount, vswitchDpdkIfcRatio, b)
}
func BenchmarkMetricsVswitchIfc(b *testing.B) {
	benchmarkProcessMetrics("vswitch_interface_metrics", vswitchIfcCount, vswitchIfcRatio, b)
}
