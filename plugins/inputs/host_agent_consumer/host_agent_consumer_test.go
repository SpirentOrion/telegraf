package host_agent_consumer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
