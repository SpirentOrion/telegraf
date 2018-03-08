package host_agent_consumer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGlimpseArgs(t *testing.T) {
	h := &HostAgent{}
	c := CloudProvider{
		AuthUrl:  "http://myurl",
		User:     "username",
		Password: "mypassword",
		Tenant:   "admin",
		Provider: "Openstack",
		Addr:     "1.0.0.1",
		isValid:  true,
	}
	a, err := h.glimpseArgs(c, "list", "hypervisors")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(a), 14)

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
