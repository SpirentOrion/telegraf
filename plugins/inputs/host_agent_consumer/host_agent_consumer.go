package host_agent_consumer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/proto/metrics"
	zmq "github.com/pebbe/zmq4"
)

type HostAgent struct {
	sync.Mutex

	SubscriberPort int

	CloudProviders []CloudProvider

	subscriber *zmq.Socket

	msgs chan []string
	done chan struct{}

	cloudHypervisors           map[string]CloudHypervisor
	cloudInstancesLock         sync.RWMutex
	cloudInstances             map[string]*CloudInstance
	cloudMacAddrNetworkMap     atomic.Value    // mac to network map.  replaced when updated
	cloudMacAddrNetworkUpdated CloudUpdateTime // last time openstack network was updated

	acc telegraf.Accumulator

	prevTime  time.Time
	prevValue int64
	currValue int64
}

type CloudProvider struct {
	Name         string
	AuthUrl      string
	User         string
	Password     string
	Tenant       string
	Region       string
	TenantDomain string
	UserDomain   string
	Provider     string
	Addr         string
	isValid      bool
}

type CloudHypervisors struct {
	Hypervisors []CloudHypervisor `json:"hypervisors,required"`
}

type CloudHypervisor struct {
	HostIP    string `json:"host_ip,required"`
	HostName  string `json:"host_name,required"`
	CloudName string
}

type CloudInstances struct {
	Instances []CloudInstance `json:"instances,required"`
}

type CloudInstance struct {
	Id   string `json:"id,required"`
	Name string `json:"name,required"`
}

type CloudNetworkPorts struct {
	NetworkPorts []CloudNetworkPort `json:"network_ports,required"`
}

type CloudNetworkPort struct {
	MacAddress  string `json:"mac_address,required"`
	NetworkName string `json:"network_name,required"`
}

type CloudMacAddrNetworkMap map[string]*CloudNetworkPort

type CloudUpdateTime map[string]time.Time

const (
	ovsUUID             = "11111111-2222-3333-4444-555555555555"
	avsUUID             = "11111111-2222-3333-4444-555555555556"
	unknownLibvirtUUID  = "00000000-0000-0000-0000-000000000000"
	unknownMacAddr      = "00:00:00:00:00:00"
	unknownIpAddr       = "0.0.0.0"
	unknownInstanceName = "unknown"
	unknownNetworkName  = "unknown"
)

var sampleConfig = `
  ## host agent subscriber port
  subscriberPort = 40003
  [[inputs.host_agent_consumer.cloudProviders]]
    ## cloud name
    name = "cloud1"
    ## cloud Auth URL string
    authUrl = "http://10.140.64.103:5000"
    ## cloud user name
    user = "admin"
    ## cloud password
    password = "password"
    ## cloud tenant
    tenant = "admin"
    ## cloud region
    region = "RegionOne"
    ## cloud provider
    provider = "openstack"
    ## cloud Addr
    addr = "10.140.64.103"
`

func (h *HostAgent) SampleConfig() string {
	return sampleConfig
}

func (h *HostAgent) Description() string {
	return "Read metrics from host agents"
}

func (h *HostAgent) Start(acc telegraf.Accumulator) error {
	h.Lock()
	defer h.Unlock()

	h.acc = acc

	h.msgs = make(chan []string)
	h.done = make(chan struct{})

	h.prevTime = time.Now()
	h.prevValue = 0

	subscriber, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		log.Printf("E! Unable to create subscriber socket: %s", err.Error())
		return err
	}
	h.subscriber = subscriber

	err = h.subscriber.SetIpv6(true)
	if err != nil {
		log.Printf("E! Unable to set IPv6 on subscriber socket: %s", err.Error())
		return err
	}

	err = h.subscriber.Bind("tcp://*:" + strconv.Itoa(h.SubscriberPort))
	if err != nil {
		log.Printf("E! Unable to bind to subscriber port %s: %s", "tcp://*:"+strconv.Itoa(h.SubscriberPort), err.Error())
		return err
	}

	err = h.subscriber.SetSubscribe("")
	if err != nil {
		log.Printf("E! Unable to subscribe on subscriber socket: %s", err.Error())
		return err
	}

	for i := range h.CloudProviders {
		h.CloudProviders[i].isValid = true
	}

	// Initialize Cloud Hypervisors
	h.cloudHypervisors = make(map[string]CloudHypervisor)
	h.loadCloudHypervisors()

	// Initialize Cloud Instances
	h.cloudInstances = make(map[string]*CloudInstance)
	h.loadCloudInstances()

	// Initialize Cloud Network Ports
	netPortMap := make(CloudMacAddrNetworkMap)
	h.updateCloudNetworkPorts(netPortMap, []string{})
	h.cloudMacAddrNetworkMapStore(netPortMap)
	h.cloudMacAddrNetworkUpdated = make(CloudUpdateTime)

	// Start the zmq message subscriber
	go h.subscribe()

	log.Printf("I! Started the host agent consumer service. Subscribing on *:%d\n", h.SubscriberPort)

	return nil
}

func (h *HostAgent) Stop() {
	h.Lock()
	defer h.Unlock()

	close(h.done)
	log.Printf("I! Stopping the host agent consumer service\n")
	if err := h.subscriber.Close(); err != nil {
		log.Printf("E! Error closing host agent consumer service: %s\n", err.Error())
	}
}

func (h *HostAgent) Gather(acc telegraf.Accumulator) error {
	currTime := time.Now()
	diffTime := currTime.Sub(h.prevTime) / time.Second
	h.prevTime = currTime
	diffValue := h.currValue - h.prevValue
	h.prevValue = h.currValue

	if diffTime == 0 {
		return nil
	}

	rate := float64(diffValue) / float64(diffTime)
	log.Printf("D! Processed %f host agent metrics per second\n", rate)
	return nil
}

// subscribe() reads all incoming messages from the host agents, and parses them into
// influxdb metric points.
func (h *HostAgent) subscribe() {
	go h.processMessages()
	for {
		msg, err := h.subscriber.RecvMessage(0)
		if err != nil {
			errno := zmq.AsErrno(err)
			if errno == zmq.Errno(syscall.EAGAIN) || errno == zmq.Errno(syscall.EINTR) {
				log.Printf("I! host agent subscriber receive signal %s", err)
				continue
			}
			log.Printf("I! host agent subscriber receive error %s", err)
			break
		} else {
			h.msgs <- msg
		}
	}
}

// parseMetricValues parses metric data sent from the metrics agent and puts it
// into a map.
func (h *HostAgent) parseMetricValues(mv []*metrics.MetricValue) (map[string]interface{}, error) {
	values := make(map[string]interface{})
	for _, v := range mv {
		switch v.Value.(type) {
		case *metrics.MetricValue_DoubleValue:
			values[*v.Name] = v.GetDoubleValue()
		case *metrics.MetricValue_Int64Value:
			values[*v.Name] = v.GetInt64Value()
		default:
			errors.New("unexpected metric value type")
		}
	}
	return values, nil
}

// parseMetricDimensions parses dimension (or tag) data sent from the metrics
// agent and puts it into a map.  Additional dimensional data not available
// from the metrics agent are created and derived off what was received.
// For example, host IP and user-friendly instance names are added as additional
// dimensions.
func (h *HostAgent) parseMetricDimensions(measurement string, md []*metrics.MetricDimension) (map[string]string, error) {
	dimensions := make(map[string]string)
	for _, d := range md {
		dimensions[*d.Name] = *d.Value
	}
	// set the Host IP address given the hostname
	hostName := ""
	if val, found := dimensions["hostname"]; found {
		if len(val) > 0 {
			hostName = val
			hostIP := h.getHypervisorHostIP(hostName)
			if hostIP != "" {
				dimensions["host_ip"] = hostIP
			} else {
				// reload cloud hypervisors - looks like new hypervisor came online
				h.loadCloudHypervisors()
				hostIP = h.getHypervisorHostIP(hostName)
				if hostIP == "" {
					cloudHypervisor := CloudHypervisor{unknownIpAddr, hostName, ""}
					h.cloudHypervisors[hostName] = cloudHypervisor
					hostIP = cloudHypervisor.HostIP
				}
				dimensions["host_ip"] = hostIP
			}
		}
	}
	// set the Instance Name given the libvirt UUID
	if val, found := dimensions["libvirt_uuid"]; found {
		if len(val) > 0 && val != unknownLibvirtUUID {
			instName, err := h.instanceName(val)
			if err != nil {
				// load cloud instance for missing instance
				cloudNames := h.getHypervisorCloudNames(hostName)
				h.loadCloudInstance(val, cloudNames)
				inst, ok := h.cloudInstance(val)
				if ok {
					instName = inst.Name
				} else {
					inst = &CloudInstance{val, unknownInstanceName}
					h.setCloudInstance(val, inst)
					instName = inst.Name
				}
			}
			dimensions["instance_name"] = instName
		}
	}
	// set the Network Name given the mac address
	if macAddr, found := dimensions["mac_addr"]; found {
		if len(hostName) > 0 {
			networkPort, ok := h.cloudMacAddrNetwork(macAddr)
			if ok {
				dimensions["network_name"] = networkPort.NetworkName
			} else {
				// reload cloud network ports - looks like new network was instantiated
				cloudNames := h.getHypervisorCloudNames(hostName)
				networkPort := h.updateCloudNetworkPort(macAddr, cloudNames)
				dimensions["network_name"] = networkPort.NetworkName
			}
		}
	}
	return dimensions, nil
}

// instanceName returns the user-friendly name of the instance matching the
// given libvirt instance UUID.  Error is returned if the lookup fails.
func (h *HostAgent) instanceName(id string) (string, error) {
	inst, ok := h.cloudInstance(id)
	if ok {
		return inst.Name, nil
	} else if id == ovsUUID {
		inst = &CloudInstance{id, "ovs"}
		h.setCloudInstance(id, inst)
		return inst.Name, nil
	} else if id == avsUUID {
		inst = &CloudInstance{id, "avs"}
		h.setCloudInstance(id, inst)
		return inst.Name, nil
	}
	return "", fmt.Errorf("cannot find name for instance %s", id)
}

// processMetrics parses the real-time metric values and dimensions sent by the
// metrics-agent and adds the data to the accumulator.
func (h *HostAgent) processMetrics(m []*metrics.Metric) {
	for _, metric := range m {
		values, err := h.parseMetricValues(metric.GetValues())
		if err != nil {
			panic(err)
		}
		dimensions, err := h.parseMetricDimensions(*metric.Name, metric.GetDimensions())
		if err != nil {
			panic(err)
		}
		go h.acc.AddFields(*metric.Name, values, dimensions, time.Unix(0, *metric.Timestamp))
		h.currValue++
	}
}

// processMessages processes the real-time metrics from the metrics-agent that
// were received on the ZMQ subscription socket.
func (h *HostAgent) processMessages() {
	for {
		select {
		case <-h.done:
			return
		case msg := <-h.msgs:
			go func(msg []string) {
				metricsMsg := &metrics.Metrics{}
				err := proto.Unmarshal([]byte(msg[0]), metricsMsg)
				if err != nil {
					log.Fatal("E! unmarshaling error: ", err)
				}
				h.processMetrics(metricsMsg.GetMetrics())
			}(msg)
		}
	}
}

func (h *HostAgent) loadCloudHypervisors() {
	for i := range h.CloudProviders {
		c := &h.CloudProviders[i]
		if c.isValid {
			output, err := h.runGlimpse(c, "list", "hypervisors")
			if err != nil {
				log.Printf("E! %s", err)
				h.CloudProviders[i].isValid = false
				continue
			}

			var hypervisors CloudHypervisors
			json.Unmarshal([]byte(output), &hypervisors)
			log.Printf("I! Loading cloud hypervisor names from cloud: %s", c.Name)
			for _, hypervisor := range hypervisors.Hypervisors {
				hypervisor.CloudName = c.Name
				h.cloudHypervisors[hypervisor.HostName] = hypervisor
			}
		}
	}
}

func (h *HostAgent) loadCloudInstances() {
	for i := range h.CloudProviders {
		c := &h.CloudProviders[i]
		if c.isValid {
			output, err := h.runGlimpse(c, "list", "instances")
			if err != nil {
				log.Printf("E! %s", err)
				h.CloudProviders[i].isValid = false
				continue
			}

			var instances CloudInstances
			json.Unmarshal([]byte(output), &instances)
			log.Printf("I! Loading cloud instance names from cloud %s, count %d", c.Name, len(instances.Instances))
			for ci := range instances.Instances {
				instance := &instances.Instances[ci]
				h.setCloudInstance(instance.Id, instance)
			}
		}
	}
}

func (h *HostAgent) loadCloudInstance(instanceId string, cloudNames []string) {
	h.Lock()
	defer h.Unlock()
	if _, ok := h.cloudInstance(instanceId); ok {
		return
	}

	for i := range h.CloudProviders {
		c := &h.CloudProviders[i]
		if !c.isValid {
			continue
		}
		if len(cloudNames) > 0 {
			found := false
			for _, cloudName := range cloudNames {
				if c.Name == cloudName {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		output, err := h.runGlimpse(c, "list", "instances", "-id", instanceId)
		if err != nil {
			log.Printf("E! %s", err)
			h.CloudProviders[i].isValid = false
			continue
		}

		var instances CloudInstances
		json.Unmarshal([]byte(output), &instances)
		for ci := range instances.Instances {
			instance := &instances.Instances[ci]
			log.Printf("I! Adding new cloud instance name from cloud %s for instance id %s - instance name = %s", c.Name, instanceId, instance.Name)
			h.setCloudInstance(instance.Id, instance)
		}
	}
}

func (h *HostAgent) updateCloudNetworkPorts(netPortMap CloudMacAddrNetworkMap, cloudNames []string) {
	for i := range h.CloudProviders {
		c := &h.CloudProviders[i]
		if !c.isValid {
			continue
		}
		if len(cloudNames) > 0 {
			found := false
			for _, cloudName := range cloudNames {
				if c.Name == cloudName {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		output, err := h.runGlimpse(c, "list", "network-ports")
		if err != nil {
			log.Printf("E! %s", err)
			h.CloudProviders[i].isValid = false
			continue
		}

		var networkPorts CloudNetworkPorts
		json.Unmarshal([]byte(output), &networkPorts)
		log.Printf("I! Loading cloud network names from cloud %s", c.Name)
		for ni := range networkPorts.NetworkPorts {
			networkPort := &networkPorts.NetworkPorts[ni]
			if _, ok := netPortMap[networkPort.MacAddress]; !ok {
				netPortMap[networkPort.MacAddress] = networkPort
			}
		}
	}
}

func (h *HostAgent) cloudMacAddrNetworkMapLoad() CloudMacAddrNetworkMap {
	return h.cloudMacAddrNetworkMap.Load().(CloudMacAddrNetworkMap)
}

func (h *HostAgent) cloudMacAddrNetworkMapStore(c CloudMacAddrNetworkMap) {
	h.cloudMacAddrNetworkMap.Store(c)
}

func (h *HostAgent) cloudMacAddrNetwork(macAddr string) (*CloudNetworkPort, bool) {
	netPortMap := h.cloudMacAddrNetworkMapLoad()
	v, ok := netPortMap[macAddr]
	return v, ok
}

func (h *HostAgent) cloudInstance(uuid string) (*CloudInstance, bool) {
	h.cloudInstancesLock.RLock()
	c, ok := h.cloudInstances[uuid]
	h.cloudInstancesLock.RUnlock()
	return c, ok
}

func (h *HostAgent) setCloudInstance(uuid string, c *CloudInstance) {
	h.cloudInstancesLock.Lock()
	h.cloudInstances[uuid] = c
	h.cloudInstancesLock.Unlock()
}

func (h *HostAgent) getHypervisorCloudNames(hostName string) []string {
	cloudNames := []string{}
	if len(hostName) == 0 {
		return cloudNames
	}
	lcHostName := strings.ToLower(hostName)
	for _, v := range h.cloudHypervisors {
		// unlikely but in case there are multiple hypervisors with the same hostname in the organization collect all clouds
		if strings.HasPrefix(strings.ToLower(v.HostName), lcHostName) {
			if len(v.CloudName) > 0 {
				cloudNames = append(cloudNames, v.CloudName)
			}
		}
	}
	return cloudNames
}

// getHypervisorHostIP gets the IP address for the given hostname.
func (h *HostAgent) getHypervisorHostIP(hostname string) string {
	for k, v := range h.cloudHypervisors {
		if strings.HasPrefix(strings.ToLower(k), strings.ToLower(hostname)) {
			return v.HostIP
		}
	}
	return ""
}

func (h *HostAgent) updateCloudNetworkPort(macAddr string, cloudNames []string) *CloudNetworkPort {
	currTime := time.Now()

	h.Lock()
	defer h.Unlock()
	var networkPort *CloudNetworkPort
	var ok bool

	netPortMap := h.cloudMacAddrNetworkMapLoad()
	if networkPort, ok = netPortMap[macAddr]; ok {
		return networkPort
	}

	maxRefreshSecs := 5.0
	refresh := false
	for _, cloudName := range cloudNames {
		lastTime, lastTimeOk := h.cloudMacAddrNetworkUpdated[cloudName]
		if !lastTimeOk || (math.Abs(lastTime.Sub(currTime).Seconds()) >= maxRefreshSecs) {
			refresh = true
			break
		}
	}

	newNetPortMap := make(CloudMacAddrNetworkMap)
	for k, v := range netPortMap {
		newNetPortMap[k] = v
	}
	if refresh {
		h.updateCloudNetworkPorts(newNetPortMap, cloudNames)
		for _, cloudName := range cloudNames {
			h.cloudMacAddrNetworkUpdated[cloudName] = currTime
		}
	} else {
		for _, cloudName := range cloudNames {
			log.Printf("D! Skipping cloud network update for cloud %s. Last network update was less than %f seconds from this refresh request", cloudName, maxRefreshSecs)
		}
	}
	if networkPort, ok = newNetPortMap[macAddr]; !ok {
		networkPort = &CloudNetworkPort{macAddr, unknownNetworkName}
		newNetPortMap[macAddr] = networkPort
	}
	h.cloudMacAddrNetworkMapStore(newNetPortMap)
	return networkPort
}

func (h *HostAgent) runGlimpse(c *CloudProvider, args ...string) (string, error) {
	cmdArgs, err := h.glimpseArgs(c, args...)
	if err != nil {
		return "", fmt.Errorf("Error cloud %s for glimpse %s: %s", c.Name, strings.Join(args, " "), err)
	}
	cmd := exec.Command(h.glimpsePath(), cmdArgs...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("Error getting StdoutPipe cloud %s for glimpse %s: %s", c.Name, strings.Join(args, " "), err)
	}
	// read the data from stdout
	buf := bufio.NewReader(cmdReader)
	err = cmd.Start()
	if err != nil {
		return "", fmt.Errorf("Error starting cloud %s glimpse %s: %s", c.Name, strings.Join(args, " "), err)
	}
	output, _ := buf.ReadString('\n')
	if err = cmd.Wait(); err != nil {
		return "", fmt.Errorf("Error returned for cloud %s glimpse %s: %s", c.Name, strings.Join(args, " "), err)
	}
	return output, nil
}

func (h *HostAgent) glimpsePath() string {
	return "./glimpse"
}

func (h *HostAgent) glimpseArgs(c *CloudProvider, args ...string) ([]string, error) {
	a := []string{
		"-provider", c.Provider,
	}
	if c.AuthUrl != "" {
		a = append(a, "-auth-url", c.AuthUrl)
	}
	if c.User != "" {
		a = append(a, "-user", c.User)
	}
	if c.Password != "" {
		a = append(a, "-pass", c.Password)
	}
	if c.Tenant != "" {
		a = append(a, "-tenant", c.Tenant)
	}
	if c.Region != "" {
		a = append(a, "-region", c.Region)
	}
	if c.TenantDomain != "" {
		a = append(a, "-tenant-domain", c.TenantDomain)
	}
	if c.UserDomain != "" {
		a = append(a, "-user-domain", c.UserDomain)
	}
	if c.Addr != "" {
		a = append(a, "-addr", c.Addr)
	}
	return append(a, args...), nil
}

func init() {
	inputs.Add("host_agent_consumer", func() telegraf.Input {
		return &HostAgent{}
	})
}
