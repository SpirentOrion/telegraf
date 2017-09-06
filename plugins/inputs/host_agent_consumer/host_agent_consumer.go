package host_agent_consumer

import (
	"bufio"
	"encoding/json"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
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

	cloudHypervisors  map[string]CloudHypervisor
	cloudInstances    map[string]CloudInstance
	cloudNetworkPorts []CloudNetworkPort

	acc telegraf.Accumulator

	prevTime  time.Time
	prevValue int64
	currValue int64
}

type CloudProvider struct {
	Name     string
	AuthUrl  string
	User     string
	Password string
	Tenant   string
	Provider string
	isValid  bool
}

type CloudHypervisors struct {
	Hypervisors []CloudHypervisor `json:"hypervisors,required"`
}

type CloudHypervisor struct {
	HostIP   string `json:"host_ip,required"`
	HostName string `json:"host_name,required"`
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

const (
	ovsUUID = "11111111-2222-3333-4444-555555555555"
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
    rser = "admin"
    ## cloud password
    password = "password"
    ## cloud tenant
    tenant = "admin"
    ## cloud provider
    provider = "openstack"
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

	h.subscriber, _ = zmq.NewSocket(zmq.SUB)
	h.subscriber.Bind("tcp://*:" + strconv.Itoa(h.SubscriberPort))
	h.subscriber.SetSubscribe("")

	for i, _ := range h.CloudProviders {
		h.CloudProviders[i].isValid = true
	}

	// Initialize Cloud Hypervisors
	h.loadCloudHypervisors()

	// Initialize Cloud Instances
	h.loadCloudInstances()

	// Initialize Cloud Network Ports
	h.loadCloudNetworkPorts()

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
			break
		} else {
			h.msgs <- msg
		}
	}
}

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
				metricsList := metricsMsg.GetMetrics()
				for _, metric := range metricsList {
					values := make(map[string]interface{})
					for _, v := range metric.Values {
						switch v.Value.(type) {
						case *metrics.MetricValue_DoubleValue:
							values[*v.Name] = v.GetDoubleValue()
						case *metrics.MetricValue_Int64Value:
							values[*v.Name] = v.GetInt64Value()
						default:
							panic("unreachable")
						}
					}
					dimensions := make(map[string]string)
					for _, d := range metric.Dimensions {
						dimensions[*d.Name] = *d.Value
						if *d.Name == "hostname" && len(*d.Value) > 0 {
							found := false
							for k, v := range h.cloudHypervisors {
								if strings.HasPrefix(k, *d.Value) {
									found = true
									dimensions["host_ip"] = v.HostIP
								}
							}
							if !found {
								// reload cloud hypervisors - looks like new hypervisor came online
								h.loadCloudHypervisors()
								for k, v := range h.cloudHypervisors {
									if strings.HasPrefix(k, *d.Value) {
										found = true
										dimensions["host_ip"] = v.HostIP
									}
								}
								if !found {
									cloudHypervisor := CloudHypervisor{"0.0.0.0", *d.Value}
									h.cloudHypervisors[*d.Value] = cloudHypervisor
									dimensions["host_ip"] = cloudHypervisor.HostIP
								}
							}
						}
						if *metric.Name == "host_proc_metrics" ||
							*metric.Name == "intel_pcm_core_metrics" ||
							*metric.Name == "libvirt_domain_metrics" ||
							*metric.Name == "libvirt_domain_core_metrics" ||
							*metric.Name == "libvirt_domain_block_metrics" ||
							*metric.Name == "libvirt_domain_interface_metrics" ||
							*metric.Name == "vswitch_interface_metrics" ||
							*metric.Name == "vswitch_dpdk_interface_metrics" ||
							*metric.Name == "avs_vswitch_interface_metrics" ||
							*metric.Name == "avs_vswitch_port_metrics" ||
							*metric.Name == "avs_vswitch_port_queue_metrics" {
							if *d.Name == "libvirt_uuid" && len(*d.Value) > 0 {
								cloudInstance, ok := h.cloudInstances[*d.Value]
								if ok {
									dimensions["instance_name"] = cloudInstance.Name
								} else if *d.Value == ovsUUID {
									cloudInstance = CloudInstance{*d.Value, "ovs"}
									h.cloudInstances[*d.Value] = cloudInstance
									dimensions["instance_name"] = cloudInstance.Name
								} else {
									// load cloud instance for missing instance
									h.loadCloudInstance(*d.Value)
									cloudInstance, ok := h.cloudInstances[*d.Value]
									if ok {
										dimensions["instance_name"] = cloudInstance.Name
									} else {
										cloudInstance = CloudInstance{*d.Value, "unknown"}
										h.cloudInstances[*d.Value] = cloudInstance
										dimensions["instance_name"] = cloudInstance.Name
									}
								}
							}
							if *d.Name == "mac_addr" && *d.Value != "na" {
								found := false
								for _, networkPort := range h.cloudNetworkPorts {
									if networkPort.MacAddress == *d.Value {
										dimensions["network_name"] = networkPort.NetworkName
										found = true
										break
									}
								}
								if !found {
									// reload cloud network ports - looks like new network was instantiated
									h.loadCloudNetworkPorts()
									found := false
									for _, networkPort := range h.cloudNetworkPorts {
										if networkPort.MacAddress == *d.Value {
											dimensions["network_name"] = networkPort.NetworkName
											found = true
											break
										}
									}
									if !found {
										networkPort := CloudNetworkPort{*d.Value, "unknown"}
										h.cloudNetworkPorts = append(h.cloudNetworkPorts, networkPort)
										dimensions["network_name"] = networkPort.NetworkName
									}
								}
							}
						}
					}
					go h.acc.AddFields(*metric.Name, values, dimensions, time.Unix(0, *metric.Timestamp))
					h.currValue++
				}
			}(msg)
		}
	}
}

func (h *HostAgent) loadCloudHypervisors() {
	h.cloudHypervisors = make(map[string]CloudHypervisor)
	for i, c := range h.CloudProviders {
		if c.isValid {
			cmd := exec.Command("./glimpse",
				"-auth-url", c.AuthUrl,
				"-user", c.User,
				"-pass", c.Password,
				"-tenant", c.Tenant,
				"-provider", c.Provider,
				"list", "hypervisors")

			cmdReader, err := cmd.StdoutPipe()
			if err != nil {
				log.Printf("E! Error creating StdoutPipe for glimpse to list hypervisors for cloud %s: %s", c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}
			// read the data from stdout
			buf := bufio.NewReader(cmdReader)

			if err = cmd.Start(); err != nil {
				log.Printf("E! Error starting glimpse to list hypervisors for cloud %s: %s", c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}

			output, _ := buf.ReadString('\n')
			if err = cmd.Wait(); err != nil {
				log.Printf("E! Error returned from glimpse to list hypervisors for cloud %s: %s - %s", c.Name, err.Error(), output)
				h.CloudProviders[i].isValid = false
				continue
			}

			var hypervisors CloudHypervisors
			json.Unmarshal([]byte(output), &hypervisors)

			log.Printf("I! Loading cloud hypervisor names from cloud: %s", c.Name)

			for _, hypervisor := range hypervisors.Hypervisors {
				h.cloudHypervisors[hypervisor.HostName] = hypervisor
			}
		}
	}
}

func (h *HostAgent) loadCloudInstances() {
	h.cloudInstances = make(map[string]CloudInstance)
	for i, c := range h.CloudProviders {
		if c.isValid {
			cmd := exec.Command("./glimpse",
				"-auth-url", c.AuthUrl,
				"-user", c.User,
				"-pass", c.Password,
				"-tenant", c.Tenant,
				"-provider", c.Provider,
				"list", "instances")

			cmdReader, err := cmd.StdoutPipe()
			if err != nil {
				log.Printf("E! Error creating StdoutPipe for glimpse to list instances for cloud %s: %s", c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}
			// read the data from stdout
			buf := bufio.NewReader(cmdReader)

			if err = cmd.Start(); err != nil {
				log.Printf("E! Error starting glimpse to list instances for cloud %s: %s", c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}

			output, _ := buf.ReadString('\n')
			if err = cmd.Wait(); err != nil {
				log.Printf("E! Error returned from glimpse to list instances for cloud %s: %s - %s", c.Name, err.Error(), output)
				h.CloudProviders[i].isValid = false
				continue
			}

			var instances CloudInstances
			json.Unmarshal([]byte(output), &instances)

			log.Printf("I! Loading cloud instance names from cloud %s", c.Name)

			for _, instance := range instances.Instances {
				h.cloudInstances[instance.Id] = instance
			}
		}
	}
}

func (h *HostAgent) loadCloudInstance(instanceId string) {
	h.Lock()
	defer h.Unlock()
	for i, c := range h.CloudProviders {
		if c.isValid {
			cmd := exec.Command("./glimpse",
				"-auth-url", c.AuthUrl,
				"-user", c.User,
				"-pass", c.Password,
				"-tenant", c.Tenant,
				"-provider", c.Provider,
				"list", "instances",
				"-id", instanceId)

			cmdReader, err := cmd.StdoutPipe()
			if err != nil {
				log.Printf("E! Error creating StdoutPipe for glimpse to list instance %s for cloud %s: %s", instanceId, c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}
			// read the data from stdout
			buf := bufio.NewReader(cmdReader)

			if err = cmd.Start(); err != nil {
				log.Printf("E! Error starting glimpse to list instance %s for cloud %s: %s", instanceId, c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}

			output, _ := buf.ReadString('\n')
			if err = cmd.Wait(); err != nil {
				log.Printf("E! Error returned from glimpse to list instance %s for cloud %s: %s", instanceId, c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}

			var instances CloudInstances
			json.Unmarshal([]byte(output), &instances)

			for _, instance := range instances.Instances {
				log.Printf("I! Adding new cloud instance name from cloud %s for instance id %s - instance name = %s", c.Name, instanceId, instance.Name)
				h.cloudInstances[instance.Id] = instance
			}
		}
	}
}

func (h *HostAgent) loadCloudNetworkPorts() {
	for i, c := range h.CloudProviders {
		if c.isValid {
			cmd := exec.Command("./glimpse",
				"-auth-url", c.AuthUrl,
				"-user", c.User,
				"-pass", c.Password,
				"-tenant", c.Tenant,
				"-provider", c.Provider,
				"list", "network-ports")

			cmdReader, err := cmd.StdoutPipe()
			if err != nil {
				log.Printf("E! Error creating StdoutPipe for glimpse to list network-ports for cloud %s: %s", c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}
			// read the data from stdout
			buf := bufio.NewReader(cmdReader)

			if err = cmd.Start(); err != nil {
				log.Printf("E! Error starting glimpse to list network-ports for cloud %s: %s", c.Name, err.Error())
				h.CloudProviders[i].isValid = false
				continue
			}

			output, _ := buf.ReadString('\n')
			if err = cmd.Wait(); err != nil {
				log.Printf("E! Error returned from glimpse to list network-ports for %s: %s - %s", c.Name, err.Error(), output)
				h.CloudProviders[i].isValid = false
				continue
			}

			var networkPorts CloudNetworkPorts
			json.Unmarshal([]byte(output), &networkPorts)

			log.Printf("I! Loading cloud network names from cloud %s", c.Name)

			for _, networkPort := range networkPorts.NetworkPorts {
				h.cloudNetworkPorts = append(h.cloudNetworkPorts, networkPort)
			}
		}
	}
}

func init() {
	inputs.Add("host_agent_consumer", func() telegraf.Input {
		return &HostAgent{}
	})
}
