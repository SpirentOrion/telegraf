package host_agent_consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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

	mapMut sync.RWMutex
	uuidMap map[string]string

	SubscriberPort int
	MapperPort int

	subscriber *zmq.Socket
	mapper *zmq.Socket

	msgs chan []string
	done chan struct{}

	mapBytes chan []byte
	mapDone chan struct{}

	acc telegraf.Accumulator

	prevTime  time.Time
	prevValue int64
	currValue int64
}

var sampleConfig = `
  ## host agent subscriber port
  subscriberPort = 40003
  mapperPort = 9199
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
	h.mapBytes = make(chan []byte)
	h.mapDone = make(chan struct{})

	h.mapMut.Lock()
	h.uuidMap = make(map[string]string)
	h.mapMut.Unlock()

	h.prevTime = time.Now()
	h.prevValue = 0

	h.subscriber, _ = zmq.NewSocket(zmq.SUB)
	h.subscriber.Bind("tcp://*:" + strconv.Itoa(h.SubscriberPort))
	h.subscriber.SetSubscribe("")

	// Start the zmq message subscriber
	go h.subscribe()

	h.mapper, _ = zmq.NewSocket(zmq.PULL)
	h.mapper.Bind("tcp://*:" + strconv.Itoa(h.MapperPort))

	// Start the control listener
	go h.handleMapper()

	log.Printf("Started the host agent consumer service. Subscribing on *:%d\n", h.SubscriberPort)

	return nil
}

func (h *HostAgent) Stop() {
	h.Lock()
	defer h.Unlock()

	close(h.done)
	close(h.mapDone)
	log.Printf("Stopping the host agent consumer service\n")
	if err := h.subscriber.Close(); err != nil {
		log.Printf("Error closing host agent consumer service: %s\n", err.Error())
	}
	log.Println("Stopping the host agent consumer service mapping port")
	if err := h.mapper.Close(); err != nil {
		log.Printf("Error closing host agent consumer service mapping port: %s\n", err.Error())
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
	fmt.Printf("Processed %f host agent metrics per second\n", rate)
	return nil
}

// subscribe() reads all incoming messages from the host agents, and parses
// them into influxdb metric points.
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
					log.Fatal("unmarshaling error: ", err)
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
					}
					// At this point, check whether this set of tags has
					// libvirt_uuid and instance_name. Only fill it if
					// instance_name matches
					uuid := dimensions["libvirt_uuid"]
					iName := dimensions["instance_name"]
					if uuid != "" && iName == "" {
						h.mapMut.RLock()
						iName = h.uuidMap[uuid]
						if iName != "" {
							dimensions["instance_name"] = iName
						}
						h.mapMut.RUnlock()
					}

					h.acc.AddFields(*metric.Name, values, dimensions, time.Unix(0, *metric.Timestamp))
					h.currValue++
				}
			}(msg)
		}
	}
}


func (h *HostAgent) handleMapper() {
	log.Println("Starting mapping listener at port:", h.MapperPort)
	go h.processMapping()
	for {
		bytes, err := h.mapper.RecvBytes(0)
		if err != nil {
			break
		} else {
			log.Println("Processing", len(bytes), "from listener")
			h.mapBytes <- bytes
		}
	}
	log.Println("Mapping listener exited")
}

func (h *HostAgent) processMapping() {
	for {
		select {
		case <-h.mapDone:
			return
		case mapBytes := <-h.mapBytes:
			go func(bytes []byte) {
				mapData := make(map[string]interface{})
				log.Println("unmarshalling", bytes)
				if err := json.Unmarshal(bytes, &mapData); err != nil {
					log.Fatal("map unmarshalling error:", err)
				}
				log.Println("Received mapping information:", mapData)
				if len(mapData) > 0 {
					h.mapMut.Lock()
					for k, v := range mapData {
						h.uuidMap[k] = v.(string)
					}
					h.mapMut.Unlock()
				}
			}(mapBytes)
		}
	}
}

func init() {
	inputs.Add("host_agent_consumer", func() telegraf.Input {
		return &HostAgent{}
	})
}
