package host_agent_consumer

import (
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

	SubscriberPort int

	subscriber *zmq.Socket

	msgs chan []string
	done chan struct{}

	acc telegraf.Accumulator

	prevTime  time.Time
	prevValue int64
	currValue int64
}

var sampleConfig = `
  ## host agent subscriber port
  subscriberPort = 40003
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

	// Start the zmq message subscriber
	go h.subscribe()

	log.Printf("Started the host agent consumer service. Subscribing on *:%d\n", h.SubscriberPort)

	return nil
}

func (h *HostAgent) Stop() {
	h.Lock()
	defer h.Unlock()

	close(h.done)
	log.Printf("Stopping the host agent consumer service\n")
	if err := h.subscriber.Close(); err != nil {
		log.Printf("Error closing host agent consumer service: %s\n", err.Error())
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
					h.acc.AddFields(*metric.Name, values, dimensions, time.Unix(0, *metric.Timestamp))
					h.currValue++
				}
			}(msg)
		}
	}
}

func init() {
	inputs.Add("host_agent_consumer", func() telegraf.Input {
		return &HostAgent{}
	})
}
