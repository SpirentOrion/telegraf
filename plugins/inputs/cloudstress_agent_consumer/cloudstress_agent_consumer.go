package cloudstress_agent_consumer

import (
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/proto/result"

	zmq "github.com/pebbe/zmq4"
)

type CloudStressAgent struct {
	sync.Mutex

	SubscriberPort int
	RecvTimeout    int

	subscriber *zmq.Socket

	deltaValues map[string]map[string]uint64
	deltaRates  map[string]map[string]map[string]int64

	msgs chan []string
	done chan struct{}

	// keep the accumulator internally:
	acc telegraf.Accumulator

	running           bool
	startTime         time.Time
	totalTime         time.Duration
	prevTime          time.Time
	prevMsgsValue     int64
	currMsgsValue     int64
	totalMsgsValue    int64
	prevMetricsValue  int64
	currMetriscValue  int64
	totalMetricsValue int64
}

var sampleConfig = `
  ## cloudstress subscriber port
  subscriberPort = 40004
`

func (c *CloudStressAgent) SampleConfig() string {
	return sampleConfig
}

func (c *CloudStressAgent) Description() string {
	return "Read metrics from cloudstress agents"
}

func (c *CloudStressAgent) Start(acc telegraf.Accumulator) error {
	c.Lock()
	defer c.Unlock()

	c.acc = acc
	//acc.SetPrecision(time.Second, 0)

	c.msgs = make(chan []string)
	c.done = make(chan struct{})

	c.deltaValues = make(map[string]map[string]uint64)
	c.deltaRates = make(map[string]map[string]map[string]int64)

	c.subscriber, _ = zmq.NewSocket(zmq.SUB)
	c.subscriber.Bind("tcp://*:" + strconv.Itoa(c.SubscriberPort))
	//for _, agentIp := range c.AgentList {
	//	c.subscriber.Connect("tcp://" + agentIp + ":" + strconv.Itoa(c.PublisherPort))
	//}
	c.subscriber.SetSubscribe("")

	// Start the zmq message subscriber
	go c.subscribe()

	log.Printf("I! Started the cloudstress agent consumer service. Subscribing on *:%d\n", c.SubscriberPort)

	return nil
}

func (c *CloudStressAgent) Stop() {
	c.Lock()
	defer c.Unlock()

	close(c.done)
	log.Printf("I! Stopping the cloudstress agent consumer service\n")
	if err := c.subscriber.Close(); err != nil {
		log.Printf("E! Error closing cloudstress agent consumer service: %s\n", err.Error())
	}
}

func (c *CloudStressAgent) Gather(acc telegraf.Accumulator) error {
	currTime := time.Now()
	diffTime := currTime.Sub(c.prevTime) / time.Second
	c.prevTime = currTime
	diffMsgsValue := c.currMsgsValue - c.prevMsgsValue
	c.prevMsgsValue = c.currMsgsValue
	diffMetricsValue := c.currMetriscValue - c.prevMetricsValue
	c.prevMetricsValue = c.currMetriscValue

	if diffTime == 0 {
		return nil
	}

	if c.running && diffMsgsValue > 0 {
		c.totalTime = currTime.Sub(c.startTime) / time.Second
	} else {
		if diffMsgsValue == 0 {
			c.running = false
		}
	}

	rate := float64(diffMsgsValue) / float64(diffTime)
	log.Printf("D! Processed %f cloud-stress agent metric messages per second\n", rate)
	log.Printf("D! Processed %d total cloud-stress agent metric messages\n", c.totalMsgsValue)
	rate = float64(diffMetricsValue) / float64(diffTime)
	log.Printf("D! Processed %f cloud-stress agent metric values per second\n", rate)
	log.Printf("D! Processed %d total cloud-stress agent metric values\n", c.totalMetricsValue)
	rate = float64(c.totalMetricsValue) / float64(c.totalTime)
	if math.IsNaN(rate) {
		rate = 0.0
	}
	log.Printf("D! Proccessed on average of %f agent metrics values per second", rate)
	return nil
}

// subscribe() reads all incoming messages from the cloudstress agents, and parses them into
// influxdb metric points.
func (c *CloudStressAgent) subscribe() {
	go c.processMessages()
	for {
		msg, err := c.subscriber.RecvMessage(0)
		if err != nil {
			break
		} else {
			if !c.running {
				c.startTime = time.Now()
				c.running = true
			}
			c.msgs <- msg
		}
	}
}

func (c *CloudStressAgent) processMessages() {
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.msgs:
			updateMsg := &result.Update{}
			err := proto.Unmarshal([]byte(msg[1]), updateMsg)
			if err != nil {
				log.Fatal("E! unmarshaling error: ", err)
			}

			tags := make(map[string]string)
			tags["uuid"] = updateMsg.GetUuid()

			for _, d := range updateMsg.Tags {
				tags[*d.Key] = *d.Value
			}

			timestamp := int64(updateMsg.GetTimestamp())
			loadType := "cs_agent_" + strings.ToLower(updateMsg.GetType().String())

			switch updateMsg.GetType() {
			case result.Update_CPU:
				metrics := c.processCPUMessage(loadType, updateMsg)
				go c.acc.AddFields(loadType, metrics, tags, time.Unix(timestamp, 0))
				c.currMsgsValue++
				c.totalMsgsValue++
				c.currMetriscValue += int64(len(metrics))
				c.totalMetricsValue += int64(len(metrics))
			case result.Update_MEMORY:
				metrics := c.processMemoryMessage(loadType, timestamp, updateMsg)
				go c.acc.AddFields(loadType, metrics, tags, time.Unix(timestamp, 0))
				c.currMsgsValue++
				c.totalMsgsValue++
				c.currMetriscValue += int64(len(metrics))
				c.totalMetricsValue += int64(len(metrics))
			case result.Update_BLOCK:
				metrics := c.processBlockMessage(loadType, timestamp, updateMsg)
				go c.acc.AddFields(loadType, metrics, tags, time.Unix(timestamp, 0))
				c.currMsgsValue++
				c.totalMsgsValue++
				c.currMetriscValue += int64(len(metrics))
				c.totalMetricsValue += int64(len(metrics))
			case result.Update_NETWORK:
				metrics := c.processNetworkMessage(loadType, timestamp, updateMsg)
				go c.acc.AddFields(loadType, metrics, tags, time.Unix(timestamp, 0))
				c.currMsgsValue++
				c.totalMsgsValue++
				c.currMetriscValue += int64(len(metrics))
				c.totalMetricsValue += int64(len(metrics))
			default:
				log.Printf("E! Unknown Type\n")
			}
		}
	}
}

func (c *CloudStressAgent) processCPUMessage(loadType string, msg *result.Update) map[string]interface{} {
	metrics := make(map[string]interface{})
	srcKey := loadType + msg.GetUuid()

	metric := "utmost"
	utmost_delta := c.delta(srcKey, metric, msg.GetCpu().GetUtmost())

	metric = "target"
	metric_delta := c.delta(srcKey, metric, msg.GetCpu().GetTarget())
	metrics[metric] = float64(metric_delta) / float64(utmost_delta) * 100

	metric = "actual"
	metric_delta = c.delta(srcKey, metric, msg.GetCpu().GetActual())
	metrics[metric] = float64(metric_delta) / float64(utmost_delta) * 100

	metric = "system"
	metric_delta = c.delta(srcKey, metric, msg.GetCpu().GetSystem())
	metrics[metric] = float64(metric_delta) / float64(utmost_delta) * 100

	metric = "user"
	metric_delta = c.delta(srcKey, metric, msg.GetCpu().GetUser())
	metrics[metric] = float64(metric_delta) / float64(utmost_delta) * 100

	metric = "steal"
	metric_delta = c.delta(srcKey, metric, msg.GetCpu().GetSteal())
	metrics[metric] = float64(metric_delta) / float64(utmost_delta) * 100

	actual, _ := metrics["actual"].(float64)
	target, _ := metrics["target"].(float64)
	metrics["load_error"] = actual - target

	return metrics
}

func (c *CloudStressAgent) processMemoryMessage(loadType string, currTime int64, msg *result.Update) map[string]interface{} {
	metrics := c.processIOStats(loadType, currTime, msg)

	return metrics
}

func (c *CloudStressAgent) processBlockMessage(loadType string, currTime int64, msg *result.Update) map[string]interface{} {
	metrics := c.processIOStats(loadType, currTime, msg)

	srcKey := msg.GetUuid() + "Read" + loadType

	metric := "latency_min_read"
	metric_delta := c.delta(srcKey, metric, msg.GetIo().GetRead().GetLatencyMin())
	metrics[metric] = metric_delta

	metric = "latency_max_read"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetRead().GetLatencyMax())
	metrics[metric] = metric_delta

	srcKey = msg.GetUuid() + "Write" + loadType

	metric = "latency_min_write"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetWrite().GetLatencyMin())
	metrics[metric] = metric_delta

	metric = "latency_max_write"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetWrite().GetLatencyMax())
	metrics[metric] = metric_delta

	return metrics
}

func (c *CloudStressAgent) processNetworkMessage(loadType string, currTime int64, msg *result.Update) map[string]interface{} {
	metrics := c.processIOStats(loadType, currTime, msg)

	srcKey := msg.GetUuid() + "Read" + loadType

	metric := "latency_min_read"
	metric_delta := c.delta(srcKey, metric, msg.GetIo().GetRead().GetLatencyMin())
	metrics[metric] = metric_delta

	metric = "latency_max_read"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetRead().GetLatencyMax())
	metrics[metric] = metric_delta

	srcKey = msg.GetUuid() + "Write" + loadType

	metric = "latency_min_write"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetWrite().GetLatencyMin())
	metrics[metric] = metric_delta

	metric = "latency_max_write"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetWrite().GetLatencyMax())
	metrics[metric] = metric_delta

	return metrics
}

func (c *CloudStressAgent) processIOStats(loadType string, currTime int64, msg *result.Update) map[string]interface{} {
	metrics := make(map[string]interface{})

	srcKey := msg.GetUuid() + "Read" + loadType

	metric := "ops_target_read"
	ops_target_rate, ops_target_count := c.iocount(currTime, srcKey, metric, msg.GetIo().GetRead().GetOpsTarget())
	metrics[metric+"_rate"] = ops_target_rate
	metrics[metric] = ops_target_count

	metric = "ops_actual_read"
	ops_actual_rate, ops_actual_count := c.iocount(currTime, srcKey, metric, msg.GetIo().GetRead().GetOpsActual())
	metrics[metric+"_rate"] = ops_actual_rate
	metrics[metric] = ops_actual_count
	ops_actual_delta := c.delta(srcKey, metric, msg.GetIo().GetRead().GetOpsActual())

	metric = "bytes_target_read"
	rate, count := c.iocount(currTime, srcKey, metric, msg.GetIo().GetRead().GetBytesTarget())
	metrics[metric+"_rate"] = rate
	metrics[metric] = count

	metric = "bytes_actual_read"
	rate, count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetRead().GetBytesActual())
	metrics[metric+"_rate"] = rate
	metrics[metric] = count

	metric = "io_errors_read"
	rate, count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetRead().GetErrors())
	metrics[metric+"_rate"] = rate
	metrics[metric] = count

	if ops_target_rate != 0 {
		metrics["load_error_read"] = (float64(ops_actual_rate) - float64(ops_target_rate)) / float64(ops_target_rate) * 100
	} else {
		metrics["load_error_read"] = 0.0
	}

	metric = "latency_read"
	metric_delta := c.delta(srcKey, metric, msg.GetIo().GetRead().GetLatency())
	metrics[metric] = float64(metric_delta) / float64(ops_actual_delta) / 1000000000.0

	srcKey = msg.GetUuid() + "Write" + loadType

	metric = "ops_target_write"
	ops_target_rate, ops_target_count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetWrite().GetOpsTarget())
	metrics[metric+"_rate"] = ops_target_rate
	metrics[metric] = ops_target_count

	metric = "ops_actual_write"
	ops_actual_rate, ops_actual_count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetWrite().GetOpsActual())
	metrics[metric+"_rate"] = ops_actual_rate
	metrics[metric] = ops_actual_count
	ops_actual_delta = c.delta(srcKey, metric, msg.GetIo().GetWrite().GetOpsActual())

	metric = "bytes_target_write"
	rate, count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetWrite().GetBytesTarget())
	metrics[metric+"_rate"] = rate
	metrics[metric] = count

	metric = "bytes_actual_write"
	rate, count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetWrite().GetBytesActual())
	metrics[metric+"_rate"] = rate
	metrics[metric] = count

	metric = "io_errors_write"
	rate, count = c.iocount(currTime, srcKey, metric, msg.GetIo().GetWrite().GetErrors())
	metrics[metric+"_rate"] = rate
	metrics[metric] = count

	if ops_target_rate != 0 {
		metrics["load_error_write"] = (float64(ops_actual_rate) - float64(ops_target_rate)) / float64(ops_target_rate) * 100
	} else {
		metrics["load_error_write"] = 0.0
	}

	metric = "latency_write"
	metric_delta = c.delta(srcKey, metric, msg.GetIo().GetWrite().GetLatency())
	metrics[metric] = float64(metric_delta) / float64(ops_actual_delta) / 1000000000.0

	return metrics
}

func (c *CloudStressAgent) delta(srcKey string, metric string, value uint64) uint64 {
	if _, ok := c.deltaValues[srcKey]; ok == false {
		c.deltaValues[srcKey] = make(map[string]uint64)
	}
	if _, ok := c.deltaValues[srcKey][metric]; ok == false {
		c.deltaValues[srcKey][metric] = value
	}
	pv := c.deltaValues[srcKey][metric]
	dv := value - pv
	c.deltaValues[srcKey][metric] = value
	return dv
}

func (c *CloudStressAgent) iorate(currTime int64, srcKey string, metric string, value uint64) float64 {
	if _, ok := c.deltaRates[srcKey]; ok == false {
		c.deltaRates[srcKey] = make(map[string]map[string]int64)
	}
	if _, ok := c.deltaRates[srcKey][metric]; ok == false {
		c.deltaRates[srcKey][metric] = make(map[string]int64)
	}
	if _, ok := c.deltaRates[srcKey][metric]["time"]; ok == false {
		c.deltaRates[srcKey][metric]["time"] = 0
	}
	if _, ok := c.deltaRates[srcKey][metric]["value"]; ok == false {
		c.deltaRates[srcKey][metric]["value"] = int64(value)
	}

	pT := c.deltaRates[srcKey][metric]["time"]
	dT := currTime - pT
	pV := c.deltaRates[srcKey][metric]["value"]
	dV := int64(value) - pV
	rate := float64(dV) / float64(dT)

	c.deltaRates[srcKey][metric]["time"] = currTime
	c.deltaRates[srcKey][metric]["value"] = int64(value)

	return rate
}

func (c *CloudStressAgent) iocount(currTime int64, srcKey string, metric string, value uint64) (float64, int64) {
	rate := c.iorate(currTime, srcKey, metric, value)
	return rate, int64(value)
}

func init() {
	inputs.Add("cloudstress_agent_consumer", func() telegraf.Input {
		return &CloudStressAgent{}
	})
}
