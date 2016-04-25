package internal_models

import (
	"log"
	"time"

	"github.com/influxdata/telegraf"
)

const (
	// Default size of metrics batch size.
	DEFAULT_METRIC_BATCH_SIZE = 1000

	// Default number of metrics kept. It should be a multiple of batch size.
	DEFAULT_METRIC_BUFFER_LIMIT = 10000
)

// tmpmetrics point to batch of metrics ready to be wrote to output.
// readI point to the oldest batch of metrics (the first to sent to output). It
// may point to nil value if tmpmetrics is empty.
// writeI point to the next slot to buffer a batch of metrics is output fail to
// write.
type RunningOutput struct {
	Name              string
	Output            telegraf.Output
	Config            *OutputConfig
	Quiet             bool
	MetricBufferLimit int
	MetricBatchSize   int

	metrics     *Buffer
	failMetrics *Buffer
}

func NewRunningOutput(
	name string,
	output telegraf.Output,
	conf *OutputConfig,
	batchSize int,
	bufferLimit int,
) *RunningOutput {
	if bufferLimit == 0 {
		bufferLimit = DEFAULT_METRIC_BUFFER_LIMIT
	}
	if batchSize == 0 {
		batchSize = DEFAULT_METRIC_BATCH_SIZE
	}
	ro := &RunningOutput{
		Name:              name,
		metrics:           NewBuffer(batchSize),
		failMetrics:       NewBuffer(bufferLimit),
		Output:            output,
		Config:            conf,
		MetricBufferLimit: bufferLimit,
		MetricBatchSize:   batchSize,
	}
	return ro
}

// AddMetric adds a metric to the output. This function can also write cached
// points if FlushBufferWhenFull is true.
func (ro *RunningOutput) AddMetric(metric telegraf.Metric) {
	if ro.Config.Filter.IsActive {
		if !ro.Config.Filter.ShouldMetricPass(metric) {
			return
		}
	}

	// Filter any tagexclude/taginclude parameters before adding metric
	if len(ro.Config.Filter.TagExclude) != 0 || len(ro.Config.Filter.TagInclude) != 0 {
		// In order to filter out tags, we need to create a new metric, since
		// metrics are immutable once created.
		tags := metric.Tags()
		fields := metric.Fields()
		t := metric.Time()
		name := metric.Name()
		ro.Config.Filter.FilterTags(tags)
		// error is not possible if creating from another metric, so ignore.
		metric, _ = telegraf.NewMetric(name, tags, fields, t)
	}

	ro.metrics.Add(metric)
	if ro.metrics.Len() == ro.MetricBatchSize {
		batch := ro.metrics.Batch(ro.MetricBatchSize)
		err := ro.write(batch)
		if err != nil {
			ro.failMetrics.Add(batch...)
		}
	}
}

// Write writes all cached points to this output.
func (ro *RunningOutput) Write() error {
	if !ro.failMetrics.IsEmpty() {
		bufLen := ro.failMetrics.Len()
		// how many batches of failed writes we need to write.
		nBatches := bufLen/ro.MetricBatchSize + 1
		batchSize := ro.MetricBatchSize

		for i := 0; i < nBatches; i++ {
			// If it's the last batch, only grab the metrics that have not had
			// a write attempt already (this is primarily to preserve order).
			if i == nBatches-1 {
				batchSize = bufLen % ro.MetricBatchSize
			}
			batch := ro.failMetrics.Batch(batchSize)
			err := ro.write(batch)
			if err != nil {
				ro.failMetrics.Add(batch...)
			}
		}
	}

	batch := ro.metrics.Batch(ro.MetricBatchSize)
	err := ro.write(batch)
	if err != nil {
		ro.failMetrics.Add(batch...)
		return err
	}
	return nil
}

func (ro *RunningOutput) write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	start := time.Now()
	err := ro.Output.Write(metrics)
	elapsed := time.Since(start)
	if err == nil {
		if !ro.Quiet {
			log.Printf("Wrote %d metrics to output %s in %s\n",
				len(metrics), ro.Name, elapsed)
		}
	}
	return err
}

// OutputConfig containing name and filter
type OutputConfig struct {
	Name   string
	Filter Filter
}
