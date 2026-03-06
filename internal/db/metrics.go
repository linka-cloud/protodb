// Copyright 2023 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package db

import (
	"expvar"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "protodb"

var (
	badgerCollector = newBadgerCollector()
	metrics         = Metrics{
		Get:    newOpMetrics("gets"),
		Set:    newOpMetrics("sets"),
		Delete: newOpMetrics("deletes"),
		Watch:  newOpMetrics("watches"),
		Tx: TxMetrics{
			OpMetrics: newOpMetrics("tx"),
			SizeHist: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "tx",
				Name:      "size",
				Buckets:   prometheus.DefBuckets,
			}),
			OpCountHist: promauto.NewHistogram(prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "tx",
				Name:      "operations_count",
				Buckets:   prometheus.DefBuckets,
			}),
			Get:    newOpMetrics("tx_gets"),
			Set:    newOpMetrics("tx_sets"),
			Delete: newOpMetrics("tx_deletes"),
		},
	}
)

type Metrics struct {
	Get    OpMetrics
	Set    OpMetrics
	Delete OpMetrics
	Watch  OpMetrics
	Tx     TxMetrics
}

func newOpMetrics(op string) OpMetrics {
	labels := []string{"message"}
	return OpMetrics{
		OpsCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: op,
			Name:      "total",
		}, labels),
		ErrorsCounter: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: op,
			Name:      "error_total",
		}, labels),
		DurationHist: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: op,
			Name:      "duration_seconds",
			Buckets:   prometheus.DefBuckets,
		}, labels),
		Inflight: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: op,
			Name:      "inflight",
		}, labels),
	}
}

type MetricsEnd interface {
	End()
}

type endFn func()

func (f endFn) End() {
	f()
}

type OpMetrics struct {
	OpsCounter    *prometheus.CounterVec
	ErrorsCounter *prometheus.CounterVec
	DurationHist  *prometheus.HistogramVec
	Inflight      *prometheus.GaugeVec
}

func (m *OpMetrics) Start(lvs ...string) MetricsEnd {
	m.OpsCounter.WithLabelValues(lvs...).Inc()
	m.Inflight.WithLabelValues(lvs...).Inc()
	start := time.Now()
	return endFn(func() {
		duration := time.Since(start)
		m.Inflight.WithLabelValues(lvs...).Dec()
		m.DurationHist.WithLabelValues(lvs...).Observe(duration.Seconds())
	})
}

type TxMetrics struct {
	OpMetrics
	SizeHist    prometheus.Histogram
	OpCountHist prometheus.Histogram
	Get         OpMetrics
	Set         OpMetrics
	Delete      OpMetrics
}

type badgerMetric struct {
	expvarName string
	desc       *prometheus.Desc
	valueType  prometheus.ValueType
}

type badgerMapMetric struct {
	badgerMetric
}

type badgerMetricsCollector struct {
	scalarMetrics []badgerMetric
	mapMetrics    []badgerMapMetric
}

func newBadgerCollector() *badgerMetricsCollector {
	return &badgerMetricsCollector{
		scalarMetrics: []badgerMetric{
			{
				expvarName: "badger_v3_disk_reads_total",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_disk_reads_total", "Disk reads", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_disk_writes_total",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_disk_writes_total", "Disk writes", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_read_bytes",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_read_bytes", "Read bytes", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_written_bytes",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_written_bytes", "Written bytes", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_gets_total",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_gets_total", "Gets", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_puts_total",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_puts_total", "Puts", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_blocked_puts_total",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_blocked_puts_total", "Blocked puts", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_memtable_gets_total",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_memtable_gets_total", "Memtable gets", nil, nil),
				valueType:  prometheus.CounterValue,
			},
			{
				expvarName: "badger_v3_compactions_current",
				desc:       prometheus.NewDesc(namespace+"_"+"badger_compactions_current", "Current table compactions", nil, nil),
				valueType:  prometheus.GaugeValue,
			},
		},
		mapMetrics: []badgerMapMetric{
			{
				badgerMetric: badgerMetric{
					expvarName: "badger_v3_lsm_level_gets_total",
					desc:       prometheus.NewDesc(namespace+"_"+"badger_lsm_level_gets_total", "LSM level gets", []string{"level"}, nil),
					valueType:  prometheus.CounterValue,
				},
			},
			{
				badgerMetric: badgerMetric{
					expvarName: "badger_v3_lsm_bloom_hits_total",
					desc:       prometheus.NewDesc(namespace+"_"+"badger_lsm_bloom_hits_total", "LSM bloom hits", []string{"level"}, nil),
					valueType:  prometheus.CounterValue,
				},
			},
			{
				badgerMetric: badgerMetric{
					expvarName: "badger_v3_lsm_size_bytes",
					desc:       prometheus.NewDesc(namespace+"_"+"badger_lsm_size_bytes", "LSM size in bytes", []string{"database"}, nil),
					valueType:  prometheus.GaugeValue,
				},
			},
			{
				badgerMetric: badgerMetric{
					expvarName: "badger_v3_vlog_size_bytes",
					desc:       prometheus.NewDesc(namespace+"_"+"badger_vlog_size_bytes", "Value log size in bytes", []string{"database"}, nil),
					valueType:  prometheus.GaugeValue,
				},
			},
			{
				badgerMetric: badgerMetric{
					expvarName: "badger_v3_pending_writes_total",
					desc:       prometheus.NewDesc(namespace+"_"+"badger_pending_writes_total", "Pending writes", []string{"database"}, nil),
					valueType:  prometheus.GaugeValue,
				},
			},
		},
	}
}

func (c *badgerMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.scalarMetrics {
		ch <- metric.desc
	}
	for _, metric := range c.mapMetrics {
		ch <- metric.desc
	}
}

func (c *badgerMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range c.scalarMetrics {
		value, ok := expvarFloat(metric.expvarName)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(metric.desc, metric.valueType, value)
	}

	for _, metric := range c.mapMetrics {
		expvarMap, ok := expvar.Get(metric.expvarName).(*expvar.Map)
		if !ok {
			continue
		}
		expvarMap.Do(func(kv expvar.KeyValue) {
			value, err := strconv.ParseFloat(kv.Value.String(), 64)
			if err != nil {
				return
			}
			ch <- prometheus.MustNewConstMetric(metric.desc, metric.valueType, value, kv.Key)
		})
	}
}

func expvarFloat(name string) (float64, bool) {
	v := expvar.Get(name)
	if v == nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(v.String(), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func init() {
	prometheus.MustRegister(badgerCollector)
}
