// Copyright 2021 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protodb

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "protodb"

// Source: https://grafana.com/grafana/dashboards/9574
var (
	badgerExpvarCollector = collectors.NewExpvarCollector(map[string]*prometheus.Desc{
		"badger_v2_disk_reads_total":     prometheus.NewDesc(namespace+"_"+"badger_disk_reads_total", "Disk Reads", nil, nil),
		"badger_v2_disk_writes_total":    prometheus.NewDesc(namespace+"_"+"badger_disk_writes_total", "Disk Writes", nil, nil),
		"badger_v2_read_bytes":           prometheus.NewDesc(namespace+"_"+"badger_read_bytes", "Read bytes", nil, nil),
		"badger_v2_written_bytes":        prometheus.NewDesc(namespace+"_"+"badger_written_bytes", "Written bytes", nil, nil),
		"badger_v2_lsm_level_gets_total": prometheus.NewDesc(namespace+"_"+"badger_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
		"badger_v2_lsm_bloom_hits_total": prometheus.NewDesc(namespace+"_"+"badger_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
		"badger_v2_gets_total":           prometheus.NewDesc(namespace+"_"+"badger_gets_total", "Gets", nil, nil),
		"badger_v2_puts_total":           prometheus.NewDesc(namespace+"_"+"badger_puts_total", "Puts", nil, nil),
		"badger_v2_blocked_puts_total":   prometheus.NewDesc(namespace+"_"+"badger_blocked_puts_total", "Blocked Puts", nil, nil),
		"badger_v2_memtable_gets_total":  prometheus.NewDesc(namespace+"_"+"badger_memtable_gets_total", "Memtable gets", nil, nil),
		"badger_v2_lsm_size_bytes":       prometheus.NewDesc(namespace+"_"+"badger_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
		"badger_v2_vlog_size_bytes":      prometheus.NewDesc(namespace+"_"+"badger_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
		"badger_v2_pending_writes_total": prometheus.NewDesc(namespace+"_"+"badger_pending_writes_total", "Pending Writes", []string{"database"}, nil),
	})
	metrics = Metrics{
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

func init() {
	prometheus.MustRegister(badgerExpvarCollector)
}
