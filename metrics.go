package pipeline

import (
	"sync"
	"time"
)

type metrics struct {
	initialized bool
	minTime     uint64
	maxTime     uint64
	sumTime     uint64
	count       uint64
	locking     bool
	sync.Mutex
}

type metricsResult struct {
	Min   string `json:"min"`
	Avg   string `json:"avg"`
	Max   string `json:"max"`
	Count uint64 `json:"count"`
}

func newMetrics(locking bool) *metrics {
	return &metrics{
		locking: locking,
	}
}

func (m *metrics) recordDuration(d time.Duration) {
	duration := uint64(d)
	if m.locking {
		m.Lock()
	}
	if !m.initialized {
		m.minTime = duration
		m.maxTime = duration
		m.initialized = true
	} else {
		if duration < m.minTime {
			m.minTime = duration
		}
		if duration > m.maxTime {
			m.maxTime = duration
		}
	}
	m.sumTime += duration
	m.count++
	if m.locking {
		m.Unlock()
	}
}

func (m *metrics) results() *metricsResult {
	if m.locking {
		m.Lock()
	}
	min := time.Duration(m.minTime)
	avg := time.Duration(0)
	if m.count > 0 {
		avg = time.Duration(m.sumTime / m.count)
	}
	max := time.Duration(m.maxTime)
	if m.locking {
		m.Unlock()
	}
	return &metricsResult{min.String(), avg.String(), max.String(), m.count}
}
