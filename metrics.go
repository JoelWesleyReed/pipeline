package pipeline

import (
	"fmt"
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

func newMetrics(locking bool) *metrics {
	return &metrics{}
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

func (m *metrics) String() string {
	if m.locking {
		m.Lock()
	}
	min := time.Duration(m.minTime)
	max := time.Duration(m.maxTime)
	avg := time.Duration(0)
	if m.count > 0 {
		avg = time.Duration(m.sumTime / m.count)
	}
	str := fmt.Sprintf("[min: %v max: %v avg: %v]", min, max, avg)
	if m.locking {
		m.Unlock()
	}
	return str
}
