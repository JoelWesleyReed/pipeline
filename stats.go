package pipeline

import (
	"fmt"
	"sync"
	"time"
)

type stats struct {
	initialized bool
	minTime     uint64
	maxTime     uint64
	sumTime     uint64
	count       uint64
	locking     bool
	sync.Mutex
}

func newStats(locking bool) *stats {
	return &stats{}
}

func (s *stats) recordDuration(d time.Duration) {
	duration := uint64(d)
	if s.locking {
		s.Lock()
	}
	if !s.initialized {
		s.minTime = duration
		s.maxTime = duration
		s.initialized = true
	} else {
		if duration < s.minTime {
			s.minTime = duration
		}
		if duration > s.maxTime {
			s.maxTime = duration
		}
	}
	s.sumTime += duration
	s.count++
	if s.locking {
		s.Unlock()
	}
}

func (s *stats) String() string {
	if s.locking {
		s.Lock()
	}
	min := time.Duration(s.minTime)
	max := time.Duration(s.maxTime)
	avg := time.Duration(0)
	if s.count > 0 {
		avg = time.Duration(s.sumTime / s.count)
	}
	str := fmt.Sprintf("[min: %v max: %v avg: %v]", min, max, avg)
	if s.locking {
		s.Unlock()
	}
	return str
}
