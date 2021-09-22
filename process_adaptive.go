package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mxmCherry/movavg"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	bufChanSize = 10
)

type ProcessAdaptiveConfig struct {
	SamplingInterval time.Duration
	SamplingWindow   int
	ScaleInterval    time.Duration
}

var DefaultProcessAdaptiveConfig = &ProcessAdaptiveConfig{
	SamplingInterval: 5 * time.Second,
	SamplingWindow:   70,
	ScaleInterval:    30 * time.Second,
}

type processAdaptive struct {
	id             string
	minThreads     int
	maxThreads     int
	currentThreads int
	worker         ProcessWorker
	srcChan        chan interface{}
	srcBufChan     chan interface{}
	dstChan        chan interface{}
	dstBufChan     chan interface{}
	ma             movavg.MA
	g              *errgroup.Group
	doneChan       []chan bool
	config         *ProcessAdaptiveConfig
	m              *metrics
	sync.Mutex
	logger *zap.Logger
}

func NewProcessAdaptive(id string, minThreads, maxThreads int, config *ProcessAdaptiveConfig, worker ProcessWorker) (process, error) {
	if minThreads < 1 {
		return nil, errors.New("minimum threads setting must be greater than or equal to 1")
	}
	if maxThreads <= minThreads {
		return nil, errors.New("maximum threads setting must be grather than minimum threads")
	}
	if config == nil {
		config = DefaultProcessAdaptiveConfig
	}
	return &processAdaptive{
		id:             id,
		minThreads:     minThreads,
		maxThreads:     maxThreads,
		currentThreads: minThreads,
		worker:         worker,
		srcBufChan:     make(chan interface{}, bufChanSize),
		dstBufChan:     make(chan interface{}, bufChanSize),
		doneChan:       make([]chan bool, 0),
		config:         config,
		m:              newMetrics(true),
	}, nil
}

func (p *processAdaptive) setup(srcChan, dstChan chan interface{}, logger *zap.Logger) {
	p.srcChan = srcChan
	p.dstChan = dstChan
	p.logger = logger
}

func (p *processAdaptive) getDstChan() chan interface{} {
	return p.dstChan
}

func (p *processAdaptive) run(ctx context.Context) error {
	p.startSrcBuffer(ctx)
	p.startDstBuffer(ctx)
	defer close(p.dstBufChan)
	p.ma = movavg.ThreadSafe(movavg.NewSMA(p.config.SamplingWindow))
	var gctx context.Context
	p.g, gctx = errgroup.WithContext(ctx)
	p.startSamplingTicker(gctx)
	p.startScaleTicker(gctx)
	p.Lock()
	for i := 0; i < p.currentThreads; i++ {
		p.scaleUp(gctx)
	}
	p.Unlock()
	return p.g.Wait()
}

func (p *processAdaptive) startSrcBuffer(ctx context.Context) {
	go func() {
		p.logger.Debug("source buffer starting", zap.String("id", p.id))
		defer p.logger.Debug("source buffer exiting", zap.String("id", p.id))
		defer close(p.srcBufChan)
		for {
			select {
			case <-ctx.Done():
				return
			case item, open := <-p.srcChan:
				if !open {
					return
				}
				select {
				case <-ctx.Done():
					return
				case p.srcBufChan <- item:
				}
			}
		}
	}()
}

func (p *processAdaptive) startDstBuffer(ctx context.Context) {
	go func() {
		p.logger.Debug("destination buffer starting", zap.String("id", p.id))
		defer p.logger.Debug("destination buffer exiting", zap.String("id", p.id))
		defer close(p.dstChan)
		for {
			select {
			case <-ctx.Done():
				return
			case item, open := <-p.dstBufChan:
				if !open {
					return
				}
				select {
				case <-ctx.Done():
					return
				case p.dstChan <- item:
				}
			}
		}
	}()
}

func (p *processAdaptive) scaleUp(ctx context.Context) {
	id := len(p.doneChan)
	tid := fmt.Sprintf("%s:%d", p.id, id)
	doneChan := make(chan bool)
	p.g.Go(func() error {
		p.logger.Debug("process adaptive starting", zap.String("id", tid))
		defer p.logger.Debug("process adaptive exiting", zap.String("id", tid))
		for {
			select {
			case <-doneChan:
				doneChan <- true
				return nil
			case <-ctx.Done():
				return nil
			case item, open := <-p.srcBufChan:
				if !open {
					return nil
				}
				startTime := time.Now()
				err := p.worker.Process(ctx, tid, item, func(item interface{}) { p.emit(ctx, item) })
				if err != nil {
					return fmt.Errorf("process '%s' error: %v", tid, err)
				}
				p.m.recordDuration(time.Now().Sub(startTime))
			}
		}
	})
	p.doneChan = append(p.doneChan, doneChan)
}

func (p *processAdaptive) metrics() string {
	return fmt.Sprintf("%s:%s", p.id, p.m.String())
}

func (p *processAdaptive) emit(ctx context.Context, item interface{}) {
	select {
	case <-ctx.Done():
		break
	case p.dstBufChan <- item:
	}
}

func (p *processAdaptive) scaleDown() {
	idx := len(p.doneChan) - 1
	p.doneChan[idx] <- true
	<-p.doneChan[idx]
	p.doneChan = p.doneChan[:idx]
}

func (p *processAdaptive) startSamplingTicker(ctx context.Context) {
	p.logger.Debug("process adaptive sampling ticker starting", zap.String("id", p.id))
	ticker := time.NewTicker(p.config.SamplingInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				p.logger.Debug("process adaptive sampling ticker shutting down", zap.String("id", p.id))
				return
			case <-ticker.C:
				p.Lock()
				p.ma.Add(float64(len(p.srcBufChan) - len(p.dstBufChan)))
				p.Unlock()
			}
		}
	}()
}

func (p *processAdaptive) startScaleTicker(ctx context.Context) {
	p.logger.Debug("process adaptive scale ticker starting", zap.String("id", p.id))
	ticker := time.NewTicker(p.config.ScaleInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				p.logger.Debug("process adaptive scale ticker shutting down", zap.String("id", p.id))
				return
			case <-ticker.C:
				avg := p.ma.Avg()
				p.Lock()
				if avg > float64(bufChanSize)/2.0 && len(p.doneChan) < p.maxThreads {
					p.logger.Debug("scaling up", zap.String("id", p.id), zap.Int("threads", len(p.doneChan)+1))
					p.scaleUp(ctx)
				} else if avg < float64(-bufChanSize)/2.0 && len(p.doneChan) > p.minThreads {
					p.logger.Debug("scaling down", zap.String("id", p.id), zap.Int("threads", len(p.doneChan)-1))
					p.scaleDown()
				}
				p.Unlock()
			}
		}
	}()
}
