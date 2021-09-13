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

type ProcessAdaptiveConfig struct {
	StatsInterval time.Duration
	StatsWindow   int
	ScaleInterval time.Duration
}

var DefaultProcessAdaptiveConfig = &ProcessAdaptiveConfig{
	StatsInterval: 5 * time.Second,
	StatsWindow:   70,
	ScaleInterval: 30 * time.Second,
}

type processAdaptive struct {
	id             string
	minThreads     int
	maxThreads     int
	currentThreads int
	worker         ProcessWorker
	srcChan        chan interface{}
	dstChan        chan interface{}
	stats          movavg.MA
	g              *errgroup.Group
	doneChan       []chan bool
	config         *ProcessAdaptiveConfig
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
		doneChan:       make([]chan bool, 0),
		config:         config,
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
	defer close(p.dstChan)
	p.stats = movavg.ThreadSafe(movavg.NewSMA(p.config.StatsWindow))
	var gctx context.Context
	p.g, gctx = errgroup.WithContext(ctx)
	p.startStatsTicker(gctx)
	p.startScaleTicker(gctx)
	p.Lock()
	for i := 0; i < p.currentThreads; i++ {
		p.scaleUp(gctx)
	}
	p.Unlock()
	return p.g.Wait()
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
			case item, open := <-p.srcChan:
				if !open {
					return nil
				}
				err := p.worker.Process(ctx, tid, item, func(item interface{}) { p.emit(ctx, item) })
				if err != nil {
					return fmt.Errorf("process '%s' error: %v", tid, err)
				}
			}
		}
	})
	p.doneChan = append(p.doneChan, doneChan)
}

func (p *processAdaptive) emit(ctx context.Context, item interface{}) {
	select {
	case <-ctx.Done():
		break
	case p.dstChan <- item:
	}
}

func (p *processAdaptive) scaleDown() {
	idx := len(p.doneChan) - 1
	p.doneChan[idx] <- true
	<-p.doneChan[idx]
	p.doneChan = p.doneChan[:idx]
}

func (p *processAdaptive) startStatsTicker(ctx context.Context) {
	p.logger.Debug("process adaptive stats ticker starting", zap.String("id", p.id))
	ticker := time.NewTicker(p.config.StatsInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				p.logger.Debug("process adaptive stats ticker shutting down", zap.String("id", p.id))
				return
			case <-ticker.C:
				p.Lock()
				p.stats.Add(float64(len(p.srcChan) - len(p.dstChan)))
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
				avg := p.stats.Avg()
				p.Lock()
				if avg > float64(chanSize)/2.0 && len(p.doneChan) < p.maxThreads {
					p.logger.Debug("scaling up", zap.String("id", p.id), zap.Int("threads", len(p.doneChan)+1))
					p.scaleUp(ctx)
				} else if avg < float64(-chanSize)/2.0 && len(p.doneChan) > p.minThreads {
					p.logger.Debug("scaling down", zap.String("id", p.id), zap.Int("threads", len(p.doneChan)-1))
					p.scaleDown()
				}
				p.Unlock()
			}
		}
	}()
}