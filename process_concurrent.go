package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type processConcurrent struct {
	id          string
	threads     int
	worker      ProcessWorker
	srcChan     chan interface{}
	dstChan     chan interface{}
	srcMetrics  *metrics
	procMetrics *metrics
	emitMetrics *metrics
	logger      *zap.Logger
}

func NewProcessConcurrent(id string, threads int, worker ProcessWorker) (process, error) {
	if threads <= 1 {
		return nil, errors.New("thread setting must be greater than 1")
	}
	return &processConcurrent{
		id:          id,
		threads:     threads,
		worker:      worker,
		srcMetrics:  newMetrics(true),
		procMetrics: newMetrics(true),
		emitMetrics: newMetrics(true),
	}, nil
}

func (p *processConcurrent) setup(srcChan, dstChan chan interface{}, logger *zap.Logger) {
	p.srcChan = srcChan
	p.dstChan = dstChan
	p.logger = logger
}

func (p *processConcurrent) getDstChan() chan interface{} {
	return p.dstChan
}

func (p *processConcurrent) run(ctx context.Context) error {
	defer close(p.dstChan)
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < p.threads; i++ {
		tid := fmt.Sprintf("%s:%d", p.id, i)
		g.Go(func() error {
			p.logger.Debug("process concurrent starting", zap.String("id", tid))
			defer p.logger.Debug("process concurrent exiting", zap.String("id", tid))
			for {
				srcStartTime := time.Now()
				select {
				case <-gctx.Done():
					return nil
				case item, open := <-p.srcChan:
					if !open {
						return nil
					}
					p.srcMetrics.recordDuration(time.Now().Sub(srcStartTime))
					procStartTime := time.Now()
					emitTimeSum := time.Duration(0)
					err := p.worker.Process(ctx, tid, item, func(item interface{}) {
						emitStartTime := time.Now()
						p.emit(ctx, item)
						emitDuration := time.Now().Sub(emitStartTime)
						p.emitMetrics.recordDuration(emitDuration)
						emitTimeSum += emitDuration
					})
					if err != nil {
						return fmt.Errorf("process '%s' error: %v", tid, err)
					}
					procDuration := time.Now().Sub(procStartTime) - emitTimeSum
					p.procMetrics.recordDuration(procDuration)
				}
			}
		})
	}
	return g.Wait()
}

func (p *processConcurrent) metrics() string {
	return fmt.Sprintf("{ %s: srcWait:%s proc:%s emitWait%s }",
		p.id,
		p.srcMetrics.String(),
		p.procMetrics.String(),
		p.emitMetrics.String())
}

func (p *processConcurrent) emit(ctx context.Context, item interface{}) {
	select {
	case <-ctx.Done():
		break
	case p.dstChan <- item:
	}
}
