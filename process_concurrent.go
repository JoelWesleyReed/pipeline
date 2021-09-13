package pipeline

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type processConcurrent struct {
	id      string
	threads int
	worker  ProcessWorker
	srcChan chan interface{}
	dstChan chan interface{}
	logger  *zap.Logger
}

func NewProcessConcurrent(id string, threads int, worker ProcessWorker) (process, error) {
	if threads <= 1 {
		return nil, errors.New("thread setting must be greater than 1")
	}
	return &processConcurrent{
		id:      id,
		threads: threads,
		worker:  worker,
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
				select {
				case <-gctx.Done():
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
	}
	return g.Wait()
}

func (p *processConcurrent) emit(ctx context.Context, item interface{}) {
	select {
	case <-ctx.Done():
		break
	case p.dstChan <- item:
	}
}
