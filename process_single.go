package pipeline

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

type processSingle struct {
	id      string
	worker  ProcessWorker
	srcChan chan interface{}
	dstChan chan interface{}
	logger  *zap.Logger
}

func NewProcessSingle(id string, worker ProcessWorker) process {
	return &processSingle{
		id:     id,
		worker: worker,
	}
}

func (p *processSingle) setup(srcChan, dstChan chan interface{}, logger *zap.Logger) {
	p.srcChan = srcChan
	p.dstChan = dstChan
	p.logger = logger
}

func (p *processSingle) getDstChan() chan interface{} {
	return p.dstChan
}

func (p *processSingle) run(ctx context.Context) error {
	defer close(p.dstChan)
	p.logger.Debug("process single starting", zap.String("id", p.id))
	defer p.logger.Debug("process single exiting", zap.String("id", p.id))
	for {
		select {
		case <-ctx.Done():
			return nil
		case item, open := <-p.srcChan:
			if !open {
				return nil
			}
			err := p.worker.Process(ctx, p.id, item, func(item interface{}) { p.emit(ctx, item) })
			if err != nil {
				return fmt.Errorf("process '%s' error: %v", p.id, err)
			}
		}
	}
}

func (p *processSingle) emit(ctx context.Context, item interface{}) {
	select {
	case <-ctx.Done():
		break
	case p.dstChan <- item:
	}
}
