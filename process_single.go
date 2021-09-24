package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type processSingle struct {
	id          string
	worker      ProcessWorker
	srcChan     chan interface{}
	dstChan     chan interface{}
	srcMetrics  *metrics
	procMetrics *metrics
	emitMetrics *metrics
	logger      *zap.Logger
}

func NewProcessSingle(id string, worker ProcessWorker) process {
	return &processSingle{
		id:          id,
		worker:      worker,
		srcMetrics:  newMetrics(false),
		procMetrics: newMetrics(false),
		emitMetrics: newMetrics(false),
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
		srcStartTime := time.Now()
		select {
		case <-ctx.Done():
			return nil
		case item, open := <-p.srcChan:
			if !open {
				return nil
			}
			p.srcMetrics.recordDuration(time.Now().Sub(srcStartTime))
			procStartTime := time.Now()
			emitTimeSum := time.Duration(0)
			err := p.worker.Process(ctx, p.id, item, func(item interface{}) {
				emitStartTime := time.Now()
				p.emit(ctx, item)
				emitDuration := time.Now().Sub(emitStartTime)
				p.emitMetrics.recordDuration(emitDuration)
				emitTimeSum += emitDuration
			})
			if err != nil {
				return fmt.Errorf("process '%s' error: %v", p.id, err)
			}
			procDuration := time.Now().Sub(procStartTime) - emitTimeSum
			p.procMetrics.recordDuration(procDuration)
		}
	}
}

func (p *processSingle) metrics() string {
	return fmt.Sprintf("{ %s: srcWait:%s proc:%s emitWait%s }",
		p.id,
		p.srcMetrics.String(),
		p.procMetrics.String(),
		p.emitMetrics.String())
}

func (p *processSingle) emit(ctx context.Context, item interface{}) {
	select {
	case <-ctx.Done():
		break
	case p.dstChan <- item:
	}
}
