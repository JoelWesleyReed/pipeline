package pipeline

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type pipelineState uint8

const (
	chanSize = 0
)

type processMetrics struct {
	SrcWait  *metricsResult `json:"source_wait"`
	Proc     *metricsResult `json:"processing_time"`
	EmitWait *metricsResult `json:"emit_wait"`
}

type process interface {
	getID() string
	setup(srcChan, dstChan chan interface{}, logger *zap.Logger)
	getDstChan() chan interface{}
	run(context.Context) error
	metrics() *processMetrics
}

type sinkMetrics struct {
	SrcWait *metricsResult `json:"source_wait"`
	Sink    *metricsResult `json:"sink_time"`
}

type sink interface {
	getID() string
	setup(srcChan chan interface{}, logger *zap.Logger)
	run(context.Context) error
	metrics() *sinkMetrics
}

type Pipeline struct {
	srcChan chan interface{}
	process []process
	sink    sink
	logger  *zap.Logger
}

func NewPipeline(logger *zap.Logger) *Pipeline {
	return &Pipeline{
		srcChan: make(chan interface{}, chanSize),
		process: make([]process, 0),
		logger:  logger,
	}
}

func (p *Pipeline) Add(proc process) error {
	dstChan := make(chan interface{}, chanSize)
	srcChan := p.srcChan
	if l := len(p.process); l > 0 {
		srcChan = p.process[l-1].getDstChan()
	}
	proc.setup(srcChan, dstChan, p.logger)
	p.process = append(p.process, proc)
	return nil
}

func (p *Pipeline) Run(ctx context.Context, s sink) error {
	dstChan := p.srcChan
	if len(p.process) > 0 {
		dstChan = p.process[len(p.process)-1].getDstChan()
	}
	s.setup(dstChan, p.logger)
	p.sink = s
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(cctx)
	for _, pw := range p.process {
		func(p process) {
			g.Go(func() error {
				if err := p.run(gctx); err != nil {
					return err
				}
				return nil
			})
		}(pw)
	}
	g.Go(func() error {
		if err := p.sink.run(gctx); err != nil {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (p *Pipeline) Submit(ctx context.Context, item interface{}) error {
	select {
	case <-ctx.Done():
		return nil
	case p.srcChan <- item:
	}
	return nil
}

func (p *Pipeline) Metrics() string {
	type pipelineMetric struct {
		Id      string      `json:"id"`
		Metrics interface{} `json:"metrics"`
	}
	metrics := make([]*pipelineMetric, 0)
	for _, proc := range p.process {
		metrics = append(metrics, &pipelineMetric{proc.getID(), proc.metrics()})
	}
	if p.sink != nil {
		metrics = append(metrics, &pipelineMetric{p.sink.getID(), p.sink.metrics()})
	}
	json, err := json.MarshalIndent(metrics, "", "    ")
	if err != nil {
		panic(err)
	}
	return string(json)
}

func (p *Pipeline) Shutdown() error {
	close(p.srcChan)
	p.logger.Debug("pipeline draining for shutdown")
	return nil
}
