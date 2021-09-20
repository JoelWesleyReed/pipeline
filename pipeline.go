package pipeline

import (
	"context"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type pipelineState uint8

const (
	chanSize = 10
)

type process interface {
	setup(srcChan, dstChan chan interface{}, logger *zap.Logger)
	getDstChan() chan interface{}
	run(context.Context) error
	stats() string
}

type sink interface {
	setup(srcChan chan interface{}, logger *zap.Logger)
	run(context.Context) error
	stats() string
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

func (p *Pipeline) Submit(item interface{}) error {
	p.srcChan <- item
	return nil
}

func (p *Pipeline) Stats() string {
	var buf strings.Builder
	for _, p := range p.process {
		buf.WriteString(p.stats())
		buf.WriteString("  ")
	}
	buf.WriteString(p.sink.stats())
	return buf.String()
}

func (p *Pipeline) Shutdown() error {
	close(p.srcChan)
	p.logger.Debug("pipeline draining for shutdown")
	return nil
}
