package pipeline

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type pipelineState uint8

const (
	unititialized = iota
	running
	draining
)

const (
	chanSize = 10
)

type process interface {
	setup(srcChan, dstChan chan interface{}, logger *zap.Logger)
	getDstChan() chan interface{}
	run(context.Context) error
}

type sink interface {
	setup(srcChan chan interface{}, logger *zap.Logger)
	run(context.Context) error
}

type Pipeline struct {
	state   pipelineState
	srcChan chan interface{}
	process []process
	sink    sink
	sync.Mutex
	logger *zap.Logger
}

func NewPipeline(logger *zap.Logger) *Pipeline {
	return &Pipeline{
		state:   unititialized,
		srcChan: make(chan interface{}, chanSize),
		process: make([]process, 0),
		logger:  logger,
	}
}

func (p *Pipeline) Add(proc process) error {
	if p.state != unititialized {
		return errors.New("cannot add process to running or draining pipeline")
	}
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
	p.Lock()
	if p.state != unititialized {
		p.Unlock()
		return errors.New("cannot run a running or draining pipeline")
	}
	p.Unlock()
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
	p.Lock()
	p.state = running
	p.Unlock()
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (p *Pipeline) Submit(item interface{}) error {
	p.Lock()
	if p.state == draining {
		p.Unlock()
		return errors.New("cannot submit when pipeline is draining")
	}
	p.Unlock()
	p.srcChan <- item
	return nil
}

func (p *Pipeline) Shutdown() error {
	p.Lock()
	defer p.Unlock()
	if p.state != running {
		return errors.New("can only shutdown a running pipeline")
	}
	p.state = draining
	close(p.srcChan)
	p.logger.Debug("pipeline draining for shutdown")
	return nil
}
