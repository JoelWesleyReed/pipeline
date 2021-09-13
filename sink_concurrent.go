package pipeline

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type sinkConcurrent struct {
	id      string
	threads int
	sink    SinkWorker
	srcChan chan interface{}
	logger  *zap.Logger
}

func NewSinkConcurrent(id string, threads int, worker SinkWorker) (sink, error) {
	if threads <= 1 {
		return nil, errors.New("thread setting must be greater than 1")
	}
	return &sinkConcurrent{
		id:      id,
		threads: threads,
		sink:    worker,
	}, nil
}

func (s *sinkConcurrent) setup(srcChan chan interface{}, logger *zap.Logger) {
	s.srcChan = srcChan
	s.logger = logger
}

func (s *sinkConcurrent) run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < s.threads; i++ {
		tid := fmt.Sprintf("%s:%d", s.id, i)
		g.Go(func() error {
			s.logger.Debug("sink concurrent starting", zap.String("id", tid))
			defer s.logger.Debug("sink concurrent exiting", zap.String("id", tid))
			for {
				select {
				case <-gctx.Done():
					return nil
				case item, open := <-s.srcChan:
					if !open {
						return nil
					}
					err := s.sink.Sink(ctx, tid, item)
					if err != nil {
						return fmt.Errorf("sink '%s' error: %v", tid, err)
					}
				}
			}
		})
	}
	return g.Wait()
}