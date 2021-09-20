package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type sinkSingle struct {
	id      string
	sink    SinkWorker
	srcChan chan interface{}
	s       *stats
	logger  *zap.Logger
}

func NewSinkSingle(id string, worker SinkWorker) sink {
	return &sinkSingle{
		id:   id,
		sink: worker,
		s:    newStats(false),
	}
}

func (s *sinkSingle) setup(srcChan chan interface{}, logger *zap.Logger) {
	s.srcChan = srcChan
	s.logger = logger
}

func (s *sinkSingle) run(ctx context.Context) error {
	s.logger.Debug("sink single starting", zap.String("id", s.id))
	defer s.logger.Debug("sink single exiting", zap.String("id", s.id))
	for {
		select {
		case <-ctx.Done():
			return nil
		case item, open := <-s.srcChan:
			if !open {
				return nil
			}
			startTime := time.Now()
			err := s.sink.Sink(ctx, s.id, item)
			if err != nil {
				return fmt.Errorf("sink '%s' error: %v", s.id, err)
			}
			s.s.recordDuration(time.Now().Sub(startTime))
		}
	}
}

func (s *sinkSingle) stats() string {
	return fmt.Sprintf("%s:%s", s.id, s.s.String())
}
