package pipeline

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type sinkSingle struct {
	id          string
	sink        SinkWorker
	srcChan     chan interface{}
	srcMetrics  *metrics
	sinkMetrics *metrics
	logger      *zap.Logger
}

func NewSinkSingle(id string, worker SinkWorker) sink {
	return &sinkSingle{
		id:          id,
		sink:        worker,
		srcMetrics:  newMetrics(false),
		sinkMetrics: newMetrics(false),
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
		srcStartTime := time.Now()
		select {
		case <-ctx.Done():
			return nil
		case item, open := <-s.srcChan:
			if !open {
				return nil
			}
			s.srcMetrics.recordDuration(time.Now().Sub(srcStartTime))
			sinkStartTime := time.Now()
			err := s.sink.Sink(ctx, s.id, item)
			if err != nil {
				return fmt.Errorf("sink '%s' error: %v", s.id, err)
			}
			s.sinkMetrics.recordDuration(time.Now().Sub(sinkStartTime))
		}
	}
}

func (s *sinkSingle) metrics() string {
	return fmt.Sprintf("{ %s: srcWait:%s sink:%s }",
		s.id,
		s.srcMetrics.String(),
		s.sinkMetrics.String())
}
