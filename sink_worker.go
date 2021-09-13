package pipeline

import "context"

type SinkWorkerFunc func(ctx context.Context, id string, item interface{}) error

type SinkWorker interface {
	Sink(context.Context, string, interface{}) error
}

type SimpleSinkWorker struct {
	f SinkWorkerFunc
}

func NewSimpleSinkWorker(f SinkWorkerFunc) SinkWorker {
	return &SimpleSinkWorker{f}
}

func (ssw *SimpleSinkWorker) Sink(ctx context.Context, id string, item interface{}) error {
	return ssw.f(ctx, id, item)
}
