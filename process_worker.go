package pipeline

import "context"

type ProcessWorker interface {
	Process(context.Context, string, interface{}, func(interface{})) error
}

type ProcessWorkerFunc func(ctx context.Context, id string, item interface{}, emit func(interface{})) error

type SimpleProcessWorker struct {
	f ProcessWorkerFunc
}

func NewSimpleProcessWorker(f ProcessWorkerFunc) ProcessWorker {
	return &SimpleProcessWorker{f}
}

func (spw *SimpleProcessWorker) Process(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
	return spw.f(ctx, id, item, emit)
}
