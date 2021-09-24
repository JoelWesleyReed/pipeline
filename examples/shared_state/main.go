package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/JoelWesleyReed/pipeline"

	"go.uber.org/zap"
)

type process1 struct {
	multiplier int
	sync.Mutex
}

func newProcess1() *process1 {
	return &process1{multiplier: 1}
}

func (p *process1) Process(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
	p.Lock()
	emit(item.(int) * p.multiplier)
	p.multiplier++
	p.Unlock()
	time.Sleep(100 * time.Millisecond)
	return nil
}

func main() {
	logger, _ := zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel))
	defer logger.Sync()

	// Create the pipeline
	p := pipeline.NewPipeline(logger)

	// Set up process 1
	p1, err := pipeline.NewProcessConcurrent("process1", 8, newProcess1())
	if err != nil {
		panic(err)
	}
	p.Add(p1)

	// Start go func to send data into pipeline and then shutdown
	go func() {
		defer p.Shutdown()
		for i := 1; i <= 10; i++ {
			if err := p.Submit(i); err != nil {
				panic(err)
			}
		}
	}()

	// Start the pipeline and wait until it has completed
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = p.Run(ctx, pipeline.NewSinkSingle("sink1", pipeline.NewSimpleSinkWorker(
		func(ctx context.Context, id string, item interface{}) error {
			fmt.Printf("%s: %v\n", id, item)
			return nil
		})))
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Metrics: %s\n", p.Metrics())
}
