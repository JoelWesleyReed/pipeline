package main

import (
	"context"
	"fmt"
	"time"

	"github.com/JoelWesleyReed/pipeline"

	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel))
	defer logger.Sync()

	// Create the pipeline
	p := pipeline.NewPipeline(logger)

	// Set up process 1
	p1, err := pipeline.NewProcessConcurrent("process1", 2, pipeline.NewSimpleProcessWorker(
		func(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
			time.Sleep(100 * time.Millisecond)
			emit(item)
			return nil
		}))
	if err != nil {
		panic(err)
	}
	p.Add(p1)

	// Set up process 2
	p2, err := pipeline.NewProcessConcurrent("process2", 4, pipeline.NewSimpleProcessWorker(
		func(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
			time.Sleep(200 * time.Millisecond)
			emit(item)
			return nil
		}))
	if err != nil {
		panic(err)
	}
	p.Add(p2)

	// Start go func to send data into pipeline and then shutdown
	go func() {
		defer p.Shutdown()
		for i := 1; i <= 50; i++ {
			if err := p.Submit(i); err != nil {
				panic(err)
			}
		}
	}()

	// Start the pipeline and wait until it has completed
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	s, err := pipeline.NewSinkConcurrent("sink1", 4, pipeline.NewSimpleSinkWorker(
		func(ctx context.Context, id string, item interface{}) error {
			fmt.Printf("%s: %v\n", id, item)
			return nil
		}))
	if err != nil {
		panic(err)
	}
	err = p.Run(ctx, s)
	if err != nil {
		fmt.Println(err)
	}
}
