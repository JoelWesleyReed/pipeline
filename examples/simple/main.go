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
	p.Add(pipeline.NewProcessSingle("process1", pipeline.NewSimpleProcessWorker(
		func(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
			time.Sleep(100 * time.Millisecond)
			emit(item)
			return nil
		})))

	// Set up process 2
	p.Add(pipeline.NewProcessSingle("process2", pipeline.NewSimpleProcessWorker(
		func(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
			time.Sleep(200 * time.Millisecond)
			emit(item)
			return nil
		})))

	// Start go func to send data into pipeline and then shutdown
	go func() {
		defer func() {
			if err := p.Shutdown(); err != nil {
				panic(err)
			}
		}()
		for i := 1; i <= 5; i++ {
			if err := p.Submit(i); err != nil {
				panic(err)
			}
		}
	}()

	// Start the pipeline and wait until it has completed
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := p.Run(ctx, pipeline.NewSinkSingle("sink1", pipeline.NewSimpleSinkWorker(
		func(ctx context.Context, id string, item interface{}) error {
			fmt.Printf("%s: %v\n", id, item)
			return nil
		})))
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Metrics: %s\n", p.Metrics())
}
