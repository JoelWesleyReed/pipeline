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
			time.Sleep(time.Millisecond * 100)
			if item.(int) == 5 {
				return fmt.Errorf("I don't know what to do with a '%v'", item)
			}
			emit(item)
			return nil
		})))

	// Create the context
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start go func to send data into pipeline and then shutdown
	go func() {
		defer p.Shutdown()
		for i := 1; i <= 10; i++ {
			if err := p.Submit(ctx, i); err != nil {
				panic(err)
			}
		}
	}()

	// Start the pipeline and wait until it has completed
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
