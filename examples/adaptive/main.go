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
			if item.(int) > 40 {
				time.Sleep(time.Millisecond * 2000)
			}
			emit(item)
			return nil
		})))

	// Set up process 2
	config := &pipeline.ProcessAdaptiveConfig{
		SamplingInterval: 50 * time.Millisecond,
		SamplingWindow:   2,
		ScaleInterval:    time.Second,
	}
	p2, err := pipeline.NewProcessAdaptive("process2", 2, 4, config, pipeline.NewSimpleProcessWorker(
		func(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
			// if item.(int) < 40 {
			time.Sleep(time.Millisecond * 200)
			// }
			emit(item)
			return nil
		}))
	if err != nil {
		panic(err)
	}
	p.Add(p2)

	// Set up process 3
	p.Add(pipeline.NewProcessSingle("process3", pipeline.NewSimpleProcessWorker(
		func(ctx context.Context, id string, item interface{}, emit func(interface{})) error {
			if item.(int) > 40 && item.(int) < 50 {
				time.Sleep(time.Millisecond * 4000)
			}
			emit(item)
			return nil
		})))

	// Start go func to send data into pipeline and then shutdown
	go func() {
		defer p.Shutdown()
		for i := 0; i < 70; i++ {
			if err := p.Submit(i); err != nil {
				panic(err)
			}
		}
	}()

	// Start the pipeline and wait until it has completed
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000)
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
