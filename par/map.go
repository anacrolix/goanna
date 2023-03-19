package par

import (
	"runtime"
	"sync"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/goanna/iter"
)

// Runs the map function parallelized over the values in the input iterator. It does batching to
// reduce scheduler and channel overhead. Inspired by Rust's rayon's par_iter. It does not do
// dynamic chunk sizing (yet). You must completely consume the output iterator.
func Map[T, O any, WorkerState any](
	iter iter.Iter[T],
	f func(T, *WorkerState) O,
) iter.Iter[O] {
	numWorkers := runtime.NumCPU()
	batches := make(chan []T)
	outputBatches := make(chan []O)
	var closeOutputsBarrier sync.WaitGroup
	for range g.Range(numWorkers) {
		closeOutputsBarrier.Add(1)
		go func() {
			var workerState WorkerState
			for batch := range batches {
				outputBatch := make([]O, 0, len(batch))
				for _, t := range batch {
					outputBatch = append(outputBatch, f(t, &workerState))
				}
				outputBatches <- outputBatch
			}
			closeOutputsBarrier.Done()
		}()
	}
	go func() {
		var batcher batcher[T]
		batcher.Limit = 1024
		for iter.Next() {
			batcher.Submit(iter.Value())
			if batcher.Full() {
				batches <- batcher.Drain()
			}
		}
		batches <- batcher.Drain()
		close(batches)
		closeOutputsBarrier.Wait()
		close(outputBatches)
	}()
	return &parMapOutputIter[O]{
		outputBatches: outputBatches,
	}
}

type parMapOutputIter[T any] struct {
	outputBatches chan []T
	currentBatch  []T
}

func (me *parMapOutputIter[T]) Next() bool {
	if len(me.currentBatch) > 0 {
		me.currentBatch = me.currentBatch[1:]
	}
	for len(me.currentBatch) == 0 {
		var ok bool
		me.currentBatch, ok = <-me.outputBatches
		if !ok {
			return false
		}
	}
	return len(me.currentBatch) > 0
}

func (me *parMapOutputIter[T]) Value() T {
	return me.currentBatch[0]
}
