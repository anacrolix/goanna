package par

import (
	"runtime"
	"sync"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/goanna/iter"
)

type MapOutputIter[T any] interface {
	iter.Iter[T]
	Close() error
}

// Runs the map function parallelized over the values in the input iterator. It does batching to
// reduce scheduler and channel overhead. Inspired by Rust's rayon's par_iter. It does not do
// dynamic chunk sizing (yet). You must completely consume the output iterator.
func Map[T, O any, WorkerState any](
	iter iter.Iter[T],
	f func(T, *WorkerState) O,
) MapOutputIter[O] {
	return MapOpt(defaultMapOpts, iter, f)
}

type MapOpts struct {
	BatchSize int
}

var defaultMapOpts = MapOpts{
	BatchSize: 1024,
}

// Runs the map function parallelized over the values in the input iterator. It does batching to
// reduce scheduler and channel overhead. Inspired by Rust's rayon's par_iter. It does not do
// dynamic chunk sizing (yet). You must completely consume the output iterator.
func MapOpt[T, O any, WorkerState any](
	opts MapOpts,
	iter iter.Iter[T],
	f func(T, *WorkerState) O,
) MapOutputIter[O] {
	numWorkers := runtime.NumCPU()
	inputBatches := make(chan []T)
	outputBatches := make(chan []O)
	var workersStopped sync.WaitGroup
	closed := make(chan struct{})
	for range g.Range(numWorkers) {
		workersStopped.Add(1)
		go func() {
			var workerState WorkerState
		processInput:
			for batch := range inputBatches {
				outputBatch := make([]O, 0, len(batch))
				for _, t := range batch {
					outputBatch = append(outputBatch, f(t, &workerState))
				}
				select {
				case outputBatches <- outputBatch:
				case <-closed:
					break processInput
				}
			}
			workersStopped.Done()
		}()
	}
	go func() {
		var batcher batcher[T]
		batcher.Limit = opts.BatchSize
	batchInput:
		for iter.Next() {
			batcher.Submit(iter.Value())
			if batcher.IsFull() {
				select {
				case inputBatches <- batcher.Drain():
				case <-closed:
					break batchInput
				}
			}
		}
		if !batcher.IsEmpty() {
			select {
			case inputBatches <- batcher.Drain():
			case <-closed:
			}
		}
		close(inputBatches)
		workersStopped.Wait()
		close(outputBatches)
	}()
	return &parMapOutputIter[O]{
		outputBatches:  outputBatches,
		closed:         closed,
		workersStopped: &workersStopped,
	}
}

type parMapOutputIter[T any] struct {
	outputBatches  chan []T
	currentBatch   []T
	closed         chan struct{}
	closeOnce      sync.Once
	workersStopped *sync.WaitGroup
}

var _ MapOutputIter[struct{}] = (*parMapOutputIter[struct{}])(nil)

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

func (me *parMapOutputIter[T]) Close() error {
	me.closeOnce.Do(func() {
		close(me.closed)
	})
	me.workersStopped.Wait()
	return nil
}
