package par

import (
	"iter"
	"runtime"
	"sync"
)

// Runs the map function parallelized over the values in the input iterator. It does batching to
// reduce scheduler and channel overhead. Inspired by Rust's rayon's par_iter. It does not do
// dynamic chunk sizing (yet). You must completely consume the output iterator.
func Map[T, O any, WorkerState any](
	iter iter.Seq[T],
	f func(T, *WorkerState) O,
) iter.Seq[O] {
	return MapOpt(defaultMapOpts, iter, f)
}

type MapOpts struct {
	// How many items to group up to submit to workers at a time.
	BatchSize int
	// Just feed the input directly into the output, no workers.
	Sequential bool
}

var defaultMapOpts = MapOpts{
	BatchSize: 1024,
}

// Runs the map function parallelized over the values in the input iterator. It does batching to
// reduce scheduler and channel overhead. Inspired by Rust's rayon's par_iter. It does not do
// dynamic chunk sizing (yet). You must completely consume the output iterator.
func MapOpt[T, O any, WorkerState any](
	opts MapOpts,
	iter iter.Seq[T],
	f func(T, *WorkerState) O,
) iter.Seq[O] {
	if opts.Sequential {
		var workerState WorkerState
		return func(yield func(O) bool) {
			for input := range iter {
				if !yield(f(input, &workerState)) {
					return
				}
			}
		}
	}
	return func(yield func(O) bool) {
		numWorkers := runtime.NumCPU()
		inputBatches := make(chan []T)
		outputBatches := make(chan []O)
		var workersStopped sync.WaitGroup
		closed := make(chan struct{})
		for range numWorkers {
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
			for value := range iter {
				batcher.Submit(value)
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
		outputIter := parMapOutputIter[O]{
			outputBatches:  outputBatches,
			closed:         closed,
			workersStopped: &workersStopped,
		}
		defer outputIter.Close()
		for outputIter.Next() {
			if !yield(outputIter.Value()) {
				return
			}
		}
	}
}

type parMapOutputIter[T any] struct {
	outputBatches  chan []T
	currentBatch   []T
	closed         chan struct{}
	closeOnce      sync.Once
	workersStopped *sync.WaitGroup
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

func (me *parMapOutputIter[T]) Close() error {
	me.closeOnce.Do(func() {
		close(me.closed)
	})
	me.workersStopped.Wait()
	return nil
}
