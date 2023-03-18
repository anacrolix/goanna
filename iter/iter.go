package iter

import (
	g "github.com/anacrolix/generics"
)

// An iterator over a sequence of values. This avoids using an Option type for easier use as a loop
// condition. An interesting property of this is that it might be necessary to store the value
// inside the iterator after checking Next.
type Iter[T any] interface {
	Value() T
	Next() bool
}

func FromFunc[T any](f func() g.Option[T]) Iter[T] {
	return &fromFunc[T]{f: f}
}

type fromFunc[T any] struct {
	v g.Option[T]
	f func() g.Option[T]
}

func (me *fromFunc[T]) Next() bool {
	me.v = me.f()
	return me.v.Ok
}

func (me *fromFunc[T]) Value() T {
	return me.v.Unwrap()
}

// Channels introduce considerable overhead. Try to avoid this.
func ToChan[T any](iter Iter[T]) <-chan T {
	ch := make(chan T)
	go func() {
		for iter.Next() {
			ch <- iter.Value()
		}
		close(ch)
	}()
	return ch
}
