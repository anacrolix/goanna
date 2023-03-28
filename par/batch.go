package par

type batcher[T any] struct {
	Limit int
	sl    []T
}

func (b *batcher[T]) IsEmpty() bool {
	return len(b.sl) == 0
}

func (b *batcher[T]) IsFull() bool {
	return len(b.sl) >= b.Limit
}

func (b *batcher[T]) Drain() []T {
	ret := b.sl
	b.sl = nil
	return ret
}

func (b *batcher[T]) Submit(t T) {
	if b.sl == nil {
		b.sl = make([]T, 0, b.Limit)
	}
	b.sl = append(b.sl, t)
}
