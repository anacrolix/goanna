package par

import (
	"log"
	"testing"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/goanna/iter"
)

func TestWorkerPanic(t *testing.T) {
	i := 0
	output := MapOpt(
		MapOpts{
			BatchSize: 2,
		},
		iter.FromFunc(func() g.Option[int] {
			i++
			return g.Some(i)
		}),
		func(i int, _ *struct{}) int {
			return i
		})
	for output.Next() {
		log.Print(output.Value())
		if output.Value() == 42 {
			break
		}
	}
	output.Close()
}
