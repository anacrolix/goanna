package par

import (
	"log"
	"testing"
)

func TestWorkerPanic(t *testing.T) {
	output := MapOpt(
		MapOpts{
			BatchSize: 2,
		},
		func(yield func(int) bool) {
			i := 0
			for yield(i) {
				i++
			}
		},
		func(i int, _ *struct{}) int {
			return i
		},
	)
	for i := range output {
		log.Print(i)
		if i == 42 {
			break
		}
	}
}
