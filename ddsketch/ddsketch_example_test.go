package ddsketch_test

import (
	"fmt"
	"math/rand"

	"github.com/DataDog/sketches-go/ddsketch"
)

func Example() {
	rand.Seed(1234)

	c := ddsketch.NewDefaultConfig()
	sketch := ddsketch.NewDDSketch(c)

	for i := 0; i < 500; i++ {
		v := rand.NormFloat64()
		sketch.Add(v)
	}

	anotherSketch := ddsketch.NewDDSketch(c)
	for i := 0; i < 500; i++ {
		v := rand.NormFloat64()
		anotherSketch.Add(v)
	}
	sketch.Merge(anotherSketch)

	fmt.Println(quantiles(sketch))
	fmt.Println(quantiles(anotherSketch))
	// Output:
	// [-0.06834362559246944 0.6404974768745979 1.2809242125081708 3.0655851632106974]
	// [0.03220264075781927 0.7071609687152794 1.4142438333843486 3.0655851632106974]
}

func quantiles(sketch *ddsketch.DDSketch) []float64 {
	qs := []float64{0.5, 0.75, 0.9, 1}
	quantiles := make([]float64, len(qs))
	for i, q := range qs {
		quantiles[i] = sketch.Quantile(q)
	}
	return quantiles
}
