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
	// [0.03238161865314805 0.704644633038024 1.4190131494921128 3.0655851632106974]
	// [0.15410634860115344 0.8780477199413346 1.476926144063773 3.0282161483661967]
}

func quantiles(sketch *ddsketch.DDSketch) []float64 {
	qs := []float64{0.5, 0.75, 0.9, 1}
	quantiles := make([]float64, len(qs))
	for i, q := range qs {
		quantiles[i] = sketch.Quantile(q)
	}
	return quantiles
}
