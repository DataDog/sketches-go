package dogsketch

import (
	"testing"

	"github.com/DataDog/sketches-go/dataset"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

var testAlpha = 0.05
var testMaxBins = 1024
var testMinValue = 1.0e-9

var testQuantiles = []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}

var testSizes = []int{3, 5, 10, 100, 1000}

func EvaluateSketch(t *testing.T, n int, gen dataset.Generator) {
	c := NewConfig(testAlpha, testMaxBins, testMinValue)
	g := NewDogSketch(c)
	d := dataset.NewDataset()
	for i := 0; i < n; i++ {
		value := gen.Generate()
		g.Add(value)
		d.Add(value)
	}
	AssertSketchesAccurate(t, d, g, n, c)
}

func AssertSketchesAccurate(t *testing.T, d *dataset.Dataset, g *DogSketch, n int, c *Config) {
	assert := assert.New(t)
	eps := float64(1.0e-6)
	for _, q := range testQuantiles {
		assert.InDelta(d.Quantile(q), g.Quantile(q), testAlpha*d.Quantile(q))
	}
	assert.Equal(d.Min(), g.min)
	assert.Equal(d.Max(), g.max)
	assert.InEpsilon(d.Avg(), g.avg, eps)
	assert.InEpsilon(d.Sum(), g.sum, eps)
	assert.Equal(d.Count, g.count)
}

func TestConstant(t *testing.T) {
	for _, n := range testSizes {
		constantGenerator := dataset.NewConstant(42)
		c := NewConfig(testAlpha, testMaxBins, testMinValue)
		g := NewDogSketch(c)
		d := dataset.NewDataset()
		for i := 0; i < n; i++ {
			value := constantGenerator.Generate()
			g.Add(value)
			d.Add(value)
		}
		AssertSketchesAccurate(t, d, g, n, c)
	}
}

func TestUniform(t *testing.T) {
	for _, n := range testSizes {
		uniformGenerator := dataset.NewUniform()
		EvaluateSketch(t, n, uniformGenerator)
	}
}

func TestNormal(t *testing.T) {
	for _, n := range testSizes {
		normalGenerator := dataset.NewNormal(35, 1)
		EvaluateSketch(t, n, normalGenerator)
	}
}

func TestExponential(t *testing.T) {
	for _, n := range testSizes {
		expGenerator := dataset.NewExponential(2)
		EvaluateSketch(t, n, expGenerator)
	}
}
func TestMergeNormal(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		c := NewConfig(testAlpha, testMaxBins, testMinValue)
		g1 := NewDogSketch(c)
		generator1 := dataset.NewNormal(35, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1.Add(value)
			d.Add(value)
		}
		g2 := NewDogSketch(c)
		generator2 := dataset.NewNormal(50, 2)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)

		g3 := NewDogSketch(c)
		generator3 := dataset.NewNormal(40, 0.5)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3.Add(value)
			d.Add(value)
		}
		g1.Merge(g3)
		AssertSketchesAccurate(t, d, g1, n, c)
	}
}

func TestMergeEmpty(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		// Merge a non-empty sketch to an empty sketch
		c := NewConfig(testAlpha, testMaxBins, testMinValue)
		g1 := NewDogSketch(c)
		g2 := NewDogSketch(c)
		generator := dataset.NewExponential(5)
		for i := 0; i < n; i++ {
			value := generator.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)
		AssertSketchesAccurate(t, d, g1, n, c)

		// Merge an empty sketch to a non-empty sketch
		g3 := NewDogSketch(c)
		g2.Merge(g3)
		AssertSketchesAccurate(t, d, g2, n, c)
	}
}

func TestMergeMixed(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		c := NewConfig(testAlpha, testMaxBins, testMinValue)
		g1 := NewDogSketch(c)
		generator1 := dataset.NewNormal(100, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1.Add(value)
			d.Add(value)
		}
		g2 := NewDogSketch(c)
		generator2 := dataset.NewExponential(5)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)

		g3 := NewDogSketch(c)
		generator3 := dataset.NewExponential(0.1)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3.Add(value)
			d.Add(value)
		}
		g1.Merge(g3)

		AssertSketchesAccurate(t, d, g1, n, c)
	}
}

// Test that successive Quantile() calls do not modify the sketch
func TestConsistentQuantile(t *testing.T) {
	var vals []float64
	var q float64
	nTests := 200
	vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	fuzzer := fuzz.New()
	c := NewConfig(testAlpha, testMaxBins, testMinValue)
	for i := 0; i < nTests; i++ {
		s := NewDogSketch(c)
		vfuzzer.Fuzz(&vals)
		fuzzer.Fuzz(&q)
		for _, v := range vals {
			s.Add(v)
		}
		q1 := s.Quantile(q)
		q2 := s.Quantile(q)
		assert.Equal(t, q1, q2)
	}
}
