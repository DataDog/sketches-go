package gk

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/DataDog/sketches-go/dataset"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

var testQuantiles = []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}

var testSizes = []int{3, 5, 10, 100, 1000, 5000}

func EvaluateSketch(t *testing.T, n int, gen dataset.Generator) {
	g := NewGKArray()
	d := dataset.NewDataset()
	for i := 0; i < n; i++ {
		value := gen.Generate()
		g = g.Add(value)
		d.Add(value)
	}
	AssertSketchesAccurate(t, d, g, n)
}

func AssertSketchesAccurate(t *testing.T, d *dataset.Dataset, g GKArray, n int) {
	assert := assert.New(t)
	eps := float64(1.0e-6)
	for _, q := range testQuantiles {
		fmt.Printf("%g, %v, %v, %v\n", q, int64(q*float64(d.Count-1))+1, d.Rank(g.Quantile(q)), EPSILON*(float64(n)))
		assert.InDelta(int64(q*float64(d.Count-1))+1, d.Rank(g.Quantile(q)), EPSILON*(float64(n)))
	}
	assert.Equal(d.Min(), g.Min)
	assert.Equal(d.Max(), g.Max)
	assert.InEpsilon(d.Avg(), g.Avg, eps)
	assert.InEpsilon(d.Sum(), g.Sum, eps)
	assert.Equal(d.Count, g.Count)
}

func TestConstant(t *testing.T) {
	for _, n := range testSizes {
		constantGenerator := dataset.NewConstant(42)
		g := NewGKArray()
		d := dataset.NewDataset()
		for i := 0; i < n; i++ {
			value := constantGenerator.Generate()
			g = g.Add(value)
			d.Add(value)
		}
		for _, q := range testQuantiles {
			assert.Equal(t, 42.0, g.Quantile(q))
		}
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
		g1 := NewGKArray()
		generator1 := dataset.NewNormal(35, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1 = g1.Add(value)
			d.Add(value)
		}
		g2 := NewGKArray()
		generator2 := dataset.NewNormal(50, 2)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2 = g2.Add(value)
			d.Add(value)
		}
		g1 = g1.Merge(g2)

		g3 := NewGKArray()
		generator3 := dataset.NewNormal(40, 0.5)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3 = g3.Add(value)
			d.Add(value)
		}
		g1 = g1.Merge(g3)
		AssertSketchesAccurate(t, d, g1, n)
	}
}

func TestMergeEmpty(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		// Merge a non-empty sketch to an empty sketch
		g1 := NewGKArray()
		g2 := NewGKArray()
		generator := dataset.NewExponential(5)
		for i := 0; i < n; i++ {
			value := generator.Generate()
			g2 = g2.Add(value)
			d.Add(value)
		}
		g1 = g1.Merge(g2)
		AssertSketchesAccurate(t, d, g1, n)

		// Merge an empty sketch to a non-empty sketch
		g3 := NewGKArray()
		g2 = g2.Merge(g3)
		AssertSketchesAccurate(t, d, g2, n)
	}
}

func TestMergeMixed(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		g1 := NewGKArray()
		generator1 := dataset.NewNormal(100, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1 = g1.Add(value)
			d.Add(value)
		}
		g2 := NewGKArray()
		generator2 := dataset.NewExponential(5)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2 = g2.Add(value)
			d.Add(value)
		}
		g1 = g1.Merge(g2)

		g3 := NewGKArray()
		generator3 := dataset.NewExponential(0.1)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3 = g3.Add(value)
			d.Add(value)
		}
		g1 = g1.Merge(g3)

		AssertSketchesAccurate(t, d, g1, n)
	}
}

// Any random GKArray will not cause panic when Add() or Merge() is called
// as long as it passes the IsValid() method
func TestValidDoesNotPanic(t *testing.T) {
	var s1, s2 GKArray
	var q float64
	nTests := 100
	fuzzer := fuzz.New()
	for i := 0; i < nTests; i++ {
		fuzzer.Fuzz(&s1)
		fuzzer.Fuzz(&s2)
		fuzzer.Fuzz(&q)
		s1 = makeValid(s1)
		s2 = makeValid(s2)
		assert.True(t, s1.IsValid())
		assert.True(t, s2.IsValid())
		assert.NotPanics(t, func() { s1.Quantile(q); s1.Merge(s2) })
	}
}

func makeValid(s GKArray) GKArray {
	if len(s.Entries) == 0 {
		s.Count = int64(len(s.Entries))
	}

	gSum := int64(0)
	for _, e := range s.Entries {
		gSum += int64(e.G)
	}
	s.Count = gSum + int64(len(s.Incoming))

	return s
}

func TestQuantiles(t *testing.T) {
	var qVals []float64
	var vals []float64
	nTests := 100
	qFuzzer := fuzz.New().NilChance(0).NumElements(5, 10)
	vFuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	for i := 0; i < nTests; i++ {
		s := NewGKArray()
		qFuzzer.Fuzz(&qVals)
		sort.Float64s(qVals)
		vFuzzer.Fuzz(&vals)
		for _, v := range vals {
			s = s.Add(v)
		}
		s = s.compressWithIncoming(nil)
		quantiles := s.Quantiles(qVals)
		eps := 1.e-6
		for j, q := range qVals {
			if q < 0 || q > 1 {
				assert.True(t, math.IsNaN(quantiles[j]))
			} else {
				assert.InEpsilon(t, s.Quantile(q), quantiles[j], eps)
			}
		}
	}
}

func TestQuantilesInvalid(t *testing.T) {
	s := NewGKArray()
	gen := dataset.NewNormal(35, 1)
	qVals := []float64{-0.2, -0.1, 0.5, 0.75, 0.95, 1.2}
	n := 200
	for i := 0; i < n; i++ {
		s = s.Add(gen.Generate())
	}
	quantiles := s.Quantiles(qVals)
	assert.True(t, math.IsNaN(quantiles[0]))
	assert.True(t, math.IsNaN(quantiles[1]))
	assert.True(t, math.IsNaN(quantiles[5]))
	eps := 1.0e-6
	assert.InEpsilon(t, s.Quantile(0.5), quantiles[2], eps)
	assert.InEpsilon(t, s.Quantile(0.75), quantiles[3], eps)
	assert.InEpsilon(t, s.Quantile(0.95), quantiles[4], eps)
}

// Test that successive Quantile() calls do not modify the sketch
func TestConsistentQuantile(t *testing.T) {
	var vals []float64
	var q float64
	nTests := 200
	vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	fuzzer := fuzz.New()
	for i := 0; i < nTests; i++ {
		s := NewGKArray()
		vfuzzer.Fuzz(&vals)
		fuzzer.Fuzz(&q)
		for _, v := range vals {
			s = s.Add(v)
		}
		q1 := s.Quantile(q)
		q2 := s.Quantile(q)
		assert.Equal(t, q1, q2)
	}
}

// Test that Quantile() calls do not panic for number of values up to 1/epsilon
func TestNoPanic(t *testing.T) {
	s := NewGKArray()
	for i := 0; i < 2*int(1/EPSILON); i++ {
		s = s.Add(float64(i))
		assert.NotPanics(t, func() { s.Quantile(0.9) })
	}
}

// Test that Merge() calls do not modify the sketch
func TestConsistentMerge(t *testing.T) {
	var vals []float64
	nTests := 200
	vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	for i := 0; i < nTests; i++ {
		g := NewGKArray()
		s := NewGKArray()
		vfuzzer.Fuzz(&vals)
		for _, v := range vals {
			g = g.Add(v)
		}
		vfuzzer.Fuzz(&vals)
		for _, v := range vals {
			s = s.Add(v)
		}
		// Copy the sketches before merging
		gBM := g.Copy()
		sBM := s.Copy()
		g.Merge(s)
		// Compare sketches before and after merge
		assert.Equal(t, gBM, g)
		assert.Equal(t, sBM, s)
	}
}
