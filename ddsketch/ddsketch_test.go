// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package ddsketch

import (
	"math"
	"math/rand"
	"testing"

	"github.com/DataDog/sketches-go/dataset"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"

	"github.com/golang/protobuf/proto"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

const (
	floatingPointAcceptableError = 1e-12
)

var (
	testAlphas    = []float64{0.1, 0.01}
	testMaxBins   = 2048
	testQuantiles = []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}
	testSizes     = []int{3, 5, 10, 100, 1000}
)

func EvaluateSketch(t *testing.T, n int, gen dataset.Generator, alpha float64) {
	sketch, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
	data := dataset.NewDataset()
	for i := 0; i < n; i++ {
		value := gen.Generate()
		sketch.Add(value)
		data.Add(value)
	}
	AssertSketchesAccurate(t, data, sketch, alpha)
	// Add negative numbers
	for i := 0; i < n; i++ {
		value := gen.Generate()
		sketch.Add(-value)
		data.Add(-value)
	}
	AssertSketchesAccurate(t, data, sketch, alpha)

	// Serialize/deserialize the sketch
	bytes, err := proto.Marshal(sketch.ToProto())
	assert.Nil(t, err)
	var sketchPb sketchpb.DDSketch
	err = proto.Unmarshal(bytes, &sketchPb)
	assert.Nil(t, err)
	deserializedSketch, err := FromProto(&sketchPb)
	assert.Nil(t, err)
	AssertSketchesAccurate(t, data, deserializedSketch, alpha)
}

func AssertSketchesAccurate(t *testing.T, data *dataset.Dataset, sketch *DDSketch, alpha float64) {
	assert := assert.New(t)
	assert.Equal(data.Count, sketch.GetCount())
	if data.Count == 0 {
		assert.True(sketch.IsEmpty())
	} else {
		for _, q := range testQuantiles {
			lowerQuantile := data.LowerQuantile(q)
			upperQuantile := data.UpperQuantile(q)
			minExpectedValue := math.Min(lowerQuantile*(1-alpha), lowerQuantile*(1+alpha))
			maxExpectedValue := math.Max(upperQuantile*(1-alpha), upperQuantile*(1+alpha))
			quantile, _ := sketch.GetValueAtQuantile(q)
			assert.True(quantile >= minExpectedValue-floatingPointAcceptableError)
			assert.True(quantile <= maxExpectedValue+floatingPointAcceptableError)
		}
	}
}

func TestConstant(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			constantGenerator := dataset.NewConstant(float64(rand.Int()))
			EvaluateSketch(t, n, constantGenerator, alpha)
		}
	}
}

func TestLinear(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			linearGenerator := dataset.NewLinear()
			EvaluateSketch(t, n, linearGenerator, alpha)
		}
	}
}

func TestNormal(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			normalGenerator := dataset.NewNormal(35, 1)
			EvaluateSketch(t, n, normalGenerator, alpha)
		}
	}
}

func TestLognormal(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			lognormalGenerator := dataset.NewLognormal(0, -2)
			EvaluateSketch(t, n, lognormalGenerator, alpha)
		}
	}
}

func TestExponential(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			expGenerator := dataset.NewExponential(1.5)
			EvaluateSketch(t, n, expGenerator, alpha)
		}
	}
}

func TestMergeNormal(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			data := dataset.NewDataset()
			sketch1, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator1 := dataset.NewNormal(35, 1)
			for i := 0; i < n; i += 3 {
				value := generator1.Generate()
				sketch1.Add(value)
				data.Add(value)
			}
			sketch2, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator2 := dataset.NewNormal(-10, 2)
			for i := 1; i < n; i += 3 {
				value := generator2.Generate()
				sketch2.Add(value)
				data.Add(value)
			}
			sketch1.MergeWith(sketch2)

			sketch3, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator3 := dataset.NewNormal(40, 0.5)
			for i := 2; i < n; i += 3 {
				value := generator3.Generate()
				sketch3.Add(value)
				data.Add(value)
			}
			sketch1.MergeWith(sketch3)
			AssertSketchesAccurate(t, data, sketch1, alpha)
		}
	}
}

func TestMergeEmpty(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			data := dataset.NewDataset()
			// Merge a non-empty sketch to an empty sketch
			sketch1, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			sketch2, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator := dataset.NewExponential(5)
			for i := 0; i < n; i++ {
				value := generator.Generate()
				sketch2.Add(value)
				data.Add(value)
			}
			sketch1.MergeWith(sketch2)
			AssertSketchesAccurate(t, data, sketch1, alpha)

			// Merge an empty sketch to a non-empty sketch
			sketch3, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			sketch2.MergeWith(sketch3)
			AssertSketchesAccurate(t, data, sketch2, alpha)
			// Sketch3 should still be empty
			assert.True(t, sketch3.IsEmpty())
		}
	}
}

func TestMergeMixed(t *testing.T) {
	for _, alpha := range testAlphas {
		for _, n := range testSizes {
			data := dataset.NewDataset()
			sketch1, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator1 := dataset.NewNormal(100, 1)
			for i := 0; i < n; i += 3 {
				value := generator1.Generate()
				sketch1.Add(value)
				data.Add(value)
			}
			sketch2, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator2 := dataset.NewExponential(5)
			for i := 1; i < n; i += 3 {
				value := generator2.Generate()
				sketch2.Add(value)
				data.Add(value)
			}
			sketch1.MergeWith(sketch2)

			sketch3, _ := LogCollapsingLowestDenseDDSketch(alpha, testMaxBins)
			generator3 := dataset.NewExponential(0.1)
			for i := 2; i < n; i += 3 {
				value := generator3.Generate()
				sketch3.Add(value)
				data.Add(value)
			}
			sketch1.MergeWith(sketch3)

			AssertSketchesAccurate(t, data, sketch1, alpha)
		}
	}
}

// Test that successive Quantile() calls do not modify the sketch
func TestConsistentQuantile(t *testing.T) {
	var vals []float64
	var q float64
	nTests := 200
	testAlpha := 0.01
	vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	fuzzer := fuzz.New()
	for i := 0; i < nTests; i++ {
		sketch, _ := LogCollapsingLowestDenseDDSketch(testAlpha, testMaxBins)
		vfuzzer.Fuzz(&vals)
		fuzzer.Fuzz(&q)
		for _, v := range vals {
			sketch.Add(v)
		}
		q1, _ := sketch.GetValueAtQuantile(q)
		q2, _ := sketch.GetValueAtQuantile(q)
		assert.Equal(t, q1, q2)
	}
}

// Test that MergeWith() calls do not modify the argument sketch
func TestConsistentMerge(t *testing.T) {
	var vals []float64
	nTests := 10
	testAlpha := 0.01
	testSize := 1000
	fuzzer := fuzz.New().NilChance(0).NumElements(10, 1000)
	sketch1, _ := LogCollapsingLowestDenseDDSketch(testAlpha, testMaxBins)
	generator := dataset.NewNormal(50, 1)
	for i := 0; i < testSize; i++ {
		sketch1.Add(generator.Generate())
	}
	for i := 0; i < nTests; i++ {
		sketch2, _ := LogCollapsingLowestDenseDDSketch(testAlpha, testMaxBins)
		fuzzer.Fuzz(&vals)
		for _, v := range vals {
			sketch2.Add(v)
		}
		quantilesBeforeMerge, _ := sketch2.GetValuesAtQuantiles(testQuantiles)
		sketch1.MergeWith(sketch2)
		quantilesAfterMerge, _ := sketch2.GetValuesAtQuantiles(testQuantiles)
		assert.InDeltaSlice(t, quantilesBeforeMerge, quantilesAfterMerge, floatingPointAcceptableError)
	}
}
