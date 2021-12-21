// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package ddsketch

import (
	"github.com/DataDog/sketches-go/ddsketch/stat"
	"math"
	"math/rand"
	"testing"

	"github.com/DataDog/sketches-go/dataset"
	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/DataDog/sketches-go/ddsketch/store"

	"github.com/golang/protobuf/proto"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

const (
	epsilon                      = 1e-6 // Acceptable relative error for counts
	floatingPointAcceptableError = 1e-11
)

type testCase struct {
	sketch                 func() quantileSketch
	exactSummaryStatistics bool
	mergeWith              func(s1, s2 quantileSketch)
	decode                 func(b []byte) (quantileSketch, error)
}

var (
	testQuantiles = []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}
	testSizes     = []int{3, 5, 10, 100, 1000}
	testCases     = []testCase{
		{
			sketch: func() quantileSketch {
				s, _ := LogUnboundedDenseDDSketch(0.1)
				return s
			},
			exactSummaryStatistics: false,
			mergeWith: func(s1, s2 quantileSketch) {
				s1.(*DDSketch).MergeWith(s2.(*DDSketch))
			},
			decode: func(b []byte) (quantileSketch, error) {
				return DecodeDDSketch(b, store.DenseStoreConstructor, nil)
			},
		},
		{
			sketch: func() quantileSketch {
				s, _ := LogUnboundedDenseDDSketch(0.01)
				return s
			},
			exactSummaryStatistics: false,
			mergeWith: func(s1, s2 quantileSketch) {
				s1.(*DDSketch).MergeWith(s2.(*DDSketch))
			},
			decode: func(b []byte) (quantileSketch, error) {
				return DecodeDDSketch(b, store.DenseStoreConstructor, nil)
			},
		},
		{
			sketch: func() quantileSketch {
				s, _ := NewDefaultDDSketchWithExactSummaryStatistics(0.1)
				return s
			},
			exactSummaryStatistics: true,
			mergeWith: func(s1, s2 quantileSketch) {
				s1.(*DDSketchWithExactSummaryStatistics).MergeWith(s2.(*DDSketchWithExactSummaryStatistics))
			},
			decode: func(b []byte) (quantileSketch, error) {
				return DecodeDDSketchWithExactSummaryStatistics(b, store.DenseStoreConstructor, nil)
			},
		},
		{
			sketch: func() quantileSketch {
				s, _ := NewDefaultDDSketchWithExactSummaryStatistics(0.01)
				return s
			},
			exactSummaryStatistics: true,
			mergeWith: func(s1, s2 quantileSketch) {
				s1.(*DDSketchWithExactSummaryStatistics).MergeWith(s2.(*DDSketchWithExactSummaryStatistics))
			},
			decode: func(b []byte) (quantileSketch, error) {
				return DecodeDDSketchWithExactSummaryStatistics(b, store.DenseStoreConstructor, nil)
			},
		},
	}
)

func evaluateSketch(t *testing.T, n int, gen dataset.Generator, sketch quantileSketch, testCase testCase) {
	data := dataset.NewDataset()
	for i := 0; i < n; i++ {
		value := gen.Generate()
		sketch.Add(value)
		data.Add(value)
	}
	assertSketchesAccurate(t, data, sketch, testCase.exactSummaryStatistics)
	// Add negative numbers
	for i := 0; i < n; i++ {
		value := gen.Generate()
		sketch.Add(-value)
		data.Add(-value)
	}
	assertSketchesAccurate(t, data, sketch, testCase.exactSummaryStatistics)

	// for each store type, serialize / deserialize the sketch into a sketch with that store type, and check that new sketch is still accurate
	assertDeserializedSketchAccurate(t, sketch, store.DenseStoreConstructor, data, testCase)
	assertDeserializedSketchAccurate(t, sketch, store.BufferedPaginatedStoreConstructor, data, testCase)
	assertDeserializedSketchAccurate(t, sketch, store.SparseStoreConstructor, data, testCase)
}

// makes sure that if we serialize and deserialize a sketch, it will still be accurate
func assertDeserializedSketchAccurate(t *testing.T, sketch quantileSketch, storeProvider store.Provider, data *dataset.Dataset, testCase testCase) {
	encoded := &[]byte{}
	sketch.Encode(encoded, false)
	decoded, err := testCase.decode(*encoded)
	assert.Nil(t, err)
	assertSketchesAccurate(t, data, decoded, testCase.exactSummaryStatistics)

	s, ok := sketch.(*DDSketch)
	if !ok {
		return
	}
	serialized, err := proto.Marshal(s.ToProto())
	assert.Nil(t, err)
	var sketchPb sketchpb.DDSketch
	err = proto.Unmarshal(serialized, &sketchPb)
	assert.Nil(t, err)
	deserializedSketch, err := FromProtoWithStoreProvider(&sketchPb, storeProvider)
	assert.Nil(t, err)
	assertSketchesAccurate(t, data, deserializedSketch, false)
}

func assertSketchesAccurate(t *testing.T, data *dataset.Dataset, sketch quantileSketch, exactSummaryStatistics bool) {
	alpha := sketch.RelativeAccuracy()
	assert := assert.New(t)
	assert.Equal(data.Count, sketch.GetCount())
	if data.Count == 0 {
		assert.True(sketch.IsEmpty())
		_, minErr := sketch.GetMinValue()
		_, maxErr := sketch.GetMaxValue()
		_, quantileErr := sketch.GetValueAtQuantile(0.5)
		_, quantilesErr := sketch.GetValuesAtQuantiles([]float64{0.1, 0.9})
		assert.NotNil(minErr)
		assert.NotNil(maxErr)
		assert.NotNil(quantileErr)
		assert.NotNil(quantilesErr)
	} else {
		minValue, minErr := sketch.GetMinValue()
		maxValue, maxErr := sketch.GetMaxValue()
		assert.Nil(minErr)
		assert.Nil(maxErr)
		expectedMinValue := data.Min()
		expectedMaxValue := data.Max()
		if exactSummaryStatistics {
			assert.Equal(expectedMinValue, minValue)
			assert.Equal(expectedMaxValue, maxValue)
			assert.InDelta(data.Sum(), sketch.GetSum(), floatingPointAcceptableError)
		} else {
			assertRelativelyAccurate(assert, alpha, expectedMinValue, expectedMinValue, minValue)
			assertRelativelyAccurate(assert, alpha, expectedMaxValue, expectedMaxValue, maxValue)
		}
		for _, q := range testQuantiles {
			lowerQuantile := data.LowerQuantile(q)
			upperQuantile := data.UpperQuantile(q)
			quantile, quantileErr := sketch.GetValueAtQuantile(q)
			assert.Nil(quantileErr)
			assertRelativelyAccurate(assert, alpha, lowerQuantile, upperQuantile, quantile)
			assert.LessOrEqual(minValue, quantile)
			assert.GreaterOrEqual(maxValue, quantile)
			quantiles, quantilesErr := sketch.GetValuesAtQuantiles([]float64{q, q})
			assert.Nil(quantilesErr)
			assert.Len(quantiles, 2)
			assert.Equal(quantile, quantiles[0])
			assert.Equal(quantile, quantiles[1])
		}
	}
}

func assertRelativelyAccurate(a *assert.Assertions, relativeAccuracy, expectedLowerBound, expectedUpperBound, actual float64) {
	minExpectedValue := math.Min(expectedLowerBound*(1-relativeAccuracy), expectedLowerBound*(1+relativeAccuracy))
	maxExpectedValue := math.Max(expectedUpperBound*(1-relativeAccuracy), expectedUpperBound*(1+relativeAccuracy))
	a.LessOrEqual(minExpectedValue-floatingPointAcceptableError, actual)
	a.GreaterOrEqual(maxExpectedValue+floatingPointAcceptableError, actual)
}

func TestConstant(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			constantGenerator := dataset.NewConstant(float64(rand.Int()))
			evaluateSketch(t, n, constantGenerator, testCase.sketch(), testCase)
		}
	}
}

func TestLinear(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			linearGenerator := dataset.NewLinear()
			evaluateSketch(t, n, linearGenerator, testCase.sketch(), testCase)
		}
	}
}

func TestNormal(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			normalGenerator := dataset.NewNormal(35, 1)
			evaluateSketch(t, n, normalGenerator, testCase.sketch(), testCase)
		}
	}
}

func TestLognormal(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			lognormalGenerator := dataset.NewLognormal(0, -2)
			evaluateSketch(t, n, lognormalGenerator, testCase.sketch(), testCase)
		}
	}
}

func TestExponential(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			expGenerator := dataset.NewExponential(1.5)
			evaluateSketch(t, n, expGenerator, testCase.sketch(), testCase)
		}
	}
}

func TestMergeNormal(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			data := dataset.NewDataset()
			sketch1 := testCase.sketch()
			generator1 := dataset.NewNormal(35, 1)
			for i := 0; i < n; i += 3 {
				value := generator1.Generate()
				sketch1.Add(value)
				data.Add(value)
			}
			sketch2 := testCase.sketch()
			generator2 := dataset.NewNormal(-10, 2)
			for i := 1; i < n; i += 3 {
				value := generator2.Generate()
				sketch2.Add(value)
				data.Add(value)
			}
			testCase.mergeWith(sketch1, sketch2)

			sketch3 := testCase.sketch()
			generator3 := dataset.NewNormal(40, 0.5)
			for i := 2; i < n; i += 3 {
				value := generator3.Generate()
				sketch3.Add(value)
				data.Add(value)
			}
			testCase.mergeWith(sketch1, sketch3)
			assertSketchesAccurate(t, data, sketch1, testCase.exactSummaryStatistics)
		}
	}
}

func TestMergeEmpty(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			data := dataset.NewDataset()
			// Merge a non-empty sketch to an empty sketch
			sketch1 := testCase.sketch()
			sketch2 := testCase.sketch()
			generator := dataset.NewExponential(5)
			for i := 0; i < n; i++ {
				value := generator.Generate()
				sketch2.Add(value)
				data.Add(value)
			}
			testCase.mergeWith(sketch1, sketch2)
			assertSketchesAccurate(t, data, sketch1, testCase.exactSummaryStatistics)

			// Merge an empty sketch to a non-empty sketch
			sketch3 := testCase.sketch()
			testCase.mergeWith(sketch2, sketch3)
			assertSketchesAccurate(t, data, sketch2, testCase.exactSummaryStatistics)
			// Sketch3 should still be empty
			assert.True(t, sketch3.IsEmpty())
		}
	}
}

func TestMergeMixed(t *testing.T) {
	for _, testCase := range testCases {
		for _, n := range testSizes {
			data := dataset.NewDataset()
			sketch1 := testCase.sketch()
			generator1 := dataset.NewNormal(100, 1)
			for i := 0; i < n; i += 3 {
				value := generator1.Generate()
				sketch1.Add(value)
				data.Add(value)
			}
			sketch2 := testCase.sketch()
			generator2 := dataset.NewExponential(5)
			for i := 1; i < n; i += 3 {
				value := generator2.Generate()
				sketch2.Add(value)
				data.Add(value)
			}
			testCase.mergeWith(sketch1, sketch2)

			sketch3 := testCase.sketch()
			generator3 := dataset.NewExponential(0.1)
			for i := 2; i < n; i += 3 {
				value := generator3.Generate()
				sketch3.Add(value)
				data.Add(value)
			}
			testCase.mergeWith(sketch1, sketch3)

			assertSketchesAccurate(t, data, sketch1, testCase.exactSummaryStatistics)
		}
	}
}

// Test that successive Quantile() calls do not modify the sketch
func TestConsistentQuantile(t *testing.T) {
	for _, testCase := range testCases {
		var vals []float64
		var q float64
		nTests := 200
		vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
		fuzzer := fuzz.New()
		for i := 0; i < nTests; i++ {
			sketch := testCase.sketch()
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
}

// Test that MergeWith() calls do not modify the argument sketch
func TestConsistentMerge(t *testing.T) {
	for _, testCase := range testCases {
		var vals []float64
		nTests := 10
		testSize := 1000
		fuzzer := fuzz.New().NilChance(0).NumElements(10, 1000)
		sketch1 := testCase.sketch()
		generator := dataset.NewNormal(50, 1)
		for i := 0; i < testSize; i++ {
			sketch1.Add(generator.Generate())
		}
		for i := 0; i < nTests; i++ {
			sketch2 := testCase.sketch()
			fuzzer.Fuzz(&vals)
			for _, v := range vals {
				sketch2.Add(v)
			}
			quantilesBeforeMerge, _ := sketch2.GetValuesAtQuantiles(testQuantiles)
			testCase.mergeWith(sketch1, sketch2)
			quantilesAfterMerge, _ := sketch2.GetValuesAtQuantiles(testQuantiles)
			assert.InDeltaSlice(t, quantilesBeforeMerge, quantilesAfterMerge, floatingPointAcceptableError)
		}
	}
}
func TestCopy(t *testing.T) {
	sketch, _ := LogUnboundedDenseDDSketch(0.01)
	sketch.AddWithCount(0, 1.2)
	sketch.Add(3.4)
	sketch.AddWithCount(-5.6, 7.8)
	copy := sketch.Copy()
	assert.Equal(t, sketch.GetCount(), copy.GetCount())
}

// TestChangeMapping tests the change of mapping of a DDSketch.
func TestChangeMapping(t *testing.T) {
	sketch, _ := LogCollapsingLowestDenseDDSketch(0.01, 2000)
	generator := dataset.NewNormal(50, 1)
	testSize := 1000
	scaleFactor := 0.1
	for i := 0; i < testSize; i++ {
		sketch.Add(generator.Generate())
	}
	expectedQuantiles, _ := sketch.GetValuesAtQuantiles(testQuantiles)
	newMapping, _ := mapping.NewLogarithmicMapping(0.007)
	converted := sketch.ChangeMapping(newMapping, store.NewDenseStore(), store.NewDenseStore(), scaleFactor)
	quantiles, _ := converted.GetValuesAtQuantiles(testQuantiles)
	for i, q := range quantiles {
		e := expectedQuantiles[i] * scaleFactor
		assert.InDelta(t, e, q, floatingPointAcceptableError+e*(0.01+0.007))
	}
}

// TestReweight tests the reweighting of a sketch by a constant.
func TestReweight(t *testing.T) {
	m, _ := mapping.NewLogarithmicMapping(0.01)
	sketches := []*DDSketch{
		NewDDSketch(m, store.NewDenseStore(), store.NewDenseStore()),
		NewDDSketch(m, store.NewSparseStore(), store.NewSparseStore()),
		NewDDSketch(m, store.NewBufferedPaginatedStore(), store.NewBufferedPaginatedStore()),
	}
	testSize := 1000
	for _, s := range sketches {
		generator := dataset.NewNormal(50, 1)
		for i := 0; i < testSize; i++ {
			s.Add(generator.Generate())
		}
		expectedQuantiles, _ := s.GetValuesAtQuantiles(testQuantiles)
		assert.Nil(t, s.Reweight(3))
		// no matter the weight constant, the quantiles should stay the same.
		quantiles, _ := s.GetValuesAtQuantiles(testQuantiles)
		for i, q := range quantiles {
			e := expectedQuantiles[i]
			assert.InDelta(t, e, q, floatingPointAcceptableError+e*0.01)
		}
		assert.InDelta(t, float64(3*testSize), s.GetCount(), floatingPointAcceptableError)
	}
}

func TestClear(t *testing.T) {
	sketch, _ := LogUnboundedDenseDDSketch(0.01)
	sketch.AddWithCount(0, 1.2)
	sketch.Add(3.4)
	sketch.AddWithCount(-5.6, 7.8)
	sketch.Clear()
	assert.Zero(t, sketch.GetCount())
}

func TestForEach(t *testing.T) {
	{ // Empty.
		sketch, _ := LogUnboundedDenseDDSketch(0.01)
		sketch.ForEach(func(value, count float64) (stop bool) {
			assert.Fail(t, "empty sketch should have no bin")
			return false
		})
	}
	for i := 0; i < 3; i++ { // Stopping condition.
		sketch, _ := LogUnboundedDenseDDSketch(0.01)
		sketch.Add(0)
		sketch.Add(1)
		sketch.Add(-1)
		j := 0
		sketch.ForEach(func(value, count float64) (stop bool) {
			assert.LessOrEqual(t, j, i)
			j++
			return j > i
		})
		assert.Equal(t, i, j-1)
	}
}

func TestErrors(t *testing.T) {
	sketch, _ := LogUnboundedDenseDDSketch(0.01)
	assert.Equal(t, ErrUntrackableTooLow, sketch.Add(math.Inf(-1)))
	assert.Equal(t, ErrUntrackableTooHigh, sketch.Add(math.Inf(1)))
	assert.Equal(t, ErrUntrackableNaN, sketch.Add(math.NaN()))
	assert.Equal(t, ErrNegativeCount, sketch.AddWithCount(1, -1))
}

func TestDecodingErrors(t *testing.T) {
	mapping1, _ := mapping.NewCubicallyInterpolatedMappingWithGamma(1.02, 0)
	mapping2, _ := mapping.NewCubicallyInterpolatedMappingWithGamma(1.04, 0)
	storeProvider := store.BufferedPaginatedStoreConstructor
	{
		decoded, err := DecodeDDSketch([]byte{}, storeProvider, mapping1)
		assert.Nil(t, err)
		assert.True(t, decoded.IsEmpty())
	}
	{
		_, err := DecodeDDSketch([]byte{}, storeProvider, nil)
		assert.Error(t, err)
	}
	{
		encoded := &[]byte{}
		mapping2.Encode(encoded)
		_, err := DecodeDDSketch(*encoded, storeProvider, nil)
		assert.Nil(t, err)
	}
	{
		sketch := NewDDSketchFromStoreProvider(mapping1, storeProvider)
		encoded := &[]byte{}
		err := sketch.DecodeAndMergeWith(*encoded)
		assert.Nil(t, err)
	}
	{
		sketch := NewDDSketchFromStoreProvider(mapping1, storeProvider)
		encoded := &[]byte{}
		mapping2.Encode(encoded)
		err := sketch.DecodeAndMergeWith(*encoded)
		assert.Error(t, err)
	}
	{ // with exact summary statistics -> without exact summary statistics (valid)
		sketch := NewDDSketchWithExactSummaryStatistics(mapping1, storeProvider)
		sketch.Add(0)
		encoded := &[]byte{}
		sketch.Encode(encoded, false)
		decoded, err := DecodeDDSketchWithExactSummaryStatistics(*encoded, storeProvider, nil)
		assert.Nil(t, err)
		assert.Equal(t, 1.0, decoded.GetCount())
	}
	{ // without exact summary statistics -> with exact summary statistics (error)
		sketch := NewDDSketchFromStoreProvider(mapping1, storeProvider)
		sketch.Add(0)
		encoded := &[]byte{}
		sketch.Encode(encoded, false)
		_, err := DecodeDDSketchWithExactSummaryStatistics(*encoded, storeProvider, nil)
		assert.NotNil(t, err)
	}
}

func TestFromData(t *testing.T) {
	{
		emptySketch, _ := NewDefaultDDSketch(1e-2)
		emptySummaryStatistics := stat.NewSummaryStatistics()
		_, err := NewDDSketchWithExactSummaryStatisticsFromData(emptySketch, emptySummaryStatistics)
		assert.NoError(t, err)
	}
	{
		sketch, _ := NewDefaultDDSketch(1e-2)
		summaryStatistics := stat.NewSummaryStatistics()
		_ = sketch.AddWithCount(1.2, 1.0)
		summaryStatistics.Add(1.2, 1.0)
		_, err := NewDDSketchWithExactSummaryStatisticsFromData(sketch, summaryStatistics)
		assert.NoError(t, err)
	}
	{
		emptySketch, _ := NewDefaultDDSketch(1e-2)
		summaryStatistics := stat.NewSummaryStatistics()
		summaryStatistics.Add(1.2, 1.0)
		_, err := NewDDSketchWithExactSummaryStatisticsFromData(emptySketch, summaryStatistics)
		assert.Error(t, err)
	}
	{
		sketch, _ := NewDefaultDDSketch(1e-2)
		emptySummaryStatistics := stat.NewSummaryStatistics()
		_ = sketch.AddWithCount(1.2, 1.0)
		_, err := NewDDSketchWithExactSummaryStatisticsFromData(sketch, emptySummaryStatistics)
		assert.Error(t, err)
	}
}

type sketchDataTestCase struct {
	name          string
	indexMapping  mapping.IndexMapping
	storeProvider store.Provider
	fillSketch    func(sketch DDSketch)
}

var (
	indexMapping, _ = mapping.NewCubicallyInterpolatedMappingWithGamma(1.02, 0)
	dataTestCases   = []sketchDataTestCase{
		{
			name:          "dense/empty",
			indexMapping:  indexMapping,
			storeProvider: store.DenseStoreConstructor,
			fillSketch:    func(sketch DDSketch) {},
		},
		{
			name:          "dense/small_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.DenseStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.Add(0)
				sketch.Add(2)
				sketch.Add(28)
				sketch.Add(-3)
			},
		},
		{
			name:          "dense/small_non_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.DenseStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.AddWithCount(0, 0.1)
				sketch.AddWithCount(2, 1.2)
				sketch.AddWithCount(28, 8.66)
				sketch.AddWithCount(-3, 2.33)
			},
		},
		{
			name:          "dense/small_far_apart",
			indexMapping:  indexMapping,
			storeProvider: store.DenseStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.AddWithCount(1, 0.1)
				sketch.AddWithCount(1e20, 1.2)
			},
		},
		{
			name:          "collapsing_lowest_dense/log_normal_non_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.Provider(func() store.Store { return store.NewCollapsingLowestDenseStore(2048) }),
			fillSketch: func(sketch DDSketch) {
				gen := dataset.NewLognormal(0, 2)
				for i := 0; i < int(1e5); i++ {
					sketch.AddWithCount(gen.Generate(), 0.1)
				}
			},
		},
		{
			name:          "sparse/single_value",
			indexMapping:  indexMapping,
			storeProvider: store.SparseStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.Add(34654677.3676)
			},
		},
		{
			name:          "sparse/small_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.SparseStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.Add(0)
				sketch.Add(2)
				sketch.Add(28)
				sketch.Add(-3)
			},
		},
		{
			name:          "sparse/log_normal_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.SparseStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				gen := dataset.NewLognormal(0, 2)
				for i := 0; i < int(1e5); i++ {
					sketch.Add(gen.Generate())
				}
			},
		},
		{
			name:          "buffered_paginated/empty",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
			},
		},
		{
			name:          "buffered_paginated/single_value",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.Add(34654677.3676)
			},
		},
		{
			name:          "buffered_paginated/small_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.Add(0)
				sketch.Add(2)
				sketch.Add(28)
				sketch.Add(-3)
			},
		},
		{
			name:          "buffered_paginated/small_non_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				sketch.AddWithCount(0, 0.1)
				sketch.AddWithCount(2, 1.2)
				sketch.AddWithCount(28, 86676635552.8783786)
				sketch.AddWithCount(-3, 2.33)
			},
		},
		{
			name:          "buffered_paginated/int_count_linear",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				gen := dataset.NewLinear()
				for i := 0; i < int(1e5); i++ {
					sketch.Add(gen.Generate())
				}
			},
		},
		{
			name:          "buffered_paginated/log_normal_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				gen := dataset.NewLognormal(0, 2)
				for i := 0; i < int(1e5); i++ {
					sketch.Add(gen.Generate())
				}
			},
		},
		{
			name:          "buffered_paginated/log_normal_non_int_count",
			indexMapping:  indexMapping,
			storeProvider: store.BufferedPaginatedStoreConstructor,
			fillSketch: func(sketch DDSketch) {
				gen := dataset.NewLognormal(0, 2)
				for i := 0; i < int(1e5); i++ {
					sketch.AddWithCount(gen.Generate(), 0.1)
				}
			},
		},
	}
)

func TestBenchmarkEncodedSize(t *testing.T) {
	t.Logf("%-45s %6s %6s %17s\n", "test case", "proto", "custom", "custom_no_mapping")
	for _, testCase := range dataTestCases {
		sketch := NewDDSketchFromStoreProvider(testCase.indexMapping, testCase.storeProvider)
		testCase.fillSketch(*sketch)
		encoded := make([]byte, 0)
		sketch.Encode(&encoded, false)
		encodedWithoutIndexMapping := make([]byte, 0)
		sketch.Encode(&encodedWithoutIndexMapping, true)
		protoSerialized, _ := proto.Marshal(sketch.ToProto())
		t.Logf("%-45s %6d %6d %17d\n", testCase.name, len(protoSerialized), len(encoded), len(encodedWithoutIndexMapping))
	}
}

type serTestCase struct {
	name  string
	ser   func(s *DDSketch, b *[]byte)
	deser func(b []byte, s *DDSketch, p store.Provider)
}

var serTestCases []serTestCase = []serTestCase{
	{
		name: "proto",
		ser: func(s *DDSketch, b *[]byte) {
			serialized, _ := proto.Marshal(s.ToProto())
			*b = serialized
		},
		deser: func(b []byte, s *DDSketch, p store.Provider) {
			var sketchPb sketchpb.DDSketch
			proto.Unmarshal(b, &sketchPb)

			serialized, _ := FromProtoWithStoreProvider(&sketchPb, p)
			*s = *serialized
		},
	},
	{
		name: "custom",
		ser: func(s *DDSketch, b *[]byte) {
			*b = []byte{}
			s.Encode(b, false)
		},
		deser: func(b []byte, s *DDSketch, p store.Provider) {
			sketch, _ := DecodeDDSketch(b, p, nil)
			*s = *sketch
		},
	},
	{
		name: "custom_reusing",
		ser: func(s *DDSketch, b *[]byte) {
			*b = (*b)[:0]
			s.Encode(b, false)
		},
		deser: func(b []byte, s *DDSketch, p store.Provider) {
			s.Clear()
			s.DecodeAndMergeWith(b)
		},
	},
}

func TestSerDeser(t *testing.T) {
	storeProviders := []store.Provider{
		store.BufferedPaginatedStoreConstructor,
		store.DenseStoreConstructor,
		store.SparseStoreConstructor,
	}
	for _, testCase := range dataTestCases {
		sketch := NewDDSketchFromStoreProvider(testCase.indexMapping, testCase.storeProvider)
		testCase.fillSketch(*sketch)
		for _, serTestCase := range serTestCases {
			var serialized []byte
			serTestCase.ser(sketch, &serialized)
			for _, storeProvider := range storeProviders {
				deserialized := NewDDSketchFromStoreProvider(sketch.IndexMapping, storeProvider)
				serTestCase.deser(serialized, deserialized, storeProvider)
				assertSketchesEquivalent(t, sketch, deserialized)
			}
		}
	}
}

func assertSketchesEquivalent(t *testing.T, s1 *DDSketch, s2 *DDSketch) {
	assert.Equal(t, s1.IsEmpty(), s2.IsEmpty())
	if s1.IsEmpty() {
		assert.Equal(t, s1.GetCount(), s2.GetCount())
	} else {
		assert.InEpsilon(t, s1.GetCount(), s2.GetCount(), floatingPointAcceptableError)
		{
			m1, err1 := s1.GetMinValue()
			m2, err2 := s2.GetMinValue()
			assert.Nil(t, err1)
			assert.Nil(t, err2)
			assert.Equal(t, m1, m2)
		}
		{
			m1, err1 := s1.GetMaxValue()
			m2, err2 := s2.GetMaxValue()
			assert.Nil(t, err1)
			assert.Nil(t, err2)
			assert.Equal(t, m1, m2)
		}
		for _, q := range testQuantiles {
			v1l, err1l := s1.GetValueAtQuantile(clamp(q - epsilon))
			v1u, err1u := s1.GetValueAtQuantile(clamp(q + epsilon))
			v2l, err2l := s2.GetValueAtQuantile(clamp(q - epsilon))
			v2u, err2u := s2.GetValueAtQuantile(clamp(q + epsilon))
			assert.Nil(t, err1l)
			assert.Nil(t, err1u)
			assert.Nil(t, err2l)
			assert.Nil(t, err2u)
			assert.True(t, v1l <= v2u || v1u >= v2l)
		}
	}
}

func clamp(q float64) float64 {
	if q < 0 {
		return 0
	} else if q > 1 {
		return 1
	} else {
		return q
	}
}

var (
	sinkBytes  []byte
	sinkSketch *DDSketch
)

func BenchmarkAdd(b *testing.B) {
	relativeAccuracy := 1e-2
	mappings := make(map[string]mapping.IndexMapping)
	mappings["logarithmic"], _ = mapping.NewLogarithmicMapping(relativeAccuracy)
	mappings["cubic"], _ = mapping.NewCubicallyInterpolatedMapping(relativeAccuracy)
	mappings["linear"], _ = mapping.NewLinearlyInterpolatedMapping(relativeAccuracy)
	storeProvider := store.Provider(func() store.Store { return store.NewCollapsingLowestDenseStore(2048) })
	for name, mapping := range mappings {
		b.Run(name, func(b *testing.B) {
			sinkSketch = NewDDSketchFromStoreProvider(mapping, storeProvider)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sinkSketch.Add(rand.ExpFloat64())
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	for _, testCase := range dataTestCases {
		b.Run(testCase.name, func(b *testing.B) {
			sketch := NewDDSketchFromStoreProvider(testCase.indexMapping, testCase.storeProvider)
			testCase.fillSketch(*sketch)
			for _, sTestCase := range serTestCases {
				b.Run(sTestCase.name, func(b *testing.B) {
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						sTestCase.ser(sketch, &sinkBytes)
					}
				})
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	for _, testCase := range dataTestCases {
		b.Run(testCase.name, func(b *testing.B) {
			sketch := NewDDSketchFromStoreProvider(testCase.indexMapping, testCase.storeProvider)
			testCase.fillSketch(*sketch)
			for _, sTestCase := range serTestCases {
				var encoded []byte
				sTestCase.ser(sketch, &encoded)
				b.Run(sTestCase.name, func(b *testing.B) {
					sinkSketch = NewDDSketchFromStoreProvider(testCase.indexMapping, testCase.storeProvider)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						sTestCase.deser(encoded, sinkSketch, testCase.storeProvider)
					}
				})
			}
		})
	}
}
