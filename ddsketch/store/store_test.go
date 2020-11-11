// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"sort"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

var (
	testMaxNumBins = []int{8, 128, 1024}
)

func EvaluateValues(t *testing.T, store *DenseStore, values []int, collapsingLowest bool, collapsingHighest bool) {
	var count float64
	for _, b := range store.bins {
		count += b
	}
	assert.Equal(t, count, store.count)
	assert.Equal(t, count, float64(len(values)))
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if !collapsingLowest {
		minIndex, _ := store.MinIndex()
		assert.Equal(t, minIndex, values[0])
	}
	if !collapsingHighest {
		maxIndex, _ := store.MaxIndex()
		assert.Equal(t, maxIndex, values[len(values)-1])
	}
}

func EvaluateBins(t *testing.T, bins []Bin, values []int) {
	var binValues []int
	for _, b := range bins {
		for i := 0; i < int(b.Count()); i++ {
			binValues = append(binValues, b.Index())
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	assert.ElementsMatch(t, binValues, values)
}

func TestAdd(t *testing.T) {
	nTests := 100
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	// Test with int16 values so as to not run into memory issues.
	var values []int16
	var store *DenseStore
	for i := 0; i < nTests; i++ {
		store = NewDenseStore()
		f.Fuzz(&values)
		var valuesInt []int
		for _, v := range values {
			store.Add(int(v))
			valuesInt = append(valuesInt, int(v))
		}
		EvaluateValues(t, store, valuesInt, false, false)
	}
}

func TestBins(t *testing.T) {
	nTests := 100
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	// Test with int16 values so as to not run into memory issues.
	var values []int16
	var store *DenseStore
	for i := 0; i < nTests; i++ {
		store = NewDenseStore()
		f.Fuzz(&values)
		var valuesInt []int
		for _, v := range values {
			store.Add(int(v))
			valuesInt = append(valuesInt, int(v))
		}
		var bins []Bin
		for bin := range store.Bins() {
			bins = append(bins, bin)
		}
		EvaluateBins(t, bins, valuesInt)
	}
}

func TestMerge(t *testing.T) {
	nTests := 100
	// Test with int16 values so as to not run into memory issues.
	var values1, values2 []int16
	var store1, store2 *DenseStore
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		var merged []int
		f.Fuzz(&values1)
		store1 = NewDenseStore()
		for _, v := range values1 {
			store1.Add(int(v))
			merged = append(merged, int(v))
		}
		f.Fuzz(&values2)
		store2 = NewDenseStore()
		for _, v := range values2 {
			store2.Add(int(v))
			merged = append(merged, int(v))
		}
		store1.MergeWith(store2)
		EvaluateValues(t, store1, merged, false, false)
	}
}

func EvaluateCollapsingLowestStore(t *testing.T, store *CollapsingLowestDenseStore, values []int32) {
	var count float64
	for _, b := range store.bins {
		count += b
	}
	assert.Equal(t, count, store.count)
	assert.Equal(t, count, float64(len(values)))
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	maxIndex, _ := store.MaxIndex()
	assert.Equal(t, maxIndex, int(values[len(values)-1]))
	assert.GreaterOrEqual(t, store.maxNumBins, len(store.bins))
}

func EvaluateCollapsingHighestStore(t *testing.T, store *CollapsingHighestDenseStore, values []int32) {
	var count float64
	for _, b := range store.bins {
		count += b
	}
	assert.Equal(t, count, store.count)
	assert.Equal(t, count, float64(len(values)))
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	minIndex, _ := store.MinIndex()
	assert.Equal(t, minIndex, int(values[0]))
	assert.GreaterOrEqual(t, store.maxNumBins, len(store.bins))
}

func TestCollapsingLowestAdd(t *testing.T) {
	nTests := 100
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	// Store indices are limited to the int32 range
	var values []int32
	var store *CollapsingLowestDenseStore
	for i := 0; i < nTests; i++ {
		for _, maxNumBins := range testMaxNumBins {
			store = NewCollapsingLowestDenseStore(maxNumBins)
			f.Fuzz(&values)
			for _, v := range values {
				store.Add(int(v))
			}
			EvaluateCollapsingLowestStore(t, store, values)
		}
	}
}

func TestCollapsingHighestAdd(t *testing.T) {
	nTests := 100
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	// Store indices are limited to the int32 range
	var values []int32
	var store *CollapsingHighestDenseStore
	for i := 0; i < nTests; i++ {
		for _, maxNumBins := range testMaxNumBins {
			store = NewCollapsingHighestDenseStore(maxNumBins)
			f.Fuzz(&values)
			for _, v := range values {
				store.Add(int(v))
			}
			EvaluateCollapsingHighestStore(t, store, values)
		}
	}
}

func TestCollapsingLowest(t *testing.T) {
	var store *CollapsingLowestDenseStore
	for _, maxNumBins := range testMaxNumBins {
		store = NewCollapsingLowestDenseStore(maxNumBins)
		for i := 0; i < 2*maxNumBins; i++ {
			store.Add(i)
		}
		assert.Equal(t, len(store.bins), maxNumBins)
		minIndex, _ := store.MinIndex()
		assert.Equal(t, minIndex, maxNumBins)
		maxIndex, _ := store.MaxIndex()
		assert.Equal(t, maxIndex, 2*maxNumBins-1)
	}
}

func TestCollapsingHighest(t *testing.T) {
	var store *CollapsingHighestDenseStore
	for _, maxNumBins := range testMaxNumBins {
		store = NewCollapsingHighestDenseStore(maxNumBins)
		for i := 0; i < 2*maxNumBins; i++ {
			store.Add(i)
		}
		assert.Equal(t, len(store.bins), maxNumBins)
		minIndex, _ := store.MinIndex()
		assert.Equal(t, minIndex, 0)
		maxIndex, _ := store.MaxIndex()
		assert.Equal(t, maxIndex, maxNumBins-1)
	}
}

func EvaluateCollapsingBins(t *testing.T, bins []Bin, values []int32, lowest bool) {
	var binValues []int
	for _, b := range bins {
		for i := 0; i < int(b.Count()); i++ {
			binValues = append(binValues, b.Index())
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	assert.Equal(t, len(binValues), len(values))
	if lowest {
		assert.Equal(t, binValues[len(binValues)-1], int(values[len(values)-1]))
	} else {
		assert.Equal(t, binValues[0], int(values[0]))
	}
}

func TestCollapsingLowestBins(t *testing.T) {
	nTests := 100
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	// Store indices are limited to the int32 range
	var values []int32
	var store *CollapsingLowestDenseStore
	for i := 0; i < nTests; i++ {
		for _, maxNumBins := range testMaxNumBins {
			store = NewCollapsingLowestDenseStore(maxNumBins)
			f.Fuzz(&values)
			for _, v := range values {
				store.Add(int(v))
			}
			var bins []Bin
			for bin := range store.Bins() {
				bins = append(bins, bin)
			}
			EvaluateCollapsingBins(t, bins, values, true)
		}
	}
}

func TestCollapsingHighestBins(t *testing.T) {
	nTests := 100
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	// Store indices are limited to the int32 range
	var values []int32
	var store *CollapsingHighestDenseStore
	for i := 0; i < nTests; i++ {
		for _, maxNumBins := range testMaxNumBins {
			store = NewCollapsingHighestDenseStore(maxNumBins)
			f.Fuzz(&values)
			for _, v := range values {
				store.Add(int(v))
			}
			var bins []Bin
			for bin := range store.Bins() {
				bins = append(bins, bin)
			}
			EvaluateCollapsingBins(t, bins, values, false)
		}
	}
}

func TestCollapsingLowestMerge(t *testing.T) {
	nTests := 100
	// Store indices are limited to the int32 range
	var values1, values2 []int32
	var store1, store2 *CollapsingLowestDenseStore
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		for _, maxNumBins1 := range testMaxNumBins {
			for _, maxNumBins2 := range testMaxNumBins {
				f.Fuzz(&values1)
				store1 = NewCollapsingLowestDenseStore(maxNumBins1)
				for _, v := range values1 {
					store1.Add(int(v))
				}
				f.Fuzz(&values2)
				store2 = NewCollapsingLowestDenseStore(maxNumBins2)
				for _, v := range values2 {
					store2.Add(int(v))
				}
				store1.MergeWith(store2)
				EvaluateCollapsingLowestStore(t, store1, append(values1, values2...))
			}
		}
	}
}

func TestCollapsingHighestMerge(t *testing.T) {
	nTests := 100
	// Store indices are limited to the int32 range
	var values1, values2 []int32
	var store1, store2 *CollapsingHighestDenseStore
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		for _, maxNumBins1 := range testMaxNumBins {
			for _, maxNumBins2 := range testMaxNumBins {
				f.Fuzz(&values1)
				store1 = NewCollapsingHighestDenseStore(maxNumBins1)
				for _, v := range values1 {
					store1.Add(int(v))
				}
				f.Fuzz(&values2)
				store2 = NewCollapsingHighestDenseStore(maxNumBins2)
				for _, v := range values2 {
					store2.Add(int(v))
				}
				store1.MergeWith(store2)
				EvaluateCollapsingHighestStore(t, store1, append(values1, values2...))
			}
		}
	}
}

func TestMixedMerge1(t *testing.T) {
	nTests := 100
	// Test with int16 values so as to not run into memory issues.
	var values1, values2 []int16
	var store1 *CollapsingLowestDenseStore
	var store2 *DenseStore
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		for _, maxNumBins1 := range testMaxNumBins {
			f.Fuzz(&values1)
			store1 = NewCollapsingLowestDenseStore(maxNumBins1)
			var valuesInt []int
			for _, v := range values1 {
				store1.Add(int(v))
				valuesInt = append(valuesInt, int(v))
			}
			f.Fuzz(&values2)
			store2 = NewDenseStore()
			for _, v := range values2 {
				store2.Add(int(v))
				valuesInt = append(valuesInt, int(v))
			}
			if nTests/2 == 0 {
				// Merge DenseStore to CollapsingLowestDenseStore
				store1.MergeWith(store2)
				var valuesInt32 []int32
				for _, v := range valuesInt {
					valuesInt32 = append(valuesInt32, int32(v))
				}
				EvaluateCollapsingLowestStore(t, store1, valuesInt32)
			} else {
				// Merge CollapsingLowestDenseStore to DenseStore
				store2.MergeWith(store1)
				EvaluateValues(t, store2, valuesInt, true, false)
			}
		}
	}
}

func TestMixedMerge2(t *testing.T) {
	nTests := 100
	// Test with int16 values so as to not run into memory issues.
	var values1, values2 []int16
	var store1 *CollapsingHighestDenseStore
	var store2 *DenseStore
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		for _, maxNumBins1 := range testMaxNumBins {
			f.Fuzz(&values1)
			store1 = NewCollapsingHighestDenseStore(maxNumBins1)
			var valuesInt []int
			for _, v := range values1 {
				store1.Add(int(v))
				valuesInt = append(valuesInt, int(v))
			}
			f.Fuzz(&values2)
			store2 = NewDenseStore()
			for _, v := range values2 {
				store2.Add(int(v))
				valuesInt = append(valuesInt, int(v))
			}
			if nTests/2 == 0 {
				// Merge DenseStore to CollapsingHighestDenseStore
				store1.MergeWith(store2)
				var valuesInt32 []int32
				for _, v := range valuesInt {
					valuesInt32 = append(valuesInt32, int32(v))
				}
				EvaluateCollapsingHighestStore(t, store1, valuesInt32)
			} else {
				// Merge CollapsingHighestDenseStore to DenseStore
				store2.MergeWith(store1)
				EvaluateValues(t, store2, valuesInt, false, true)
			}
		}
	}
}

func TestSerialization(t *testing.T) {
	nTests := 100
	// Store indices are limited to the int32 range
	var values []int32
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		for _, maxNumBins := range testMaxNumBins {
			f.Fuzz(&values)
			storeLow := NewCollapsingLowestDenseStore(maxNumBins)
			storeHigh := NewCollapsingHighestDenseStore(maxNumBins)
			for _, v := range values {
				storeLow.Add(int(v))
				storeHigh.Add(int(v))
			}
			deserializedStoreLow := NewCollapsingLowestDenseStore(maxNumBins)
			deserializedStoreLow.FromProto(storeLow.ToProto())
			assert.Equal(t, storeLow, deserializedStoreLow)
			deserializedStoreHigh := NewCollapsingHighestDenseStore(maxNumBins)
			deserializedStoreHigh.FromProto(storeHigh.ToProto())
			assert.Equal(t, storeHigh, deserializedStoreHigh)
		}
	}
}
