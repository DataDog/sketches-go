// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

const epsilon float64 = 1e-14

type TestCase struct {
	name          string
	newStore      func() Store
	transformBins func([]Bin) []Bin
}

var (
	testMaxNumBins = []int{8, 128, 1024}
	testCases      = []TestCase{
		{name: "dense", newStore: func() Store { return NewDenseStore() }, transformBins: identity},
		{name: "collapsing_lowest_8", newStore: func() Store { return NewCollapsingLowestDenseStore(8) }, transformBins: collapsingLowest(8)},
		{name: "collapsing_lowest_128", newStore: func() Store { return NewCollapsingLowestDenseStore(128) }, transformBins: collapsingLowest(128)},
		{name: "collapsing_lowest_1024", newStore: func() Store { return NewCollapsingLowestDenseStore(1024) }, transformBins: collapsingLowest(1024)},
		{name: "collapsing_highest_8", newStore: func() Store { return NewCollapsingHighestDenseStore(8) }, transformBins: collapsingHighest(8)},
		{name: "collapsing_highest_128", newStore: func() Store { return NewCollapsingHighestDenseStore(128) }, transformBins: collapsingHighest(128)},
		{name: "collapsing_highest_1024", newStore: func() Store { return NewCollapsingHighestDenseStore(1024) }, transformBins: collapsingHighest(1024)},
		{name: "sparse", newStore: func() Store { return NewSparseStore() }, transformBins: identity},
		{name: "buffered_paginated", newStore: func() Store { return NewBufferedPaginatedStore() }, transformBins: identity},
	}
)

func identity(bins []Bin) []Bin {
	return bins
}

func collapsingLowest(maxNumBins int) func(bins []Bin) []Bin {
	return func(bins []Bin) []Bin {
		maxIndex := minInt
		for _, bin := range bins {
			maxIndex = max(maxIndex, bin.index)
		}
		if maxIndex < minInt+maxNumBins {
			return bins
		}
		minCollapsedIndex := maxIndex - maxNumBins + 1
		collapsedBins := make([]Bin, 0, len(bins))
		for _, bin := range bins {
			collapsedBins = append(collapsedBins, Bin{index: max(bin.index, minCollapsedIndex), count: bin.count})
		}
		return collapsedBins
	}
}

func collapsingHighest(maxNumBins int) func(bins []Bin) []Bin {
	return func(bins []Bin) []Bin {
		minIndex := maxInt
		for _, bin := range bins {
			minIndex = min(minIndex, bin.index)
		}
		if minIndex > maxInt-maxNumBins {
			return bins
		}
		maxCollapsedIndex := minIndex + maxNumBins - 1
		collapsedBins := make([]Bin, 0, len(bins))
		for _, bin := range bins {
			collapsedBins = append(collapsedBins, Bin{index: min(bin.index, maxCollapsedIndex), count: bin.count})
		}
		return collapsedBins
	}
}

// For fuzzy tests.
const seed int64 = 5388928120325255124

func randomIndex(random *rand.Rand) int {
	from := -1000
	to := 1000
	return random.Intn(to-from) - from
}

func randomCount(random *rand.Rand) float64 {
	max := float64(10)
	for {
		count := max * random.Float64()
		if count >= 10*epsilon {
			return count
		}
	}
}

// Generic tests

func TestEmpty(t *testing.T) {
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assertEncodeBins(t, testCase.newStore(), nil)
		})
	}
}

func TestAddIntDatasets(t *testing.T) {
	datasets := [][]int{
		{-1000},
		{-1},
		{0},
		{1},
		{1000},
		{1000, 1000},
		{1000, -1000},
		{-1000, 1000},
		{-1000, -1000},
		{0, 0, 0, 0},
	}
	counts := []float64{0.1, 1, 100}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, dataset := range datasets {
				{
					bins := make([]Bin, 0, len(dataset))
					storeAdd := testCase.newStore()
					for _, index := range dataset {
						bin := Bin{index: index, count: 1}
						bins = append(bins, bin)
						storeAdd.Add(index)
					}
					normalizedBins := normalize(testCase.transformBins(bins))
					assertEncodeBins(t, storeAdd, normalizedBins)
				}
				for _, count := range counts {
					bins := make([]Bin, 0, len(dataset))
					storeAddBin := testCase.newStore()
					storeAddWithCount := testCase.newStore()
					for _, index := range dataset {
						bin := Bin{index: index, count: count}
						bins = append(bins, bin)
						storeAddBin.AddBin(bin)
						storeAddWithCount.AddWithCount(index, count)
					}
					normalizedBins := normalize(testCase.transformBins(bins))
					assertEncodeBins(t, storeAddBin, normalizedBins)
					assertEncodeBins(t, storeAddWithCount, normalizedBins)

				}
			}
		})
	}
}

func TestAddConstant(t *testing.T) {
	indexes := []int{-1000, -1, 0, 1, 1000}
	counts := []int{0, 1, 2, 4, 5, 10, 20, 100, 1000, 10000}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, index := range indexes {
				for _, count := range counts {
					storeAdd := testCase.newStore()
					storeAddBin := testCase.newStore()
					storeAddWithCount := testCase.newStore()
					for j := 0; j < count; j++ {
						storeAdd.Add(index)
						storeAddBin.AddBin(Bin{index: index, count: 1})
						storeAddWithCount.AddWithCount(index, 1)
					}
					bins := []Bin{{index: index, count: float64(count)}}
					normalizedBins := normalize(testCase.transformBins(bins))
					assertEncodeBins(t, storeAdd, normalizedBins)
					assertEncodeBins(t, storeAddBin, normalizedBins)
					assertEncodeBins(t, storeAddWithCount, normalizedBins)
				}
			}
		})
	}
}

func TestAddMonotonous(t *testing.T) {
	increments := []int{2, 10, 100, -2, -10, -100}
	spreads := []int{2, 10, 10000}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, increment := range increments {
				for _, spread := range spreads {
					bins := make([]Bin, 0)
					storeAdd := testCase.newStore()
					storeAddBin := testCase.newStore()
					storeAddWithCount := testCase.newStore()
					for index := 0; math.Abs(float64(index)) <= float64(spread); index += increment {
						bin := Bin{index: index, count: 1}
						bins = append(bins, bin)
						storeAdd.Add(index)
						storeAddBin.AddBin(bin)
						storeAddWithCount.AddWithCount(index, 1)
					}
					normalizedBins := normalize(testCase.transformBins(bins))
					assertEncodeBins(t, storeAdd, normalizedBins)
					assertEncodeBins(t, storeAddBin, normalizedBins)
					assertEncodeBins(t, storeAddWithCount, normalizedBins)
				}
			}
		})
	}
}

func TestAddFuzzy(t *testing.T) {
	numTests := 100
	maxNumValues := 10000

	random := rand.New(rand.NewSource(seed))

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 0; i < numTests; i++ {
				bins := make([]Bin, 0)
				storeAddBin := testCase.newStore()
				storeAddWithCount := testCase.newStore()
				numValues := random.Intn(maxNumValues)
				for j := 0; j < numValues; j++ {
					bin := Bin{index: randomIndex(random), count: randomCount(random)}
					bins = append(bins, bin)
					storeAddBin.AddBin(bin)
					storeAddWithCount.AddWithCount(bin.index, bin.count)
				}
				normalizedBins := normalize(testCase.transformBins(bins))
				assertEncodeBins(t, storeAddBin, normalizedBins)
				assertEncodeBins(t, storeAddWithCount, normalizedBins)
			}
		})
	}
}

func TestAddIntFuzzy(t *testing.T) {
	numTests := 100
	maxNumValues := 10000

	random := rand.New(rand.NewSource(seed))

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 0; i < numTests; i++ {
				bins := make([]Bin, 0)
				storeAdd := testCase.newStore()
				storeAddBin := testCase.newStore()
				storeAddWithCount := testCase.newStore()
				numValues := random.Intn(maxNumValues)
				for j := 0; j < numValues; j++ {
					bin := Bin{index: randomIndex(random), count: 1}
					bins = append(bins, bin)
					storeAdd.Add(bin.index)
					storeAddBin.AddBin(bin)
					storeAddWithCount.AddWithCount(bin.index, bin.count)
				}
				normalizedBins := normalize(testCase.transformBins(bins))
				assertEncodeBins(t, storeAdd, normalizedBins)
				assertEncodeBins(t, storeAddBin, normalizedBins)
				assertEncodeBins(t, storeAddWithCount, normalizedBins)
			}
		})
	}
}

func TestMergeFuzzy(t *testing.T) {
	numTests := 100
	numMerges := 3
	maxNumAdds := 1000

	random := rand.New(rand.NewSource(seed))

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 0; i < numTests; i++ {
				bins := make([]Bin, 0)
				store := testCase.newStore()
				for j := 0; j < numMerges; j++ {
					numValues := random.Intn(maxNumAdds)
					tmpStore := testCase.newStore()
					for k := 0; k < numValues; k++ {
						bin := Bin{index: randomIndex(random), count: randomCount(random)}
						bins = append(bins, bin)
						tmpStore.AddBin(bin)
					}
					store.MergeWith(tmpStore)
				}
				normalizedBins := normalize(testCase.transformBins(bins))
				assertEncodeBins(t, store, normalizedBins)
			}

		})
	}
}

func assertEncodeBins(t *testing.T, store Store, normalizedBins []Bin) {
	expectedTotalCount := float64(0)
	for _, bin := range normalizedBins {
		expectedTotalCount += bin.count
	}

	if expectedTotalCount == 0 {
		assert.True(t, store.IsEmpty(), "empty")
		assert.Equal(t, float64(0), store.TotalCount(), "total count")

		_, minErr := store.MinIndex()
		_, maxErr := store.MaxIndex()
		assert.Equal(t, errUndefinedMinIndex, minErr, "min index err")
		assert.Equal(t, errUndefinedMaxIndex, maxErr, "max index err")

		assert.Zero(t, len(store.Bins()))
	} else {
		assert.False(t, store.IsEmpty(), "empty")
		assert.InEpsilon(t, expectedTotalCount, store.TotalCount(), epsilon, "total count")

		minIndex, minErr := store.MinIndex()
		maxIndex, maxErr := store.MaxIndex()
		assert.Nil(t, minErr, "min index err")
		assert.Nil(t, maxErr, "max index err")
		assert.Equal(t, normalizedBins[0].index, minIndex, "min index")
		assert.Equal(t, normalizedBins[len(normalizedBins)-1].index, maxIndex, "max index")

		i := 0
		for bin := range store.Bins() {
			assert.Equal(t, normalizedBins[i].index, bin.index, "bin index")
			assert.InEpsilon(t, normalizedBins[i].count, bin.count, epsilon, "bin count")
			i++
		}
		assert.Equal(t, len(normalizedBins), i)

		cumulCount := float64(0)
		for i = 0; i < len(normalizedBins)-1; i++ {
			cumulCount += normalizedBins[i].count
			if (i*100)%len(normalizedBins) != 0 {
				// Test at most 10 values to speed up tests.
				continue
			}
			assert.Equal(t, normalizedBins[i].index, store.KeyAtRank(cumulCount*(1-epsilon)), "key at rank before cumul count step")
			assert.Less(t, normalizedBins[i].index, store.KeyAtRank(cumulCount*(1+epsilon)), "key at rank after cumul count step")
		}
		cumulCount += normalizedBins[len(normalizedBins)-1].count
		assert.Equal(t, normalizedBins[len(normalizedBins)-1].index, store.KeyAtRank(cumulCount*(1-epsilon)), "key at rank before total count")
		assert.Equal(t, normalizedBins[len(normalizedBins)-1].index, store.KeyAtRank(cumulCount*(1+epsilon)), "key at rank after total count")
	}
}

// normalize deduplicates indexes, removes counts equal to zero and sorts by index.
func normalize(bins []Bin) []Bin {
	binsByIndex := make(map[int]float64)
	for _, bin := range bins {
		if bin.count <= 0 {
			continue
		}
		binsByIndex[bin.index] += bin.count
	}
	bins = bins[:0]
	for index, count := range binsByIndex {
		bins = append(bins, Bin{index: index, count: count})
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i].index < bins[j].index })
	return bins
}

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

func TestDenseBins(t *testing.T) {
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

func TestDenseMerge(t *testing.T) {
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

func TestDenseMixedMerge1(t *testing.T) {
	nTests := 100
	// Test with int16 values so as to not run into memory issues.
	var values1, values2 []int16
	var store1 *CollapsingLowestDenseStore
	var store2 *DenseStore
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		for _, maxNumBins := range testMaxNumBins {
			f.Fuzz(&values1)
			store1 = NewCollapsingLowestDenseStore(maxNumBins)
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
			if i/2 == 0 {
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

func TestDenseMixedMerge2(t *testing.T) {
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
			if i/2 == 0 {
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

func AssertDenseStoresEqual(t *testing.T, store DenseStore, other DenseStore) {
	assert.Equal(t, store.count, other.count)
	assert.Equal(t, store.minIndex, other.minIndex)
	assert.Equal(t, store.maxIndex, other.maxIndex)
	assert.Equal(
		t,
		store.bins[store.minIndex-store.offset:store.maxIndex+1-store.offset],
		other.bins[other.minIndex-other.offset:other.maxIndex+1-other.offset],
	)
}

func TestSerialization(t *testing.T) {
	nTests := 100
	// Store indices are limited to the int32 range
	var values []int32
	f := fuzz.New().NilChance(0).NumElements(10, 1000)
	for i := 0; i < nTests; i++ {
		f.Fuzz(&values)
		for _, maxNumBins := range testMaxNumBins {
			storeLow := NewCollapsingLowestDenseStore(maxNumBins)
			storeHigh := NewCollapsingHighestDenseStore(maxNumBins)
			for _, v := range values {
				storeLow.Add(int(v))
				storeHigh.Add(int(v))
			}
			deserializedStoreLow := FromProto(storeLow.ToProto())
			AssertDenseStoresEqual(t, storeLow.DenseStore, *deserializedStoreLow)
			//			EvaluateCollapsingLowestStore(t, deserializedStoreLow, values)
			// Store does not change after serializing
			assert.Equal(t, storeLow.maxNumBins, maxNumBins)
			deserializedStoreHigh := FromProto(storeHigh.ToProto())
			AssertDenseStoresEqual(t, storeHigh.DenseStore, *deserializedStoreHigh)
			//EvaluateCollapsingHighestStore(t, deserializedStoreHigh, values)
			// Store does not change after serializing
			assert.Equal(t, storeHigh.maxNumBins, maxNumBins)
		}
	}
}

func TestBufferedPaginatedCompactionDensity(t *testing.T) {
	{
		store := NewBufferedPaginatedStore()
		for index := 0; index < 4*(1<<store.pageLenLog2); index += 2 {
			store.Add(index)
		}
		store.compact()
		assert.Zero(t, len(store.pages))
	}
	{
		store := NewBufferedPaginatedStore()
		for index := 0; index < 4*(1<<store.pageLenLog2); index += 2 {
			for i := 0; i < 8; i++ {
				store.Add(index)
			}
		}
		store.compact()
		assert.Zero(t, len(store.buffer))
	}
}

func TestBufferedPaginatedCompactionFew(t *testing.T) {
	store := NewBufferedPaginatedStore()
	store.Add(2)
	store.Add(-7432)
	store.Add(977)
	store.compact()
	assert.Zero(t, len(store.pages))
}

func TestBufferedPaginatedCompactionOutliers(t *testing.T) {
	store := NewBufferedPaginatedStore()
	for index := 0; index < 1<<store.pageLenLog2; index += 1 {
		for i := 0; i < 2; i++ {
			store.Add(index)
		}
	}
	for i := 0; i < 4; i++ {
		store.Add(6377)
	}
	assert.Equal(t, 4, len(store.buffer))
}

func TestBufferedPaginatedMergeWithProtoFuzzy(t *testing.T) {
	numTests := 100
	numMerges := 3
	maxNumAdds := 1000

	random := rand.New(rand.NewSource(seed))

	for i := 0; i < numTests; i++ {
		bins := make([]Bin, 0)
		store := NewBufferedPaginatedStore()
		for j := 0; j < numMerges; j++ {
			numValues := random.Intn(maxNumAdds)
			tmpStore := NewBufferedPaginatedStore()
			for k := 0; k < numValues; k++ {
				bin := Bin{index: randomIndex(random), count: randomCount(random)}
				bins = append(bins, bin)
				tmpStore.AddBin(bin)
			}
			store.MergeWithProto(tmpStore.ToProto())
		}
		normalizedBins := normalize(bins)
		assertEncodeBins(t, store, normalizedBins)
	}
}

// Benchmarks

var sink Store

func BenchmarkNewAndAddFew(b *testing.B) {
	values := []int{3, 50, -676, 35688}
	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store := testCase.newStore()
				for _, value := range values {
					store.Add(value)
				}
				sink = store
			}
		})
	}
}

func BenchmarkNewAndAddNorm(b *testing.B) {
	for numIndexesLog10 := 0; numIndexesLog10 <= 7; numIndexesLog10++ {
		numIndexes := int(math.Pow10(numIndexesLog10))
		b.Run(fmt.Sprintf("1e%d", numIndexesLog10), func(b *testing.B) {
			for _, testCase := range testCases {
				b.Run(testCase.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						store := testCase.newStore()
						for j := 0; j < numIndexes; j++ {
							store.Add(int(rand.NormFloat64() * 200))
						}
						sink = store
					}
				})
			}
		})
	}
}

func BenchmarkNewAndAddWithCountNorm(b *testing.B) {
	for numIndexesLog10 := 0; numIndexesLog10 <= 7; numIndexesLog10++ {
		numIndexes := int(math.Pow10(numIndexesLog10))
		b.Run(fmt.Sprintf("1e%d", numIndexesLog10), func(b *testing.B) {
			for _, testCase := range testCases {
				b.Run(testCase.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						store := testCase.newStore()
						for j := 0; j < numIndexes; j++ {
							store.AddWithCount(int(rand.NormFloat64()*200), 0.5)
						}
						sink = store
					}
				})
			}
		})
	}
}
