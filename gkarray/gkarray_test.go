// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package gk

import (
	"testing"

	"github.com/DataDog/sketches-go/dataset"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

var testEps = 0.01
var testQuantiles = []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}
var testSizes = []int{3, 5, 10, 100, 1000, 5000}

func EvaluateSketch(t *testing.T, n int, gen dataset.Generator) {
	g := NewGKArray(testEps)
	d := dataset.NewDataset()
	for i := 0; i < n; i++ {
		value := gen.Generate()
		g.Add(value)
		d.Add(value)
	}
	AssertSketchesAccurate(t, d, g)
}

func AssertSketchesAccurate(t *testing.T, d *dataset.Dataset, g *GKArray) {
	assert := assert.New(t)
	eps := float64(1.0e-6)
	for _, q := range testQuantiles {
		expectedRank := int64(q*float64(d.Count-1)) + 1 // min rank
		delta := int64(g.epsilon * (float64(d.Count - 1)))
		quantile := g.Quantile(q)
		minRank := d.MinRank(quantile)
		maxRank := d.MaxRank(quantile)
		assert.True(minRank-delta <= expectedRank && expectedRank <= maxRank+delta)
	}
	assert.Equal(d.Min(), g.min)
	assert.Equal(d.Max(), g.max)
	assert.InEpsilon(d.Sum(), g.sum, eps)
	assert.Equal(d.Count, g.count)
}

func TestConstant(t *testing.T) {
	for _, n := range testSizes {
		constantGenerator := dataset.NewConstant(42)
		EvaluateSketch(t, n, constantGenerator)
	}
}

func TestLinear(t *testing.T) {
	for _, n := range testSizes {
		linearGenerator := dataset.NewLinear()
		EvaluateSketch(t, n, linearGenerator)
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
		g1 := NewGKArray(testEps)
		generator1 := dataset.NewNormal(35, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1.Add(value)
			d.Add(value)
		}
		g2 := NewGKArray(testEps)
		generator2 := dataset.NewNormal(50, 2)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)

		g3 := NewGKArray(testEps)
		generator3 := dataset.NewNormal(40, 0.5)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3.Add(value)
			d.Add(value)
		}
		g1.Merge(g3)
		AssertSketchesAccurate(t, d, g1)
	}
}

func TestMergeEmpty(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		// Merge a non-empty sketch to an empty sketch
		g1 := NewGKArray(testEps)
		g2 := NewGKArray(testEps)
		generator := dataset.NewExponential(5)
		for i := 0; i < n; i++ {
			value := generator.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)
		AssertSketchesAccurate(t, d, g1)

		// Merge an empty sketch to a non-empty sketch
		g3 := NewGKArray(testEps)
		g2.Merge(g3)
		AssertSketchesAccurate(t, d, g2)
	}
}

func TestMergeMixed(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		g1 := NewGKArray(testEps)
		generator1 := dataset.NewNormal(100, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1.Add(value)
			d.Add(value)
		}
		g2 := NewGKArray(testEps)
		generator2 := dataset.NewExponential(5)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)

		g3 := NewGKArray(testEps)
		generator3 := dataset.NewExponential(0.1)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3.Add(value)
			d.Add(value)
		}
		g1.Merge(g3)

		AssertSketchesAccurate(t, d, g1)
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
		assert.True(t, isValid(s1))
		assert.True(t, isValid(s2))
		assert.NotPanics(t, func() { s1.Quantile(q); s1.Merge(&s2) })
	}
}

func makeValid(s GKArray) GKArray {
	if len(s.entries) == 0 {
		s.count = int64(len(s.entries))
	}

	gSum := int64(0)
	for _, e := range s.entries {
		gSum += int64(e.g)
	}
	s.count = gSum + int64(len(s.incoming))

	return s
}

func isValid(s GKArray) bool {
	// Check that count is valid
	if s.count < 0 {
		return false
	}
	if len(s.entries) == 0 {
		if int64(len(s.incoming)) != s.count {
			return false
		}
	}
	gSum := int64(0)
	for _, e := range s.entries {
		gSum += int64(e.g)
	}
	if gSum+int64(len(s.incoming)) != s.count {
		return false
	}
	return true
}

// Test that successive Quantile() calls do not modify the sketch
func TestConsistentQuantile(t *testing.T) {
	var vals []float64
	var q float64
	nTests := 200
	vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	fuzzer := fuzz.New()
	for i := 0; i < nTests; i++ {
		s := NewGKArray(testEps)
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

// Test that Quantile() calls do not panic for number of values up to 1/epsilon
func TestNoPanic(t *testing.T) {
	s := NewGKArray(testEps)
	for i := 0; i < 2*int(1/s.epsilon); i++ {
		s.Add(float64(i))
		assert.NotPanics(t, func() { s.Quantile(0.9) })
	}
}

// Test that Merge() calls do not modify the sketch
func TestConsistentMerge(t *testing.T) {
	var vals []float64
	var q float64
	nTests := 200
	vfuzzer := fuzz.New().NilChance(0).NumElements(10, 500)
	fuzzer := fuzz.New()
	for i := 0; i < nTests; i++ {
		g := NewGKArray(testEps)
		s := NewGKArray(testEps)
		vfuzzer.Fuzz(&vals)
		fuzzer.Fuzz(&q)
		for _, v := range vals {
			g.Add(v)
		}
		vfuzzer.Fuzz(&vals)
		for _, v := range vals {
			s.Add(v)
		}
		q1 := s.Quantile(q)
		g.Merge(s)
		// Compare an arbitrary quantile of the sketch before and after merge
		q2 := s.Quantile(q)
		assert.Equal(t, q1, q2)
	}
}
