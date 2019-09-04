// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package ddsketch

import (
	"testing"

	"github.com/DataDog/sketches-go/dataset"
	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
)

var testAlpha = 0.01
var testMaxBins = 1024
var testMinValue = 1.0e-9

var testQuantiles = []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1}

var testSizes = []int{3, 5, 10, 100, 1000}

func EvaluateSketch(t *testing.T, n int, gen dataset.Generator) {
	c := NewConfig(testAlpha, testMaxBins, testMinValue)
	g := NewDDSketch(c)
	d := dataset.NewDataset()
	for i := 0; i < n; i++ {
		value := gen.Generate()
		g.Add(value)
		d.Add(value)
	}
	AssertSketchesAccurate(t, d, g, c)
}

func AssertSketchesAccurate(t *testing.T, d *dataset.Dataset, g *DDSketch, c *Config) {
	assert := assert.New(t)
	eps := float64(1.0e-6)
	for _, q := range testQuantiles {
		lowerQuantile := d.LowerQuantile(q)
		upperQuantile := d.UpperQuantile(q)
		var minExpectedValue, maxExpectedValue float64
		if lowerQuantile < 0 {
			minExpectedValue = lowerQuantile * (1 + testAlpha)
		} else {
			minExpectedValue = lowerQuantile * (1 - testAlpha)
		}
		if upperQuantile > 0 {
			maxExpectedValue = upperQuantile * (1 + testAlpha)
		} else {
			maxExpectedValue = upperQuantile * (1 - testAlpha)
		}
		quantile := g.Quantile(q)
		// TODO: be resilient to floating-point errors
		assert.True(minExpectedValue <= quantile)
		assert.True(quantile <= maxExpectedValue)
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

func TestLognormal(t *testing.T) {
	for _, n := range testSizes {
		lognormalGenerator := dataset.NewLognormal(0, -2)
		EvaluateSketch(t, n, lognormalGenerator)
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
		g1 := NewDDSketch(c)
		generator1 := dataset.NewNormal(35, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1.Add(value)
			d.Add(value)
		}
		g2 := NewDDSketch(c)
		generator2 := dataset.NewNormal(50, 2)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)

		g3 := NewDDSketch(c)
		generator3 := dataset.NewNormal(40, 0.5)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3.Add(value)
			d.Add(value)
		}
		g1.Merge(g3)
		AssertSketchesAccurate(t, d, g1, c)
	}
}

func TestMergeEmpty(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		// Merge a non-empty sketch to an empty sketch
		c := NewConfig(testAlpha, testMaxBins, testMinValue)
		g1 := NewDDSketch(c)
		g2 := NewDDSketch(c)
		generator := dataset.NewExponential(5)
		for i := 0; i < n; i++ {
			value := generator.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)
		AssertSketchesAccurate(t, d, g1, c)

		// Merge an empty sketch to a non-empty sketch
		g3 := NewDDSketch(c)
		g2.Merge(g3)
		AssertSketchesAccurate(t, d, g2, c)
	}
}

func TestMergeMixed(t *testing.T) {
	for _, n := range testSizes {
		d := dataset.NewDataset()
		c := NewConfig(testAlpha, testMaxBins, testMinValue)
		g1 := NewDDSketch(c)
		generator1 := dataset.NewNormal(100, 1)
		for i := 0; i < n; i += 3 {
			value := generator1.Generate()
			g1.Add(value)
			d.Add(value)
		}
		g2 := NewDDSketch(c)
		generator2 := dataset.NewExponential(5)
		for i := 1; i < n; i += 3 {
			value := generator2.Generate()
			g2.Add(value)
			d.Add(value)
		}
		g1.Merge(g2)

		g3 := NewDDSketch(c)
		generator3 := dataset.NewExponential(0.1)
		for i := 2; i < n; i += 3 {
			value := generator3.Generate()
			g3.Add(value)
			d.Add(value)
		}
		g1.Merge(g3)

		AssertSketchesAccurate(t, d, g1, c)
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
		s := NewDDSketch(c)
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
