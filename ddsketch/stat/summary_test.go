// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package stat

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromData(t *testing.T) {
	{
		_, err := NewSummaryStatisticsFromData(0.0, 0.0, math.Inf(1), math.Inf(-1))
		assert.NoError(t, err)
	}
	{
		_, err := NewSummaryStatisticsFromData(1.0, 2.0, 3.0, 3.0)
		assert.NoError(t, err)
	}
	{
		_, err := NewSummaryStatisticsFromData(1.0, 2.0, 3.0, 4.0)
		assert.NoError(t, err)
	}
	{
		_, err := NewSummaryStatisticsFromData(0.0, 0.0, 0.0, 0.0)
		assert.Error(t, err)
	}
	{
		_, err := NewSummaryStatisticsFromData(-1.0, 0.0, 0.0, 0.0)
		assert.Error(t, err)
	}
	{
		_, err := NewSummaryStatisticsFromData(1.0, 0.0, 1.0, 0.0)
		assert.Error(t, err)
	}
}

func TestEmpty(t *testing.T) {
	s := NewSummaryStatistics()
	assertEmpty(t, s)
}

func TestAddWithCount(t *testing.T) {
	s := NewSummaryStatistics()
	s.Add(0, 0)
	assert.Equal(t, 0.0, s.Count(), "count")
	assert.Equal(t, 0.0, s.Sum(), "sum")
	assert.Equal(t, 0.0, s.Min(), "min")
	assert.Equal(t, 0.0, s.Max(), "max")

	s.Add(1, -2)
	assert.Equal(t, -2.0, s.Count(), "count")
	assert.Equal(t, -2.0, s.Sum(), "sum")
	assert.Equal(t, 0.0, s.Min(), "min")
	assert.Equal(t, 1.0, s.Max(), "max")

	s.Add(-2, 3)
	assert.Equal(t, 1.0, s.Count(), "count")
	assert.Equal(t, -8.0, s.Sum(), "sum")
	assert.Equal(t, -2.0, s.Min(), "min")
	assert.Equal(t, 1.0, s.Max(), "max")
}

func TestMergeWith(t *testing.T) {
	s1 := NewSummaryStatistics()
	s2 := NewSummaryStatistics()
	s1.MergeWith(s2)
	assertEmpty(t, s1)

	s2.Add(1, -2)
	s1.MergeWith(s2)
	assertEqual(t, s1, s2)

	s3 := NewSummaryStatistics()
	s3.Add(-6, -7)
	s2.Add(-6, -7)
	s1.MergeWith(s3)
	assertEqual(t, s1, s2)
}

func TestClear(t *testing.T) {
	s := NewSummaryStatistics()
	s.Clear()
	assertEmpty(t, s)
	s.Add(1, 2)
	s.Clear()
	assertEmpty(t, s)
}

func TestCopy(t *testing.T) {
	s := NewSummaryStatistics()
	assertEmpty(t, s.Copy())
	s.Add(-1.0, 1)
	copy := s.Copy()
	s.Add(2, -3)
	copy.Add(4, 5)
	copy.Add(6, -7)

	assert.Equal(t, -2.0, s.Count())
	assert.Equal(t, -7.0, s.Sum())
	assert.Equal(t, -1.0, s.Min())
	assert.Equal(t, 2.0, s.Max())

	assert.Equal(t, -1.0, copy.Count())
	assert.Equal(t, -23.0, copy.Sum())
	assert.Equal(t, -1.0, copy.Min())
	assert.Equal(t, 6.0, copy.Max())
}

func TestReweight(t *testing.T) {
	s := NewSummaryStatistics()
	s.Reweight(0)
	assertEmpty(t, s)
	s.Reweight(1)
	assertEmpty(t, s)
	s.Reweight(-2)
	assertEmpty(t, s)
	s.Add(-1, 2)
	s.Add(3, -6)

	s.Reweight(-4)
	s2 := NewSummaryStatistics()
	s2.Add(-1, 2*-4)
	s2.Add(3, -6*-4)
	assertEqual(t, s, s2)

	s.Reweight(0)
	assertEmpty(t, s)
}

func TestRescale(t *testing.T) {
	s := NewSummaryStatistics()
	s.Rescale(0)
	assertEmpty(t, s)
	s.Rescale(3)
	assertEmpty(t, s)
	s.Rescale(-2)
	assertEmpty(t, s)
	s.Add(-1, 2)
	s.Add(3, -6)

	s.Rescale(3)
	s2 := NewSummaryStatistics()
	s2.Add(-1*3, 2)
	s2.Add(3*3, -6)
	assertEqual(t, s, s2)

	s.Rescale(-4)
	s3 := NewSummaryStatistics()
	s3.Add(-1*3*-4, 2)
	s3.Add(3*3*-4, -6)
	assertEqual(t, s, s3)

	s.Rescale(0)
	s4 := NewSummaryStatistics()
	s4.Add(0, -4)
	assertEqual(t, s, s4)
}

func assertEmpty(t *testing.T, s *SummaryStatistics) {
	assert.Equal(t, 0.0, s.Count(), "count")
	assert.Equal(t, 0.0, s.Sum(), "sum")
	assert.Equal(t, math.Inf(1), s.Min(), "min")
	assert.Equal(t, math.Inf(-1), s.Max(), "max")
}

func assertEqual(t *testing.T, s1 *SummaryStatistics, s2 *SummaryStatistics) {
	assert.Equal(t, s1.Count(), s2.Count(), "count")
	assert.Equal(t, s1.Sum(), s2.Sum(), "sum")
	assert.Equal(t, s1.Min(), s2.Min(), "min")
	assert.Equal(t, s1.Max(), s2.Max(), "max")
}
