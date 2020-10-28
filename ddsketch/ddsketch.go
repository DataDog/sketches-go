// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package ddsketch

import (
	"errors"
	"math"

	"github.com/DataDog/sketches-go/ddsketch/store"
)

type DDSketch struct {
	IndexMapping
	store     store.Store
	zeroCount float64
}

func NewDDSketch(indexMapping IndexMapping, store store.Store) *DDSketch {
	return &DDSketch{
		IndexMapping: indexMapping,
		store:        store,
	}
}

func MemoryOptimalCollapsingLowestSketch(relativeAccuracy float64, maxNumBins int) (*DDSketch, error) {
	indexMapping, err := NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, err
	}
	return NewDDSketch(indexMapping, store.NewCollapsingLowestDenseStore(maxNumBins)), nil
}

func (s *DDSketch) Accept(value float64) error {
	if value < 0 || value > s.MaxIndexableValue() {
		return errors.New("The input value is outside the range that is tracked by the sketch.")
	}

	if value < s.MinIndexableValue() {
		s.zeroCount++
	} else {
		s.store.Add(s.Index(value))
	}
	return nil
}

func (s *DDSketch) getValueAtQuantile(quantile float64) (float64, error) {
	if quantile < 0 || quantile > 1 {
		return math.NaN(), errors.New("The quantile must be between 0 and 1.")
	}

	count := s.getCount()
	if count == 0 {
		return math.NaN(), errors.New("No such element exists")
	}

	rank := quantile * (count - 1)
	if rank < s.zeroCount {
		return 0, nil
	}
	return s.Value(s.store.KeyAtRank(rank - s.zeroCount)), nil
}

func (s *DDSketch) getValuesAtQuantiles(quantiles []float64) ([]float64, error) {
	values := make([]float64, len(quantiles))
	for i, q := range quantiles {
		val, err := s.getValueAtQuantile(q)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

func (s *DDSketch) getCount() float64 {
	return s.zeroCount + s.store.TotalCount()
}

func (s *DDSketch) IsEmpty() bool {
	return s.getCount() == 0
}

func (s *DDSketch) getMaxValue() (float64, error) {
	if s.zeroCount > 0 && s.store.IsEmpty() {
		return 0, nil
	} else {
		maxIndex, err := s.store.MaxIndex()
		if err != nil {
			return math.NaN(), err
		}
		return s.Value(maxIndex), nil
	}
}

func (s *DDSketch) getMinValue() (float64, error) {
	if s.zeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.store.MinIndex()
		if err != nil {
			return math.NaN(), err
		}
		return s.Value(minIndex), nil
	}
}

func (s *DDSketch) MergeWith(other *DDSketch) error {
	if !s.IndexMapping.Equals(other.IndexMapping) {
		return errors.New("Cannot merge sketches with different index mappings.")
	}
	s.store.MergeWith(other.store)
	s.zeroCount += other.zeroCount
	return nil
}
