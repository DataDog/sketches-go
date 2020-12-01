// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package ddsketch

import (
	"errors"
	"math"

	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/DataDog/sketches-go/ddsketch/store"
)

type DDSketch struct {
	mapping.IndexMapping
	positiveValueStore store.Store
	negativeValueStore store.Store
	zeroCount          float64
}

func NewDDSketch(indexMapping mapping.IndexMapping, positiveValueStore store.Store, negativeValueStore store.Store) *DDSketch {
	return &DDSketch{
		IndexMapping:       indexMapping,
		positiveValueStore: positiveValueStore,
		negativeValueStore: negativeValueStore,
	}
}

func MemoryOptimalCollapsingLowestSketch(relativeAccuracy float64, maxNumBins int) (*DDSketch, error) {
	indexMapping, err := mapping.NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, err
	}
	return NewDDSketch(indexMapping, store.NewCollapsingLowestDenseStore(maxNumBins), store.NewCollapsingHighestDenseStore(maxNumBins)), nil
}

func (s *DDSketch) Add(value float64) error {
	return s.AddWithCount(value, float64(1))
}

func (s *DDSketch) AddWithCount(value, count float64) error {
	if value < -s.MaxIndexableValue() || value > s.MaxIndexableValue() {
		return errors.New("The input value is outside the range that is tracked by the sketch.")
	}
	if count < 0 {
		return errors.New("The count cannot be negative.")
	}

	if value > s.MinIndexableValue() {
		s.positiveValueStore.AddWithCount(s.Index(value), count)
	} else if value < -s.MinIndexableValue() {
		s.negativeValueStore.AddWithCount(s.Index(-value), count)
	} else {
		s.zeroCount += count
	}
	return nil
}

func (s *DDSketch) Copy() *DDSketch {
	return &DDSketch{
		IndexMapping:       s.IndexMapping,
		positiveValueStore: s.positiveValueStore.Copy(),
		negativeValueStore: s.negativeValueStore.Copy(),
	}
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
	negativeValueCount := s.negativeValueStore.TotalCount()
	if rank < negativeValueCount {
		return -s.Value(s.negativeValueStore.KeyAtRank(negativeValueCount - 1 - rank)), nil
	} else if rank < s.zeroCount+negativeValueCount {
		return 0, nil
	} else {
		return s.Value(s.positiveValueStore.KeyAtRank(rank - s.zeroCount - negativeValueCount)), nil
	}
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
	return s.zeroCount + s.positiveValueStore.TotalCount() + s.negativeValueStore.TotalCount()
}

func (s *DDSketch) IsEmpty() bool {
	return s.zeroCount == 0 && s.positiveValueStore.IsEmpty() && s.negativeValueStore.IsEmpty()
}

func (s *DDSketch) getMaxValue() (float64, error) {
	if !s.positiveValueStore.IsEmpty() {
		maxIndex, _ := s.positiveValueStore.MaxIndex()
		return s.Value(maxIndex), nil
	} else if s.zeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.negativeValueStore.MinIndex()
		if err != nil {
			return math.NaN(), err
		}
		return -s.Value(minIndex), nil
	}
}

func (s *DDSketch) getMinValue() (float64, error) {
	if !s.negativeValueStore.IsEmpty() {
		maxIndex, _ := s.negativeValueStore.MaxIndex()
		return -s.Value(maxIndex), nil
	} else if s.zeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.positiveValueStore.MinIndex()
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
	s.positiveValueStore.MergeWith(other.positiveValueStore)
	s.negativeValueStore.MergeWith(other.negativeValueStore)
	s.zeroCount += other.zeroCount
	return nil
}

func (s *DDSketch) ToProto() *sketchpb.DDSketch {
	return &sketchpb.DDSketch{
		Mapping:        s.IndexMapping.ToProto(),
		PositiveValues: s.positiveValueStore.ToProto(),
		NegativeValues: s.negativeValueStore.ToProto(),
		ZeroCount:      s.zeroCount,
	}
}

func (s *DDSketch) FromProto(pb *sketchpb.DDSketch) *DDSketch {
	return &DDSketch{
		IndexMapping:       s.IndexMapping.FromProto(pb.Mapping),
		positiveValueStore: s.positiveValueStore.FromProto(pb.PositiveValues),
		negativeValueStore: s.negativeValueStore.FromProto(pb.NegativeValues),
		zeroCount:          pb.ZeroCount,
	}
}
