// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package ddsketch

import (
	"errors"
	"math"
	"math/bits"
)

const (
	maxInt = 1<<(bits.UintSize-1) - 1
	minInt = -maxInt - 1
)

type IndexMapping interface {
	Equals(other IndexMapping) bool
	Index(value float64) int
	Value(index int) float64
	RelativeAccuracy() float64
	MinIndexableValue() float64
	MaxIndexableValue() float64
}

type LogarithmicMapping struct {
	relativeAccuracy float64
	multiplier       float64
}

func NewLogarithmicMapping(relativeAccuracy float64) (*LogarithmicMapping, error) {
	if relativeAccuracy <= 0 || relativeAccuracy >= 1 {
		return nil, errors.New("The relative accuracy must be between 0 and 1.")
	}
	return &LogarithmicMapping{
		relativeAccuracy: relativeAccuracy,
		multiplier:       1 / math.Log((1+relativeAccuracy)/(1-relativeAccuracy)),
	}, nil
}

func (m *LogarithmicMapping) Equals(other IndexMapping) bool {
	o, ok := other.(*LogarithmicMapping)
	if !ok {
		return false
	}
	return (m.relativeAccuracy == o.relativeAccuracy && m.multiplier == o.multiplier)
}

func (m *LogarithmicMapping) Index(value float64) int {
	index := math.Log(value) * m.multiplier
	if index >= 0 {
		return int(index)
	} else {
		return int(index) - 1 // faster than Math.Floor
	}
}

func (m *LogarithmicMapping) Value(index int) float64 {
	return math.Exp((float64(index) / m.multiplier)) * (1 + m.relativeAccuracy)
}

func (m *LogarithmicMapping) MinIndexableValue() float64 {
	return math.Max(
		math.Pow(math.E, minInt/m.multiplier+1), // so that index >= minInt
		math.SmallestNonzeroFloat64*(1+m.relativeAccuracy)/(1-m.relativeAccuracy),
	)
}

func (m *LogarithmicMapping) MaxIndexableValue() float64 {
	return math.Min(
		math.Pow(math.E, maxInt/m.multiplier-1), // so that index <= maxInt
		math.MaxFloat64/(1+m.relativeAccuracy),
	)
}

func (m *LogarithmicMapping) RelativeAccuracy() float64 {
	return m.relativeAccuracy
}
