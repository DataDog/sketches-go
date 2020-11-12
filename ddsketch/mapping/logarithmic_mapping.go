// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package mapping

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const expOverflow = 7.094361393031e+02 // The value at which math.Exp overflows

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
	return (withinTolerance(m.relativeAccuracy, o.relativeAccuracy, 1e-12) && withinTolerance(m.multiplier, o.multiplier, 1e-12))
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
		math.Exp(math.MinInt32/m.multiplier+1), // so that index >= MinInt32
		minNormalFloat64*(1+m.relativeAccuracy)/(1-m.relativeAccuracy),
	)
}

func (m *LogarithmicMapping) MaxIndexableValue() float64 {
	return math.Min(
		math.Exp(math.MaxInt32/m.multiplier-1),       // so that index <= MaxInt32
		math.Exp(expOverflow)/(1+m.relativeAccuracy), // so that math.Exp does not overflow
	)
}

func (m *LogarithmicMapping) RelativeAccuracy() float64 {
	return m.relativeAccuracy
}

func (m *LogarithmicMapping) ToProto() *sketchpb.IndexMapping {
	return &sketchpb.IndexMapping{
		Gamma:         (1 + m.relativeAccuracy) / (1 - m.relativeAccuracy),
		IndexOffset:   0,
		Interpolation: sketchpb.IndexMapping_NONE,
	}
}

func (m *LogarithmicMapping) FromProto(pb *sketchpb.IndexMapping) {
	m.relativeAccuracy = 1 - 2/(1+pb.Gamma)
	m.multiplier = 1 / math.Log((1+m.relativeAccuracy)/(1-m.relativeAccuracy))
}

func (m *LogarithmicMapping) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("relativeAccuracy: %v, multiplier: %v\n", m.relativeAccuracy, m.multiplier))
	return buffer.String()
}
