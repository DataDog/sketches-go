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

const (
	exponentBias     = 1023
	exponentMask     = uint64(0x7FF0000000000000)
	exponentShift    = 52
	significandMask  = uint64(0x000fffffffffffff)
	significandWidth = 53
	oneMask          = uint64(0x3ff0000000000000)
)

type LinearlyInterpolatedMapping struct {
	relativeAccuracy float64
	multiplier       float64
}

func NewLinearlyInterpolatedMapping(relativeAccuracy float64) (*LinearlyInterpolatedMapping, error) {
	if relativeAccuracy <= 0 || relativeAccuracy >= 1 {
		return nil, errors.New("The relative accuracy must be between 0 and 1.")
	}
	return &LinearlyInterpolatedMapping{
		relativeAccuracy: relativeAccuracy,
		multiplier:       1.0 / math.Log((1+relativeAccuracy)/(1-relativeAccuracy)),
	}, nil
}

func (m *LinearlyInterpolatedMapping) Equals(other IndexMapping) bool {
	o, ok := other.(*LinearlyInterpolatedMapping)
	if !ok {
		return false
	}
	return (withinTolerance(m.relativeAccuracy, o.relativeAccuracy, 1e-12) && withinTolerance(m.multiplier, o.multiplier, 1e-12))
}

func (m *LinearlyInterpolatedMapping) Index(value float64) int32 {
	index := m.approximateLog(value) * m.multiplier
	if index >= 0 {
		return int32(index)
	} else {
		return int32(index) - 1
	}
}

func (m *LinearlyInterpolatedMapping) Value(index int32) float64 {
	return m.approximateInverseLog(float64(index)/m.multiplier) * (1 + m.relativeAccuracy)
}

func (m *LinearlyInterpolatedMapping) approximateLog(x float64) float64 {
	bits := math.Float64bits(x)
	return float64(int((bits&exponentMask)>>exponentShift)-exponentBias) + math.Float64frombits((bits&significandMask)|oneMask)
}

func (m *LinearlyInterpolatedMapping) approximateInverseLog(x float64) float64 {
	exponent := math.Floor(x - 1)
	fullSignificand := x - exponent
	return math.Float64frombits((uint64((int(exponent)+exponentBias)<<exponentShift) & exponentMask) | (math.Float64bits(fullSignificand) & significandMask))
}

func (m *LinearlyInterpolatedMapping) MinIndexableValue() float64 {
	return math.Max(
		m.approximateInverseLog((math.MinInt32+1)/m.multiplier), // so that index >= MinInt32
		minNormalFloat64*(1+m.relativeAccuracy)/(1-m.relativeAccuracy),
	)
}

func (m *LinearlyInterpolatedMapping) MaxIndexableValue() float64 {
	return math.Min(
		math.Pow(2, math.MaxInt32/m.multiplier-m.approximateLog(float64(1))-1), // so that index <= MaxInt32
		math.MaxFloat64/(1+m.relativeAccuracy),
	)
}

func (m *LinearlyInterpolatedMapping) RelativeAccuracy() float64 {
	return m.relativeAccuracy
}

func (m *LinearlyInterpolatedMapping) ToProto() *sketchpb.IndexMapping {
	return &sketchpb.IndexMapping{
		Gamma:         math.Pow(2, 1/m.multiplier),
		IndexOffset:   0,
		Interpolation: sketchpb.IndexMapping_LINEAR,
	}
}

func (m *LinearlyInterpolatedMapping) FromProto(pb *sketchpb.IndexMapping) {
	m.relativeAccuracy = 1 - 2/(1+math.Exp(math.Log(pb.Gamma)/math.Log(2)))
	m.multiplier = math.Log(2) / math.Log(pb.Gamma)
}

func (m *LinearlyInterpolatedMapping) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("relativeAccuracy: %v, multiplier: %v\n", m.relativeAccuracy, m.multiplier))
	return buffer.String()
}

func withinTolerance(x, y, tolerance float64) bool {
	return math.Abs(x-y) <= tolerance
}
