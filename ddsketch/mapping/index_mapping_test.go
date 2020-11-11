// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package mapping

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	testMaxRelativeAccuracy      = 1 - 1e-3
	testMinRelativeAccuracy      = 1e-7
	floatingPointAcceptableError = 1e-12
)

var multiplier = 1 + math.Sqrt(2)*1e2

func EvaluateRelativeAccuracy(t *testing.T, expected, actual, relativeAccuracy float64) {
	assert.True(t, expected >= 0)
	assert.True(t, actual >= 0)
	if expected == 0 {
		assert.InDelta(t, actual, 0, floatingPointAcceptableError)
	} else {
		assert.True(t, math.Abs(expected-actual)/expected <= relativeAccuracy+floatingPointAcceptableError)
	}
}

func EvaluateMappingAccuracy(t *testing.T, mapping IndexMapping, relativeAccuracy float64) {
	for value := mapping.MinIndexableValue(); value < mapping.MaxIndexableValue(); value *= multiplier {
		mappedValue := mapping.Value(mapping.Index(value))
		EvaluateRelativeAccuracy(t, value, mappedValue, relativeAccuracy)
	}
	value := mapping.MaxIndexableValue()
	mappedValue := mapping.Value(mapping.Index(value))
	EvaluateRelativeAccuracy(t, value, mappedValue, relativeAccuracy)
}

func TestLogarithmicMappingAccuracy(t *testing.T) {
	for relativeAccuracy := testMaxRelativeAccuracy; relativeAccuracy >= testMinRelativeAccuracy; relativeAccuracy *= (testMaxRelativeAccuracy * testMaxRelativeAccuracy) {
		mapping, _ := NewLogarithmicMapping(relativeAccuracy)
		EvaluateMappingAccuracy(t, mapping, relativeAccuracy)
	}
}

func TestLinearlyInterpolatedMappingAccuracy(t *testing.T) {
	for relativeAccuracy := testMaxRelativeAccuracy; relativeAccuracy >= testMinRelativeAccuracy; relativeAccuracy *= (testMaxRelativeAccuracy * testMaxRelativeAccuracy) {
		mapping, _ := NewLinearlyInterpolatedMapping(relativeAccuracy)
		EvaluateMappingAccuracy(t, mapping, relativeAccuracy)
	}
}

func TestLogarithmicMappingSerialization(t *testing.T) {
	mapping, _ := NewLogarithmicMapping(1e-2)
	deserializedMapping, _ := NewLogarithmicMapping(0.5)
	deserializedMapping.FromProto(mapping.ToProto())
	assert.True(t, mapping.Equals(deserializedMapping))
}

func TestLinearlyInterpolatedMappingSerialization(t *testing.T) {
	mapping, _ := NewLinearlyInterpolatedMapping(1e-2)
	deserializedMapping, _ := NewLinearlyInterpolatedMapping(0.5)
	deserializedMapping.FromProto(mapping.ToProto())
	assert.True(t, mapping.Equals(deserializedMapping))
}
