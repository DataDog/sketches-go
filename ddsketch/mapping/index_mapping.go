// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package mapping

import (
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const (
	minNormalFloat64 = 2.2250738585072014e-308 //2^(-1022)
)

type IndexMapping interface {
	Equals(other IndexMapping) bool
	Index(value float64) int
	Value(index int) float64
	RelativeAccuracy() float64
	MinIndexableValue() float64
	MaxIndexableValue() float64
	String() string
	ToProto() *sketchpb.IndexMapping
	FromProto(pb *sketchpb.IndexMapping)
}