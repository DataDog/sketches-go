// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

type Store interface {
	Add(index int32)
	AddBin(bin Bin)
	Bins() <-chan Bin
	IsEmpty() bool
	MaxIndex() (int32, error)
	MinIndex() (int32, error)
	TotalCount() float64
	KeyAtRank(rank float64) int32
	KeyAtDescendingRank(rank float64) int32
	MergeWith(store Store)
	ToProto() *sketchpb.Store
	FromProto(pb *sketchpb.Store)
}
