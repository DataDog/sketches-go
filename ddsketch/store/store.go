// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

type Store interface {
	Add(index int)
	AddBin(bin Bin)
	Bins() <-chan Bin
	IsEmpty() bool
	MaxIndex() (int, error)
	MinIndex() (int, error)
	TotalCount() float64
	KeyAtRank(rank float64) int
	KeyAtDescendingRank(rank float64) int
	MergeWith(store Store)
	ToProto() *sketchpb.Store
	FromProto(pb *sketchpb.Store)
}
