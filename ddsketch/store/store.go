// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"errors"

	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const (
	maxInt = int(^uint(0) >> 1)
	minInt = ^maxInt
)

var (
	errUndefinedMinIndex = errors.New("MinIndex of empty store is undefined")
	errUndefinedMaxIndex = errors.New("MaxIndex of empty store is undefined")
)

type Store interface {
	Add(index int)
	AddBin(bin Bin)
	AddWithCount(index int, count float64)
	// Bins returns a channel that emits the bins that are encoded in the store.
	// Note that this leaks a channel and a goroutine if it is not iterated to completion.
	Bins() <-chan Bin
	// ForEach applies f to all elements of the store or until f returns true.
	ForEach(f func(b Bin) (stop bool))
	Copy() Store
	IsEmpty() bool
	MaxIndex() (int, error)
	MinIndex() (int, error)
	TotalCount() float64
	KeyAtRank(rank float64) int
	MergeWith(store Store)
	ToProto() *sketchpb.Store
	Weight(w float64)
}

// Returns an instance of DenseStore that contains the data in the provided protobuf representation.
func FromProto(pb *sketchpb.Store) *DenseStore {
	store := NewDenseStore()
	for idx, count := range pb.BinCounts {
		store.AddWithCount(int(idx), count)
	}
	for idx, count := range pb.ContiguousBinCounts {
		store.AddWithCount(idx+int(pb.ContiguousBinIndexOffset), count)
	}
	return store
}
