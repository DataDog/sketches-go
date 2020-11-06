// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const (
	// Grow the bins with an extra growthBuffer bins to prevent growing too often
	growthBuffer = 128
)

// DenseStore is a dynamically growing contiguous (non-sparse) store. The number of bins are
// bound only by the size of the slice that can be allocated.
type DenseStore struct {
	bins     []float64
	count    float64
	minIndex int
	maxIndex int
}

func NewDenseStore() *DenseStore {
	return &DenseStore{}
}

func (s *DenseStore) Add(index int) {
	s.addWithCount(index, float64(1))
}

func (s *DenseStore) AddBin(bin Bin) {
	index := bin.Index()
	count := bin.Count()
	if count == 0 {
		return
	}
	s.addWithCount(index, count)
}

func (s *DenseStore) addWithCount(index int, count float64) {
	if s.count == 0 {
		s.bins = make([]float64, growthBuffer)
		s.maxIndex = index
		s.minIndex = index - len(s.bins) + 1
	}
	if index < s.minIndex {
		s.growLeft(index)
	} else if index > s.maxIndex {
		s.growRight(index)
	}
	idx := index - s.minIndex
	s.bins[idx] += count
	s.count += count
}

func (s *DenseStore) IsEmpty() bool {
	return s.count == 0
}

func (s *DenseStore) TotalCount() float64 {
	return s.count
}

func (s *DenseStore) MinIndex() (int, error) {
	if s.count == 0 {
		return 0, errors.New("MinIndex of empty store is undefined.")
	}
	for i, b := range s.bins {
		if b > 0 {
			return i + s.minIndex, nil
		}
	}
	return s.maxIndex, nil
}

func (s *DenseStore) MaxIndex() (int, error) {
	if s.count == 0 {
		return 0, errors.New("MaxIndex of empty store is undefined.")
	}
	for i := s.maxIndex; i >= s.minIndex; i-- {
		if s.bins[i-s.minIndex] > 0 {
			return i, nil
		}
	}
	return s.minIndex, nil
}

// Return the key for the value at rank
func (s *DenseStore) KeyAtRank(rank float64) int {
	var n float64
	for i, b := range s.bins {
		n += b
		if n > rank {
			return i + s.minIndex
		}
	}
	return s.maxIndex
}

// Return the key for the value at rank from the highest bin
func (s *DenseStore) KeyAtDescendingRank(rank float64) int {
	var n float64
	for i := len(s.bins) - 1; i >= 0; i-- {
		n += s.bins[i]
		if n > rank {
			return i + s.minIndex
		}
	}
	return s.minIndex
}

func (s *DenseStore) growLeft(index int) {
	if s.minIndex < index {
		return
	}
	// Expand bins by an extra growthBuffer bins than strictly required.
	minIndex := index - growthBuffer
	// Note that there's no protection against integer overflow of s.maxIndex-minIndex+1,
	// or whether allocating a slice of this size is possible.
	tmpBins := make([]float64, s.maxIndex-minIndex+1)
	copy(tmpBins[s.minIndex-minIndex:], s.bins)
	s.bins = tmpBins
	s.minIndex = minIndex
}

func (s *DenseStore) growRight(index int) {
	if s.maxIndex > index {
		return
	}
	// Expand bins by an extra growthBuffer bins than strictly required.
	maxIndex := index + growthBuffer
	// Note that there's no protection against integer overflow of maxIndex-s.minIndex+1,
	// or whether allocating a slice of this size is possible.
	tmpBins := make([]float64, maxIndex-s.minIndex+1)
	copy(tmpBins, s.bins)
	s.bins = tmpBins
	s.maxIndex = maxIndex
}

func (s *DenseStore) MergeWith(other Store) {
	if other.TotalCount() == 0 {
		return
	}
	o, ok := other.(*DenseStore)
	if !ok {
		for bin := range other.Bins() {
			s.AddBin(bin)
		}
		return
	}
	if s.count == 0 {
		s.copy(o)
		return
	}
	if s.minIndex > o.minIndex {
		s.growLeft(o.minIndex)
	}
	if s.maxIndex < o.maxIndex {
		s.growRight(o.maxIndex)
	}
	for idx := o.minIndex; idx <= o.maxIndex; idx++ {
		s.bins[idx-s.minIndex] += o.bins[idx-o.minIndex]
	}
	s.count += o.count
}

func (s *DenseStore) Bins() <-chan Bin {
	ch := make(chan Bin)
	go func() {
		defer close(ch)
		for idx := s.minIndex; idx <= s.maxIndex; idx++ {
			if s.bins[idx-s.minIndex] > 0 {
				ch <- Bin{index: idx, count: s.bins[idx-s.minIndex]}
			}
		}
	}()
	return ch
}

func (s *DenseStore) copy(o *DenseStore) {
	s.bins = make([]float64, len(o.bins))
	copy(s.bins, o.bins)
	s.minIndex = o.minIndex
	s.maxIndex = o.maxIndex
	s.count = o.count
}

func (s *DenseStore) string() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := 0; i < len(s.bins); i++ {
		index := i + s.minIndex
		buffer.WriteString(fmt.Sprintf("%d: %f, ", index, s.bins[i]))
	}
	buffer.WriteString(fmt.Sprintf("count: %v, minIndex: %d, maxIndex: %d}", s.count, s.minIndex, s.maxIndex))
	return buffer.String()
}

func (s *DenseStore) ToProto() *sketchpb.Store {
	bins := make([]float64, len(s.bins))
	copy(bins, s.bins)
	return &sketchpb.Store{
		ContiguousBinCounts:      bins,
		ContiguousBinIndexOffset: int32(s.minIndex),
	}
}

func (s *DenseStore) FromProto(pb *sketchpb.Store) {
	// Reset the store.
	s.count = 0
	s.bins = nil
	s.minIndex = 0
	s.maxIndex = 0
	for idx, count := range pb.BinCounts {
		s.addWithCount(int(idx), count)
	}
	for idx, count := range pb.ContiguousBinCounts {
		s.addWithCount(idx+int(pb.ContiguousBinIndexOffset), count)
	}
}
