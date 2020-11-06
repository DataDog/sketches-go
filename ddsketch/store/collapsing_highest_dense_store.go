// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.

package store

type CollapsingHighestDenseStore struct {
	DenseStore
	maxNumBins int
}

func NewCollapsingHighestDenseStore(maxNumBins int) *CollapsingHighestDenseStore {
	return &CollapsingHighestDenseStore{maxNumBins: maxNumBins}
}

func (s *CollapsingHighestDenseStore) Add(index int) {
	if s.count == 0 {
		s.bins = make([]float64, min(growthBuffer, s.maxNumBins))
		s.minIndex = index
		s.maxIndex = index + len(s.bins) - 1
	}
	if index < s.minIndex {
		s.growLeft(index)
	} else if index > s.maxIndex {
		s.growRight(index)
	}
	var idx int
	if index > s.maxIndex {
		idx = len(s.bins) - 1
	} else {
		idx = index - s.minIndex
	}
	s.bins[idx]++
	s.count++
}

func (s *CollapsingHighestDenseStore) growLeft(index int) {
	if s.minIndex < index {
		return
	}
	if index <= s.minIndex-s.maxNumBins {
		s.bins = make([]float64, s.maxNumBins)
		s.minIndex = index
		s.maxIndex = index + s.maxNumBins - 1
		s.bins[s.maxNumBins-1] = s.count
	} else if index <= s.maxIndex-s.maxNumBins {
		maxIndex := index + s.maxNumBins - 1
		var n float64
		for i := max(s.minIndex, maxIndex+1); i <= s.maxIndex; i++ {
			n += s.bins[i-s.minIndex]
		}
		if len(s.bins) < s.maxNumBins {
			tmpBins := make([]float64, s.maxNumBins)
			copy(tmpBins[s.minIndex-index:], s.bins)
			s.bins = tmpBins
		} else {
			copy(s.bins[s.minIndex-index:], s.bins)
			for i := 0; i < s.minIndex-index; i++ {
				s.bins[i] = 0.0
			}
		}
		s.minIndex = index
		s.maxIndex = maxIndex
		s.bins[s.maxNumBins-1] += n
	} else {
		tmpBins := make([]float64, s.maxIndex-index+1)
		copy(tmpBins[s.minIndex-index:], s.bins)
		s.bins = tmpBins
		s.minIndex = index
	}
}

func (s *CollapsingHighestDenseStore) growRight(index int) {
	if s.maxIndex > index || len(s.bins) >= s.maxNumBins {
		return
	}
	var maxIndex int
	if index >= s.minIndex+s.maxNumBins {
		maxIndex = s.minIndex + s.maxNumBins - 1
	} else {
		// Expand bins by up to an extra growthBuffers bins than strictly required.
		maxIndex = min(index+growthBuffer, s.minIndex+s.maxNumBins-1)
	}
	tmpBins := make([]float64, maxIndex-s.minIndex+1)
	copy(tmpBins, s.bins)
	s.bins = tmpBins
	s.maxIndex = maxIndex
}

func (s *CollapsingHighestDenseStore) MergeWith(other Store) {
	if other.TotalCount() == 0 {
		return
	}
	o, ok := other.(*CollapsingHighestDenseStore)
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
	s.growRight(o.maxIndex)
	s.growLeft(o.minIndex)
	for i := max(s.minIndex, o.minIndex); i <= min(s.maxIndex, o.maxIndex); i++ {
		s.bins[i-s.minIndex] += o.bins[i-o.minIndex]
	}
	var n float64
	for i := max(s.maxIndex+1, o.minIndex); i <= o.maxIndex; i++ {
		n += o.bins[i-o.minIndex]
	}
	s.bins[len(s.bins)-1] += n
	s.count += o.count
}

func (s *CollapsingHighestDenseStore) AddBin(bin Bin) {
	index := bin.Index()
	count := bin.Count()
	if count == 0 {
		return
	}
	if index < s.minIndex {
		s.growLeft(index)
	} else if index > s.maxIndex {
		s.growRight(index)
	}
	var idx int
	if index > s.maxIndex {
		idx = s.maxIndex - s.minIndex
	} else {
		idx = index - s.minIndex
	}
	s.bins[idx] += count
	s.count += count
}

func (s *CollapsingHighestDenseStore) copy(o *CollapsingHighestDenseStore) {
	s.bins = make([]float64, len(o.bins))
	copy(s.bins, o.bins)
	s.minIndex = o.minIndex
	s.maxIndex = o.maxIndex
	s.count = o.count
	s.maxNumBins = o.maxNumBins
}
