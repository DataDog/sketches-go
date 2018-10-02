// Unless explicitly stated otherwise all files in this repository are licensed
// under the BSD-3-Clause License.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package dogsketch

import (
	"reflect"
)

type Store struct {
	bins     []uint64
	count    int64
	minKey   int
	maxKey   int
	binLimit int
}

func NewStore(binLimit int) *Store {
	// Start with a small number of bins that will grow as needed
	// up to binLimit
	nBins := binLimit / 16
	if nBins < 1 {
		nBins = 1
	}
	return &Store{
		bins:     make([]uint64, nBins),
		count:    0,
		minKey:   0,
		maxKey:   0,
		binLimit: binLimit,
	}
}

func (s *Store) length() int {
	return len(s.bins)
}

func (s *Store) add(key int) {
	if s.count == 0 {
		s.maxKey = key
		s.minKey = key - len(s.bins) + 1
	}
	if key < s.minKey {
		s.growLeft(key)
	} else if key > s.maxKey {
		s.growRight(key)
	}
	idx := key - s.minKey
	if idx < 0 {
		idx = 0
	}
	s.bins[idx]++
	s.count++
}

func (s *Store) growLeft(key int) {
	if s.minKey < key || len(s.bins) >= s.binLimit {
		return
	}
	if s.maxKey-key >= s.binLimit {
		s.bins = append(make([]uint64, s.binLimit-s.maxKey+s.minKey-1), s.bins...)
		s.minKey = s.maxKey - s.binLimit + 1
	} else {
		s.bins = append(make([]uint64, s.minKey-key), s.bins...)
		s.minKey = key
	}
}

func (s *Store) growRight(key int) {
	if s.maxKey > key {
		return
	}
	if key-s.maxKey >= s.binLimit {
		s.bins = make([]uint64, s.binLimit)
		s.maxKey = key
		s.minKey = key - s.binLimit + 1
		s.bins[0] = uint64(s.count)
	} else if key-s.minKey >= s.binLimit {
		var n uint64
		for i := s.minKey; i <= key-s.binLimit && i <= s.maxKey; i++ {
			n += s.bins[i-s.minKey]
		}
		s.bins = append(s.bins[key-s.minKey-s.binLimit+1:], make([]uint64, key-s.maxKey)...)
		s.maxKey = key
		s.minKey = key - s.binLimit + 1
		s.bins[0] += n
	} else {
		s.bins = append(s.bins, make([]uint64, key-s.maxKey)...)
		s.maxKey = key
	}
}

func (s *Store) compress() {
	if len(s.bins) <= s.binLimit {
		return
	}
	var n uint64
	for i := 0; i <= s.maxKey-s.minKey-s.binLimit; i++ {
		n += s.bins[i]
	}
	s.bins = s.bins[s.maxKey-s.minKey-s.binLimit+1:]
	s.minKey = s.maxKey - s.binLimit + 1
	s.bins[0] += n
}

func (s *Store) merge(o *Store) {
	if len(o.bins) == 0 {
		return
	}
	if len(s.bins) == 0 {
		*s = o.makeCopy()
		return
	}

	minKey := min(s.minKey, o.minKey)
	maxKey := max(s.maxKey, o.maxKey)
	tmpBins := make([]uint64, maxKey-minKey+1)
	copy(tmpBins[s.minKey-minKey:], s.bins)
	for i := 0; i < len(o.bins); i++ {
		tmpBins[i+o.minKey-minKey] += o.bins[i]
	}
	s.bins = tmpBins
	s.minKey = minKey
	s.maxKey = maxKey
	s.count += o.count

	s.compress()
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (s *Store) makeCopy() Store {
	bins := make([]uint64, len(s.bins))
	for i := 0; i < len(s.bins); i++ {
		bins[i] = s.bins[i]
	}
	return Store{
		bins:   bins,
		minKey: s.minKey,
		maxKey: s.maxKey,
		count:  s.count,
	}
}

func (s *Store) size() int {
	return int(reflect.TypeOf(*s).Size()) + cap(s.bins)*int(reflect.TypeOf(s.bins).Elem().Size())
}
