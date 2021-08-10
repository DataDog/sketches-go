// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"errors"
	"reflect"
	"unsafe"

	enc "github.com/DataDog/sketches-go/ddsketch/encoding"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

// All sizes in bytes.
const (
	ptrSizeLog2         = uint8(2 + ^uintptr(0)>>63)
	intSizeLog2         = uint8(2 + ^uint(0)>>63)
	float64sizeLog2     = uint8(3)
	bufferEntrySizeLog2 = intSizeLog2
	countSizeLog2       = float64sizeLog2

	defaultPageSizeLog2 = uint8(8) // 256 bytes, 32 float64 counts
	minSliceCap         = 8
)

var errUnverifiedPredicate = errors.New("the predicate on the cumulative count is never verified")

// BufferedPaginatedStore allocates storage for counts in aligned fixed-size
// pages, themselves stored in a dynamically-sized slice. A page encodes the
// counts for a contiguous range of indexes, and two pages that are contiguous
// in the slice encode ranges that are contiguous. In addition, input indexes
// that are added to the store with a count equal to 1 can be stored in a
// buffer.
// The store favors using the buffer and only creates pages when the memory size
// of the page is no greater than the memory space that is needed to keep in the
// buffer the indexes that could otherwise be encoded in that page. That means
// that some indexes may stay indefinitely in the buffer if, to be removed from
// the buffer, they would create a page that is almost empty. The process that
// transfers indexes from the buffer to pages is called compaction.
// This store never collapses or merges bins, therefore, it does not introduce
// any error in itself. In particular, MinIndex(), MaxIndex(), Bins() and
// KeyAtRank() return exact results.
// There is no upper bound on the memory size that this store needs to encode
// input indexes, and some input data distributions may make it reach large
// sizes. However, thanks to the buffer and the fact that only required pages
// are allocated, it can be much more space efficient than alternative stores,
// especially dense stores, in various situations, including when only few
// indexes are added (with their counts equal to 1), when the input data has a
// few outliers or when the input data distribution is multimodal.
type BufferedPaginatedStore struct {
	buffer                  buffer
	lastCompactionBufferLen int

	pages        [][]float64
	minPageIndex int // minPageIndex == maxInt iff pages are unused (they may still be allocated)
	pageLenLog2  uint8
	pageLenMask  int

	memory *memoryPool
}

func NewBufferedPaginatedStore() *BufferedPaginatedStore {
	pageSizeLog2 := defaultPageSizeLog2
	pageLenLog2 := pageSizeLog2 - countSizeLog2

	memory := newUnlimitedMemoryPool(pageSizeLog2)

	return &BufferedPaginatedStore{
		buffer:                  newBuffer(memory),
		lastCompactionBufferLen: 0,
		pages:                   nil,
		minPageIndex:            maxInt,
		pageLenLog2:             pageLenLog2,
		pageLenMask:             1<<pageLenLog2 - 1,
		memory:                  memory,
	}
}

// pageIndex returns the page number the given index falls on.
func (s *BufferedPaginatedStore) pageIndex(index int) int {
	return index >> s.pageLenLog2
}

// lineIndex returns the line number within a page that the given index falls on.
func (s *BufferedPaginatedStore) lineIndex(index int) int {
	return index & s.pageLenMask
}

// index returns the store-level index for a given page number and a line within that page.
func (s *BufferedPaginatedStore) index(pageIndex, lineIndex int) int {
	return pageIndex<<s.pageLenLog2 + lineIndex
}

// existingPage returns the page for the provided pageIndex if the page exists,
// or a slice with len == 0 otherwise (possibly nil slice).
func (s *BufferedPaginatedStore) existingPage(pageIndex int) []float64 {
	if pageIndex >= s.minPageIndex && pageIndex < s.minPageIndex+len(s.pages) {
		// No need to extend s.pages.
		return s.pages[pageIndex-s.minPageIndex]
	} else {
		return nil
	}
}

// page returns the page for the provided pageIndex, or nil. When unexisting,
// the page is created if and only if ensureExists is true.
func (s *BufferedPaginatedStore) page(pageIndex int, ensureExists bool) []float64 {
	if page := s.existingPage(pageIndex); !ensureExists || page != nil {
		return page
	}

	// Figure out the new length of s.pages and if existing pages should be shifted in s.pages.
	newPagesLen := len(s.pages)
	shift := 0
	if s.minPageIndex == maxInt {
		// len(s.pages) == 0
		newPagesLen = 1
	} else if pageIndex < s.minPageIndex {
		// Extends s.pages left.
		shift = s.minPageIndex - pageIndex
		newPagesLen = shift + len(s.pages)
	} else if pageIndex >= s.minPageIndex+len(s.pages) {
		// Extends s.pages right.
		newPagesLen = pageIndex - s.minPageIndex + 1
	}

	// Update the length and the capacity of s.pages.
	if newPagesLen > cap(s.pages) {
		newPages := make([][]float64, newPagesLen, sliceCap(newPagesLen))
		copy(newPages[shift:], s.pages)
		s.pages = newPages
	} else {
		s.pages = s.pages[:newPagesLen]
		if shift > 0 {
			copy(s.pages[shift:], s.pages)
			for i := range s.pages[:shift] {
				s.pages[i] = nil
			}
		}
	}

	// Update minPageIndex.
	if s.minPageIndex == maxInt {
		s.minPageIndex = pageIndex
	} else {
		s.minPageIndex -= shift
	}

	newPage := bytesToFloat64Slice(s.memory.acquire())
	s.pages[pageIndex-s.minPageIndex] = newPage
	return newPage
}

func sliceCap(len int) int {
	// Grow in size by multiples of 64 bytes
	increment := 64 >> ptrSizeLog2
	return (len + increment - 1) & -increment
}

func (s *BufferedPaginatedStore) compact() {
	s.buffer.compact(s.pageIndex, func(countPageIndex, sizeInBuffer int) func(int) {
		// We avoid creating a new page if it would take more memory space than
		// what we would free in the buffer. Note that even when the page itself
		// takes less memory space than the buffered indexes that can be encoded
		// in the page, because we may have to extend s.pages, the store may end
		// up larger. However, for the sake of simplicity, we ignore the length
		// of s.pages.
		ensureExists := (1<<s.pageLenLog2)<<countSizeLog2 <= sizeInBuffer
		page := s.page(countPageIndex, ensureExists)
		if page == nil {
			return nil
		} else {
			return func(i int) {
				page[s.lineIndex(i)]++
			}
		}
	})
	s.lastCompactionBufferLen = s.buffer.Len()
}

func (s *BufferedPaginatedStore) Add(index int) {
	// First, check if it can be encoded in an existing page.
	if page := s.existingPage(s.pageIndex(index)); page != nil {
		page[s.lineIndex(index)]++
		return
	}

	// The page does not exist, use the buffer.
	if s.buffer.tryAdding(index) {
		return
	}

	if s.buffer.Len() >= s.lastCompactionBufferLen+2*(1<<s.buffer.pageLenLog2) {
		s.compact()
	}

	if page := s.existingPage(s.pageIndex(index)); page != nil {
		page[s.lineIndex(index)]++
		return
	}

	s.buffer.add(index)
}

func (s *BufferedPaginatedStore) AddBin(bin Bin) {
	s.AddWithCount(bin.Index(), bin.Count())
}

func (s *BufferedPaginatedStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	} else if count == 1 {
		s.Add(index)
	} else {
		s.page(s.pageIndex(index), true)[s.lineIndex(index)] += count
	}
}

func (s *BufferedPaginatedStore) IsEmpty() bool {
	if !s.buffer.empty() {
		return false
	}
	for _, page := range s.pages {
		for _, count := range page {
			if count > 0 {
				return false
			}
		}
	}
	return true
}

func (s *BufferedPaginatedStore) TotalCount() float64 {
	totalCount := float64(s.buffer.Len())
	for _, page := range s.pages {
		for _, count := range page {
			totalCount += count
		}
	}
	return totalCount
}

func (s *BufferedPaginatedStore) MinIndex() (minIndex int, err error) {
	err = errUndefinedMinIndex
	s.forEachOrdered(func(index int, count float64) (stop bool) {
		minIndex = index
		err = nil
		return true
	}, false)
	return
}

func (s *BufferedPaginatedStore) MaxIndex() (maxIndex int, err error) {
	err = errUndefinedMaxIndex
	s.forEachOrdered(func(index int, count float64) (stop bool) {
		maxIndex = index
		err = nil
		return true
	}, true)
	return

}

func (s *BufferedPaginatedStore) KeyAtRank(rank float64) int {
	if rank < 0 {
		rank = 0
	}
	key, err := s.minIndexWithCumulCount(func(cumulCount float64) bool {
		return cumulCount > rank
	})

	if err != nil {
		maxIndex, err := s.MaxIndex()
		if err == nil {
			return maxIndex
		} else {
			// FIXME: make Store's KeyAtRank consistent with MinIndex and MaxIndex
			return 0
		}
	}
	return key
}

// minIndexWithCumulCount returns the minimum index whose cumulative count (that
// is, the sum of the counts associated with the indexes less than or equal to
// the index) verifies the predicate.
func (s *BufferedPaginatedStore) minIndexWithCumulCount(predicate func(float64) bool) (minIndex int, err error) {
	err = errUnverifiedPredicate
	cumulCount := float64(0)
	s.forEachOrdered(func(index int, count float64) (stop bool) {
		minIndex = index
		cumulCount += count
		if predicate(cumulCount) {
			err = nil
			return true
		}
		return false
	}, false)
	return
}

func (s *BufferedPaginatedStore) MergeWith(other Store) {
	if o, ok := other.(*BufferedPaginatedStore); ok {
		o.buffer.forEach(func(oIndex int) (stop bool) {
			s.Add(oIndex)
			return false
		})
		for oPageOffset, oPage := range o.pages {
			sPage := s.page(o.minPageIndex+oPageOffset, true)
			for lineIndex := range oPage {
				sPage[lineIndex] += oPage[lineIndex]
			}
		}
	} else {
		other.ForEach(func(index int, count float64) (stop bool) {
			s.AddWithCount(index, count)
			return false
		})
	}
}

func (s *BufferedPaginatedStore) MergeWithProto(pb *sketchpb.Store) {
	for index, count := range pb.BinCounts {
		s.AddWithCount(int(index), count)
	}
	for indexOffset, count := range pb.ContiguousBinCounts {
		s.AddWithCount(int(pb.ContiguousBinIndexOffset)+indexOffset, count)
	}
}

func (s *BufferedPaginatedStore) Bins() <-chan Bin {
	s.buffer.sort()
	bufferIt := s.buffer.iterator(false)
	bufferNext := bufferIt.next()
	ch := make(chan Bin)
	go func() {
		defer close(ch)

		// Iterate over the pages and the buffer simultaneously.
		for pageOffset, page := range s.pages {
			for lineIndex, count := range page {
				if count == 0 {
					continue
				}

				index := s.index(s.minPageIndex+pageOffset, lineIndex)

				// Iterate over the buffer until index is reached.
				var bufferCount int
				for {
					if bufferNext == nil || *bufferNext > index {
						break
					}
					bufferIndex := *bufferNext
					bufferNext, bufferCount = bufferIt.nextVerifies(func(i int) bool { return i != bufferIndex })
					if bufferIndex == index {
						break
					}
					ch <- Bin{index: bufferIndex, count: float64(bufferCount)}
					bufferCount = 0
				}
				ch <- Bin{index: index, count: count + float64(bufferCount)}
			}
		}

		// Iterate over the rest of the buffer.
		for bufferNext != nil {
			bufferIndex := *bufferNext
			var bufferCount int
			bufferNext, bufferCount = bufferIt.nextVerifies(func(i int) bool { return i != bufferIndex })
			ch <- Bin{index: bufferIndex, count: float64(bufferCount)}
		}
	}()
	return ch
}

func (s *BufferedPaginatedStore) ForEach(f func(index int, count float64) (stop bool)) {
	s.forEachOrdered(f, false)
}

func (s *BufferedPaginatedStore) forEachOrdered(f func(index int, count float64) (stop bool), descending bool) {
	s.buffer.sort()
	bufferIt := s.buffer.iterator(descending)
	bufferItNext := bufferIt.next()

	inc := 1
	pageOffsetFrom, pageOffsetTo := 0, len(s.pages)
	lineIndexFrom, lineIndexTo := 0, 1<<s.pageLenLog2
	if descending {
		inc *= -1
		pageOffsetFrom, pageOffsetTo = pageOffsetTo-1, pageOffsetFrom-1
		lineIndexFrom, lineIndexTo = lineIndexTo-1, lineIndexFrom-1
	}

	// Iterate over the pages and the buffer simultaneously.
	for pageOffset := pageOffsetFrom; pageOffset != pageOffsetTo; pageOffset += inc {
		page := s.pages[pageOffset]
		if page == nil {
			continue
		}
		for lineIndex := lineIndexFrom; lineIndex != lineIndexTo; lineIndex += inc {
			count := page[lineIndex]
			if count == 0 {
				continue
			}

			index := s.index(s.minPageIndex+pageOffset, lineIndex)

			// Iterate over the buffer until index is reached.
			var bufferCount int
			for {
				if bufferItNext == nil || *bufferItNext*inc > index*inc {
					break
				}
				bufferIndex := *bufferItNext
				bufferItNext, bufferCount = bufferIt.nextVerifies(func(i int) bool { return i != bufferIndex })
				if bufferIndex == index {
					break
				}
				if f(bufferIndex, float64(bufferCount)) {
					return
				}
				bufferCount = 0
			}
			if f(index, count+float64(bufferCount)) {
				return
			}
		}
	}

	// Iterate over the rest of the buffer.
	for bufferItNext != nil {
		bufferIndex := *bufferItNext
		var bufferCount int
		bufferItNext, bufferCount = bufferIt.nextVerifies(func(i int) bool { return i != bufferIndex })
		if f(bufferIndex, float64(bufferCount)) {
			return
		}
	}
}

func (s *BufferedPaginatedStore) Copy() Store {
	pagesCopy := make([][]float64, len(s.pages), sliceCap(len(s.pages)))
	for i, page := range s.pages {
		if page != nil {
			pageCopy := bytesToFloat64Slice(s.memory.acquire())
			copy(pageCopy, page)
			pagesCopy[i] = pageCopy
		}
	}
	return &BufferedPaginatedStore{
		buffer:                  s.buffer.copy(),
		lastCompactionBufferLen: s.lastCompactionBufferLen,
		pages:                   pagesCopy,
		minPageIndex:            s.minPageIndex,
		pageLenLog2:             s.pageLenLog2,
		pageLenMask:             s.pageLenMask,
		memory:                  s.memory,
	}
}

func (s *BufferedPaginatedStore) Clear() {
	s.buffer.trim(0)
	// Give pages back to the memory pool.
	for i, page := range s.pages {
		if page != nil {
			s.pages[i] = nil
			s.memory.release(float64SliceToBytes(page))
		}
	}
	// Trim s.pages.
	s.pages = s.pages[:0:min(cap(s.pages), minSliceCap)]
	s.minPageIndex = maxInt
}

func (s *BufferedPaginatedStore) ToProto() *sketchpb.Store {
	if s.IsEmpty() {
		return &sketchpb.Store{}
	}
	// FIXME: add heuristic to use contiguousBinCounts when cheaper.
	binCounts := make(map[int32]float64)
	for bin := range s.Bins() {
		binCounts[int32(bin.index)] = bin.count
	}
	return &sketchpb.Store{
		BinCounts: binCounts,
	}
}

func (s *BufferedPaginatedStore) Reweight(w float64) error {
	if w <= 0 {
		return errors.New("can't reweight by a negative factor")
	}
	if w == 1 {
		return nil
	}
	for _, p := range s.pages {
		for i := range p {
			p[i] *= w
		}
	}
	s.buffer.forEach(func(index int) (stop bool) {
		// Does not affect the buffer because w != 1.
		s.AddWithCount(index, w)
		return false
	})
	s.buffer.trim(0)
	return nil
}

func (s *BufferedPaginatedStore) Encode(b *[]byte, t enc.FlagType) {
	if s.buffer.Len() > 0 {
		enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingIndexDeltas))
		enc.EncodeUvarint64(b, uint64(s.buffer.Len()))
		previousIndex := 0
		s.buffer.forEach(func(index int) (stop bool) {
			enc.EncodeVarint64(b, int64(index-previousIndex))
			previousIndex = index
			return false
		})
	}

	for pageOffset, page := range s.pages {
		if page != nil {
			enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingContiguousCounts))
			enc.EncodeUvarint64(b, uint64(len(page)))
			enc.EncodeVarint64(b, int64(s.index(s.minPageIndex+pageOffset, 0)))
			enc.EncodeVarint64(b, 1)
			for _, count := range page {
				enc.EncodeVarfloat64(b, count)
			}
		}
	}
}

func (s *BufferedPaginatedStore) DecodeAndMergeWith(b *[]byte, encodingMode enc.SubFlag) error {
	switch encodingMode {

	case enc.BinEncodingIndexDeltas:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		remaining := int(numBins)
		index := int64(0)
		for ; remaining > 0; remaining-- {
			indexDelta, err := enc.DecodeVarint64(b)
			if err != nil {
				return err
			}
			index += indexDelta
			s.Add(int(index))
		}
		return nil

	case enc.BinEncodingContiguousCounts:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		indexOffset, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		indexDelta, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		pageLen := 1 << s.pageLenLog2
		for i := uint64(0); i < numBins; {
			page := s.page(s.pageIndex(int(indexOffset)), true)
			lineIndex := s.lineIndex(int(indexOffset))
			for lineIndex >= 0 && lineIndex < pageLen && i < numBins {
				count, err := enc.DecodeVarfloat64(b)
				if err != nil {
					return err
				}
				page[lineIndex] += count
				lineIndex += int(indexDelta)
				indexOffset += indexDelta
				i++
			}
		}
		return nil

	default:
		return DecodeAndMergeWith(s, b, encodingMode)
	}
}

var _ Store = (*BufferedPaginatedStore)(nil)

// Float64SliceToBytes converts a []float64 to []byte unsafely.
func float64SliceToBytes(s []float64) []byte {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&s))

	var b []byte
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data = sh.Data
	bh.Len = sh.Len << float64sizeLog2
	bh.Cap = sh.Cap << float64sizeLog2
	return b
}

// BytesToUint64Slice converts a []byte to []uint64 unsafely.
func bytesToFloat64Slice(b []byte) []float64 {
	if len(b)&(1<<float64sizeLog2-1) != 0 || cap(b)&(1<<float64sizeLog2-1) != 0 {
		panic("cannot cast: invalid len or cap")
	}
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var s []float64
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	sh.Data = bh.Data
	sh.Len = bh.Len >> float64sizeLog2
	sh.Cap = bh.Cap >> float64sizeLog2
	return s
}
