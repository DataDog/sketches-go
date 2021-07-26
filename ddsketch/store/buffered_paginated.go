// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"errors"
	"reflect"
	"sort"

	enc "github.com/DataDog/sketches-go/ddsketch/encoding"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

const (
	ptrSize         = 32 << (^uintptr(0) >> 63)
	intSize         = 32 << (^uint(0) >> 63)
	float64size     = 64
	bufferEntrySize = intSize
	countSize       = float64size

	defaultPageLenLog2 = 5 // pageLen = 32
)

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
	buffer                     []int // FIXME: in practice, int32 (even int16, depending on the accuracy parameter) is enough
	bufferCompactionTriggerLen int   // compaction happens only after this buffer length is reached

	pages           [][]float64
	minPageIndex    int // minPageIndex == maxInt iff pages are unused (they may still be allocated)
	pageLenLog2     int
	pageLenMask     int
	emptyPageMinPos int // any already allocated empty page has a position in pages that is greater than or equal to emptyPageMinPos
}

func NewBufferedPaginatedStore() *BufferedPaginatedStore {
	pageLenLog2 := defaultPageLenLog2
	pageLen := 1 << pageLenLog2

	return &BufferedPaginatedStore{
		buffer:                     nil,
		bufferCompactionTriggerLen: 2 * pageLen,
		pages:                      nil,
		minPageIndex:               maxInt,
		pageLenLog2:                pageLenLog2,
		pageLenMask:                pageLen - 1,
		emptyPageMinPos:            0,
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

// page returns the page for the provided pageIndex, or nil. When unexisting,
// the page is created if and only if ensureExists is true.
func (s *BufferedPaginatedStore) page(pageIndex int, ensureExists bool) []float64 {
	if pageIndex >= s.minPageIndex && pageIndex < s.minPageIndex+len(s.pages) {
		// No need to extend s.pages.
		i := pageIndex - s.minPageIndex
		if ensureExists {
			s.makePage(i)
		}
		return s.pages[i]
	}

	if !ensureExists {
		return nil
	}

	if pageIndex < s.minPageIndex {
		if s.minPageIndex == maxInt {
			if len(s.pages) == 0 {
				s.pages = append(s.pages, make([][]float64, s.newPagesCap(1))...)[:1]
			}
			s.minPageIndex = pageIndex - len(s.pages)/2
		} else {
			// Extends s.pages left.
			requiredPagesLen := s.minPageIndex - pageIndex + 1 + len(s.pages)
			addedPagesLen := requiredPagesLen - len(s.pages)
			if requiredPagesLen > cap(s.pages) {
				s.pages = append(s.pages, make([][]float64, s.newPagesCap(requiredPagesLen)-len(s.pages))...)
			}
			s.pages = s.pages[:requiredPagesLen]
			copy(s.pages[addedPagesLen:], s.pages)
			for i := 0; i < addedPagesLen; i++ {
				s.pages[i] = nil
			}
			s.minPageIndex -= addedPagesLen
			s.emptyPageMinPos += addedPagesLen
		}
	} else {
		// Extends s.pages right.
		requiredPagesLen := pageIndex - s.minPageIndex + 1
		if requiredPagesLen > cap(s.pages) {
			s.pages = append(s.pages, make([][]float64, s.newPagesCap(requiredPagesLen)-len(s.pages))...)
		}
		s.pages = s.pages[:requiredPagesLen]
	}

	i := pageIndex - s.minPageIndex
	s.makePage(i)
	return s.pages[i]
}

func (s *BufferedPaginatedStore) makePage(i int) {
	pageLen := 1 << s.pageLenLog2

	if len(s.pages[i]) > 0 {
		// No need to do anything, the page exists.
		return
	}
	if s.pages[i] != nil {
		// The page is empty (but allocated).
		s.pages[i] = append(s.pages[i], make([]float64, pageLen)...)
		return
	}

	// Look for an already allocated page (see Clear()).
	for ; s.emptyPageMinPos < len(s.pages); s.emptyPageMinPos++ {
		if s.pages[s.emptyPageMinPos] != nil && len(s.pages[s.emptyPageMinPos]) == 0 {
			s.pages[i] = append(s.pages[s.emptyPageMinPos], make([]float64, pageLen)...)
			// We know that s.emptyPageMinPos != i.
			s.pages[s.emptyPageMinPos] = nil
			s.emptyPageMinPos++
			return
		}
	}

	// Allocate a new page.
	s.pages[i] = make([]float64, pageLen)
}

func (s *BufferedPaginatedStore) newPagesCap(required int) int {
	// Grow in size by multiples of 64 bytes
	pageGrowthIncrement := 64 * 8 / ptrSize
	return (required + pageGrowthIncrement - 1) & -pageGrowthIncrement
}

// compact transfers indexes from the buffer to the pages. It only creates new
// pages if they can encode enough buffered indexes so that it frees more space
// in the buffer than the new page takes.
func (s *BufferedPaginatedStore) compact() {
	pageLen := 1 << s.pageLenLog2

	s.sortBuffer()

	for bufferPos := 0; bufferPos < len(s.buffer); {
		bufferPageStart := bufferPos
		pageIndex := s.pageIndex(s.buffer[bufferPageStart])
		bufferPos++
		for bufferPos < len(s.buffer) && s.pageIndex(s.buffer[bufferPos]) == pageIndex {
			bufferPos++
		}
		bufferPageEnd := bufferPos

		// We avoid creating a new page if it would take more memory space than
		// what we would free in the buffer. Note that even when the page itself
		// takes less memory space than the buffered indexes that can be encoded
		// in the page, because we may have to extend s.pages, the store may end
		// up larger. However, for the sake of simplicity, we ignore the length
		// of s.pages.
		ensureExists := (bufferPageEnd-bufferPageStart)*bufferEntrySize >= pageLen*float64size
		newPage := s.page(pageIndex, ensureExists)
		if len(newPage) > 0 {
			for _, index := range s.buffer[bufferPageStart:bufferPageEnd] {
				newPage[s.lineIndex(index)]++
			}
			copy(s.buffer[bufferPageStart:], s.buffer[bufferPageEnd:])
			s.buffer = s.buffer[:len(s.buffer)+bufferPageStart-bufferPageEnd]
			bufferPos = bufferPageStart
		}
	}

	s.bufferCompactionTriggerLen = len(s.buffer) + pageLen
}

func (s *BufferedPaginatedStore) sortBuffer() {
	sort.Slice(s.buffer, func(i, j int) bool { return s.buffer[i] < s.buffer[j] })
}

func (s *BufferedPaginatedStore) Add(index int) {
	pageIndex := s.pageIndex(index)
	if pageIndex >= s.minPageIndex && pageIndex < s.minPageIndex+len(s.pages) {
		page := s.pages[pageIndex-s.minPageIndex]
		if len(page) > 0 {
			page[s.lineIndex(index)]++
			return
		}
	}

	// The page does not exist, use the buffer.
	if len(s.buffer) == cap(s.buffer) && len(s.buffer) >= s.bufferCompactionTriggerLen {
		s.compact()
	}

	s.buffer = append(s.buffer, index)
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
	if len(s.buffer) > 0 {
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
	totalCount := float64(len(s.buffer))
	for _, page := range s.pages {
		for _, count := range page {
			totalCount += count
		}
	}
	return totalCount
}

func (s *BufferedPaginatedStore) MinIndex() (int, error) {
	isEmpty := true

	// Iterate over the buffer.
	var minIndex int
	for _, index := range s.buffer {
		if isEmpty || index < minIndex {
			isEmpty = false
			minIndex = index
		}
	}

	// Iterate over the pages.
	for pageIndex := s.minPageIndex; pageIndex < s.minPageIndex+len(s.pages) && (isEmpty || pageIndex <= s.pageIndex(minIndex)); pageIndex++ {
		page := s.pages[pageIndex-s.minPageIndex]
		if len(page) == 0 {
			continue
		}

		var lineIndexRangeEnd int
		if !isEmpty && pageIndex == s.pageIndex(minIndex) {
			lineIndexRangeEnd = s.lineIndex(minIndex)
		} else {
			lineIndexRangeEnd = 1 << s.pageLenLog2
		}

		for lineIndex := 0; lineIndex < lineIndexRangeEnd; lineIndex++ {
			if page[lineIndex] > 0 {
				return s.index(pageIndex, lineIndex), nil
			}
		}
	}

	if isEmpty {
		return 0, errUndefinedMinIndex
	} else {
		return minIndex, nil
	}
}

func (s *BufferedPaginatedStore) MaxIndex() (int, error) {
	isEmpty := true

	// Iterate over the buffer.
	var maxIndex int
	for _, index := range s.buffer {
		if isEmpty || index > maxIndex {
			isEmpty = false
			maxIndex = index
		}
	}

	// Iterate over the pages.
	for pageIndex := s.minPageIndex + len(s.pages) - 1; pageIndex >= s.minPageIndex && (isEmpty || pageIndex >= s.pageIndex(maxIndex)); pageIndex-- {
		page := s.pages[pageIndex-s.minPageIndex]
		if len(page) == 0 {
			continue
		}

		var lineIndexRangeStart int
		if !isEmpty && pageIndex == s.pageIndex(maxIndex) {
			lineIndexRangeStart = s.lineIndex(maxIndex)
		} else {
			lineIndexRangeStart = 0
		}

		for lineIndex := len(page) - 1; lineIndex >= lineIndexRangeStart; lineIndex-- {
			if page[lineIndex] > 0 {
				return s.index(pageIndex, lineIndex), nil
			}
		}
	}

	if isEmpty {
		return 0, errUndefinedMaxIndex
	} else {
		return maxIndex, nil
	}
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
func (s *BufferedPaginatedStore) minIndexWithCumulCount(predicate func(float64) bool) (int, error) {
	s.sortBuffer()
	cumulCount := float64(0)

	// Iterate over the pages and the buffer simultaneously.
	bufferPos := 0
	for pageOffset, page := range s.pages {
		for lineIndex, count := range page {
			index := s.index(s.minPageIndex+pageOffset, lineIndex)

			// Iterate over the buffer until index is reached.
			for ; bufferPos < len(s.buffer) && s.buffer[bufferPos] < index; bufferPos++ {
				cumulCount++
				if predicate(cumulCount) {
					return s.buffer[bufferPos], nil
				}
			}
			cumulCount += count
			if predicate(cumulCount) {
				return index, nil
			}
		}
	}

	// Iterate over the rest of the buffer
	for ; bufferPos < len(s.buffer); bufferPos++ {
		cumulCount++
		if predicate(cumulCount) {
			return s.buffer[bufferPos], nil
		}
	}

	return 0, errors.New("the predicate on the cumulative count is never verified")
}

func (s *BufferedPaginatedStore) MergeWith(other Store) {
	o, ok := other.(*BufferedPaginatedStore)
	if ok && len(o.pages) == 0 {
		// Optimized merging if the other store only has buffered data.
		oBufferOffset := 0
		for {
			bufferCapOverhead := max(cap(s.buffer), s.bufferCompactionTriggerLen) - len(s.buffer)
			if bufferCapOverhead >= len(o.buffer)-oBufferOffset {
				s.buffer = append(s.buffer, o.buffer[oBufferOffset:]...)
				return
			}
			s.buffer = append(s.buffer, o.buffer[oBufferOffset:oBufferOffset+bufferCapOverhead]...)
			oBufferOffset += bufferCapOverhead
			s.compact()
		}
	}

	// Fallback merging.
	for bin := range other.Bins() {
		s.AddBin(bin)
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
	s.sortBuffer()
	ch := make(chan Bin)
	go func() {
		defer close(ch)
		bufferPos := 0

		// Iterate over the pages and the buffer simultaneously.
		for pageOffset, page := range s.pages {
			for lineIndex, count := range page {
				if count == 0 {
					continue
				}

				index := s.index(s.minPageIndex+pageOffset, lineIndex)

				// Iterate over the buffer until index is reached.
				var indexBufferStartPos int
				for {
					indexBufferStartPos = bufferPos
					if indexBufferStartPos >= len(s.buffer) || s.buffer[indexBufferStartPos] > index {
						break
					}
					bufferPos++
					for bufferPos < len(s.buffer) && s.buffer[bufferPos] == s.buffer[indexBufferStartPos] {
						bufferPos++
					}
					if s.buffer[indexBufferStartPos] == index {
						break
					}
					ch <- Bin{index: s.buffer[indexBufferStartPos], count: float64(bufferPos - indexBufferStartPos)}
				}
				ch <- Bin{index: index, count: count + float64(bufferPos-indexBufferStartPos)}
			}
		}

		// Iterate over the rest of the buffer.
		for bufferPos < len(s.buffer) {
			indexBufferStartPos := bufferPos
			bufferPos++
			for bufferPos < len(s.buffer) && s.buffer[bufferPos] == s.buffer[indexBufferStartPos] {
				bufferPos++
			}
			bin := Bin{index: s.buffer[indexBufferStartPos], count: float64(bufferPos - indexBufferStartPos)}
			ch <- bin
		}
	}()
	return ch
}

func (s *BufferedPaginatedStore) ForEach(f func(b Bin) (stop bool)) {
	s.sortBuffer()
	bufferPos := 0

	// Iterate over the pages and the buffer simultaneously.
	for pageOffset, page := range s.pages {
		for lineIndex, count := range page {
			if count == 0 {
				continue
			}

			index := s.index(s.minPageIndex+pageOffset, lineIndex)

			// Iterate over the buffer until index is reached.
			var indexBufferStartPos int
			for {
				indexBufferStartPos = bufferPos
				if indexBufferStartPos >= len(s.buffer) || s.buffer[indexBufferStartPos] > index {
					break
				}
				bufferPos++
				for bufferPos < len(s.buffer) && s.buffer[bufferPos] == s.buffer[indexBufferStartPos] {
					bufferPos++
				}
				if s.buffer[indexBufferStartPos] == index {
					break
				}
				if f(Bin{index: s.buffer[indexBufferStartPos], count: float64(bufferPos - indexBufferStartPos)}) {
					return
				}
			}
			if f(Bin{index: index, count: count + float64(bufferPos-indexBufferStartPos)}) {
				return
			}
		}
	}

	// Iterate over the rest of the buffer.
	for bufferPos < len(s.buffer) {
		indexBufferStartPos := bufferPos
		bufferPos++
		for bufferPos < len(s.buffer) && s.buffer[bufferPos] == s.buffer[indexBufferStartPos] {
			bufferPos++
		}
		if f(Bin{index: s.buffer[indexBufferStartPos], count: float64(bufferPos - indexBufferStartPos)}) {
			return
		}
	}
}

func (s *BufferedPaginatedStore) Copy() Store {
	bufferCopy := make([]int, len(s.buffer))
	copy(bufferCopy, s.buffer)
	pagesCopy := make([][]float64, len(s.pages))
	for i, page := range s.pages {
		if len(page) > 0 {
			pageCopy := make([]float64, len(page))
			copy(pageCopy, page)
			pagesCopy[i] = pageCopy
		}
	}
	return &BufferedPaginatedStore{
		buffer:                     bufferCopy,
		bufferCompactionTriggerLen: s.bufferCompactionTriggerLen,
		pages:                      pagesCopy,
		minPageIndex:               s.minPageIndex,
		pageLenLog2:                s.pageLenLog2,
		pageLenMask:                s.pageLenMask,
		emptyPageMinPos:            len(pagesCopy),
	}
}

func (s *BufferedPaginatedStore) Clear() {
	s.buffer = s.buffer[:0]
	// Empty pages and move them to the head of s.pages so as to reuse already
	// allocated memory.
	j := 0
	for i, page := range s.pages {
		if page != nil {
			s.pages[i] = nil
			s.pages[j] = page[:0]
			j++
		}
	}
	// Trim s.pages; only keep enough to track empty pages.
	s.pages = s.pages[:j]
	s.emptyPageMinPos = 0
	s.minPageIndex = maxInt
}

func (s *BufferedPaginatedStore) memorySize() (size uint) {
	size += uint(reflect.TypeOf(s).Elem().Size())
	size += uint(cap(s.buffer)) * uint(reflect.TypeOf(s.buffer).Elem().Size())
	size += uint(cap(s.pages)) * uint(reflect.TypeOf(s.pages).Elem().Size())
	for _, page := range s.pages {
		size += uint(cap(page)) * uint(reflect.TypeOf(page).Elem().Size())
	}
	return
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
	buffer := s.buffer
	s.buffer = s.buffer[:0]
	for _, p := range s.pages {
		for i := range p {
			p[i] *= w
		}
	}
	for _, index := range buffer {
		s.AddWithCount(index, w)
	}
	return nil
}

func (s *BufferedPaginatedStore) Encode(b *[]byte, t enc.FlagType) {
	if len(s.buffer) > 0 {
		enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingIndexDeltas))
		enc.EncodeUvarint64(b, uint64(len(s.buffer)))
		previousIndex := 0
		for _, index := range s.buffer {
			enc.EncodeVarint64(b, int64(index-previousIndex))
			previousIndex = index
		}
	}

	for pageOffset, page := range s.pages {
		if len(page) > 0 {
			enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingContiguousCounts))
			enc.EncodeUvarint64(b, uint64(len(page)))
			enc.EncodeVarint64(b, int64(s.index(s.minPageIndex+pageOffset, 0)))
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
		// Process indexes in batches to avoid checking after each insertion
		// whether compaction should happen.
		for {
			batchSize := min(remaining, max(cap(s.buffer), s.bufferCompactionTriggerLen)-len(s.buffer))
			for i := 0; i < batchSize; i++ {
				indexDelta, err := enc.DecodeVarint64(b)
				if err != nil {
					return err
				}
				index += indexDelta
				s.buffer = append(s.buffer, int(index))
			}
			remaining -= batchSize
			if remaining == 0 {
				return nil
			}
			s.compact()
		}

	case enc.BinEncodingContiguousCounts:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		indexOffset, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		pageLen := 1 << s.pageLenLog2
		pageIndex := s.pageIndex(int(indexOffset))
		lineIndex := s.lineIndex(int(indexOffset))
		for i := uint64(0); i < numBins; {
			page := s.page(pageIndex, true)
			for lineIndex < pageLen && i < numBins {
				count, err := enc.DecodeVarfloat64(b)
				if err != nil {
					return err
				}
				page[lineIndex] += count
				lineIndex++
				i++
			}
			pageIndex++
			lineIndex = 0
		}
		return nil

	default:
		return DecodeAndMergeWith(s, b, encodingMode)
	}
}

var _ Store = (*BufferedPaginatedStore)(nil)
