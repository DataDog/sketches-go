package store

import (
	"reflect"
	"sort"
	"unsafe"
)

type buffer struct {
	pages       [][]int // FIXME: in practice, int32 (even int16, depending on the accuracy parameter) is enough
	lastPage    *[]int
	pageLenLog2 uint8
	pageLenMask int
	sortedLen   int

	memory *memoryPool
}

func newBuffer(memory *memoryPool) buffer {
	pageLenLog2 := memory.blockSizeLog2 - intSizeLog2
	return buffer{
		pages:       nil,
		lastPage:    &[]int{},
		pageLenLog2: pageLenLog2,
		pageLenMask: 1<<pageLenLog2 - 1,
		sortedLen:   0,
		memory:      memory,
	}
}

func (b *buffer) tryAdding(index int) bool {
	if len(*b.lastPage) < cap(*b.lastPage) {
		l := len(*b.lastPage)
		*b.lastPage = (*b.lastPage)[:l+1]
		(*b.lastPage)[l] = index
		return true
	} else {
		return false
	}
}

func (b *buffer) add(index int) {
	if b.tryAdding(index) {
		return
	}
	b.pages = append(b.pages, bytesToIntSlice(b.memory.acquire())[:0])
	b.lastPage = &b.pages[len(b.pages)-1]
	*b.lastPage = (*b.lastPage)[:1]
	(*b.lastPage)[0] = index
}

func (b *buffer) empty() bool {
	return len(b.pages) == 0
}

func (b *buffer) elt(index int) *int {
	// fmt.Printf("index: %d, len: %d, pageLen: %d, firstPageLen: %d, pageIndex: %d, lineIndex: %d\n", index, b.Len(), len(b.pages), len(b.pages[0]), index>>int(b.pageLenLog2), index&b.pageLenMask)
	return &b.pages[index>>b.pageLenLog2][index&b.pageLenMask]
}

func (b *buffer) forEach(f func(index int) (stop bool)) {
	for _, page := range b.pages {
		for _, index := range page {
			if f(index) {
				return
			}
		}
	}
}

func (b *buffer) trim(l int) {
	pageIndex := l >> b.pageLenLog2
	lineIndex := l & b.pageLenMask
	if lineIndex != 0 {
		b.pages[pageIndex] = b.pages[pageIndex][:lineIndex]
		pageIndex++
	}
	trimmedPagesLen := pageIndex
	for ; pageIndex < len(b.pages); pageIndex++ {
		b.memory.release(intSliceToBytes(b.pages[pageIndex]))
		b.pages[pageIndex] = nil
	}
	b.pages = b.pages[:trimmedPagesLen:min(cap(b.pages), sliceCap(trimmedPagesLen))]
	if len(b.pages) == 0 {
		b.lastPage = &[]int{}
	} else {
		b.lastPage = &b.pages[len(b.pages)-1]
	}
	b.sortedLen = min(b.sortedLen, l)
}

func (b *buffer) copy() buffer {
	pagesCopy := make([][]int, len(b.pages), sliceCap(len(b.pages)))
	for i, page := range b.pages {
		pagesCopy[i] = bytesToIntSlice(b.memory.acquire())[:len(page)]
		copy(pagesCopy[i], page)
	}
	return buffer{
		pages:       pagesCopy,
		pageLenLog2: b.pageLenLog2,
		pageLenMask: b.pageLenMask,
		memory:      b.memory,
	}
}

func (b *buffer) Len() int {
	if len(b.pages) == 0 {
		return 0
	} else {
		return (len(b.pages)-1)<<b.pageLenLog2 + len(b.pages[len(b.pages)-1])
	}
}

func (b *buffer) Less(i, j int) bool {
	return *b.elt(i) < *b.elt(j)
}

func (b *buffer) Swap(i, j int) {
	ei := b.elt(i)
	ej := b.elt(j)
	tmp := *ei
	*ei = *ej
	*ej = tmp
}

func (b *buffer) sort() {
	l := b.Len()
	for b.sortedLen < l {
		sortedPagesLen := b.sortedLen >> b.pageLenLog2

		nextPage := b.pages[sortedPagesLen] // partially sorted
		sort.Ints(nextPage)

		if sortedPagesLen == 0 {
			b.sortedLen = len(nextPage)
			continue
		}

		b.pages[sortedPagesLen] = bytesToIntSlice(b.memory.acquire())[:len(nextPage)]
		nextPageLineIndex := len(nextPage) - 1

		readIt := b.offsetIterator(true, sortedPagesLen<<b.pageLenLog2-1)
		writeIt := b.offsetIterator(true, sortedPagesLen<<b.pageLenLog2+len(nextPage)-1)

		readItNext := readIt.next()
	merge:
		for {
			if *readItNext > nextPage[nextPageLineIndex] {
				*writeIt.next() = *readItNext
				readItNext = readIt.next()
				if readItNext == nil {
					copy(b.pages[0], nextPage[:nextPageLineIndex+1])
					break merge
				}
			} else {
				*writeIt.next() = nextPage[nextPageLineIndex]
				nextPageLineIndex--
				if nextPageLineIndex < 0 {
					// No need to move the remaining ones.
					break merge
				}
			}
		}

		b.memory.release(intSliceToBytes(nextPage))
		b.sortedLen = sortedPagesLen<<b.pageLenLog2 + len(nextPage)
	}
}

func (b *buffer) compact(countPageIndex func(int) int, consumer func(countPageIndex int, sizeInBuffer int) func(int)) {
	b.sort()

	readHeadIt, readTailIt, writeIt := b.iterator(false), b.iterator(false), b.iterator(false)
	readHeadItNext := readHeadIt.next()
	for readHeadItNext != nil {
		sectionCountPageIndex := countPageIndex(*readHeadItNext)
		var sectionLen int
		readHeadItNext, sectionLen = readHeadIt.nextVerifies(func(i int) bool { return countPageIndex(i) != sectionCountPageIndex })

		c := consumer(sectionCountPageIndex, sectionLen<<bufferEntrySizeLog2)
		if c != nil {
			for ; sectionLen > 0; sectionLen-- {
				c(*readTailIt.next())
			}
		} else {
			for ; sectionLen > 0; sectionLen-- {
				*writeIt.next() = *readTailIt.next()
			}
		}
	}
	b.trim(writeIt.offset())
}

type bufferIterator struct {
	pageIndex int
	lineIndex int
	inc       int
	curPage   []int
	buffer    *buffer
}

func (b *buffer) iterator(descending bool) bufferIterator {
	offset := 0
	if descending {
		offset = b.Len() - 1
	}
	return b.offsetIterator(descending, offset)
}

func (b *buffer) offsetIterator(descending bool, offset int) bufferIterator {
	inc := 1
	if descending {
		inc = -1
	}
	pageIndex := offset >> b.pageLenLog2
	var curPage []int
	if pageIndex >= 0 && pageIndex < len(b.pages) {
		curPage = b.pages[pageIndex]
	}
	return bufferIterator{
		pageIndex: pageIndex,
		lineIndex: offset & b.pageLenMask,
		curPage:   curPage,
		inc:       inc,
		buffer:    b,
	}
}

func (i *bufferIterator) offset() int { return i.pageIndex<<i.buffer.pageLenLog2 + i.lineIndex }

func (i *bufferIterator) next() (next *int) {
	if i.lineIndex < 0 || i.lineIndex >= len(i.curPage) {
		if i.pageIndex+i.inc < 0 || i.pageIndex+i.inc >= len(i.buffer.pages) {
			return nil
		}
		i.pageIndex += i.inc
		i.curPage = i.buffer.pages[i.pageIndex]
		i.lineIndex = ((1 - i.inc) >> 1) * (1<<i.buffer.pageLenLog2 - 1)
	}
	next = &i.curPage[i.lineIndex]
	i.lineIndex += i.inc
	return
}

func (i *bufferIterator) nextVerifies(predicate func(int) bool) (next *int, count int) {
	for {
		next = i.next()
		count++
		if next == nil || predicate(*next) {
			return
		}
	}
}

// Float64SliceToBytes converts a []float64 to []byte unsafely.
func intSliceToBytes(s []int) []byte {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&s))

	var b []byte
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data = sh.Data
	bh.Len = sh.Len << intSizeLog2
	bh.Cap = sh.Cap << intSizeLog2
	return b
}

// BytesToUint64Slice converts a []byte to []uint64 unsafely.
func bytesToIntSlice(b []byte) []int {
	if len(b)&(1<<intSizeLog2-1) != 0 || cap(b)&(1<<intSizeLog2-1) != 0 {
		panic("cannot cast: invalid len or cap")
	}
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var s []int
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	sh.Data = bh.Data
	sh.Len = bh.Len >> intSizeLog2
	sh.Cap = bh.Cap >> intSizeLog2
	return s
}
