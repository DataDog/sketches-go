// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"errors"
	"math"

	enc "github.com/DataDog/sketches-go/ddsketch/encoding"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
)

type Provider func() Store

var (
	DefaultProvider                   = Provider(BufferedPaginatedStoreConstructor)
	DenseStoreConstructor             = Provider(func() Store { return NewDenseStore() })
	BufferedPaginatedStoreConstructor = Provider(func() Store { return NewBufferedPaginatedStore() })
	SparseStoreConstructor            = Provider(func() Store { return NewSparseStore() })
)

const (
	maxInt = int(^uint(0) >> 1)
	minInt = ^maxInt
)

var (
	errUndefinedMinIndex = errors.New("MinIndex of empty store is undefined")
	errUndefinedMaxIndex = errors.New("MaxIndex of empty store is undefined")
	errInvalidEncoding   = errors.New("invalid store encoding")
)

// MaxDecodeIndexRange bounds the number of indexes (maxIndex − minIndex + 1)
// that a store is allowed to span when decoding an encoded payload. Decoding
// into a dense store allocates a slice covering its whole index range, so an
// unbounded range lets a crafted payload trigger a huge allocation (see issue
// #85); decoding a payload whose bins span a wider range returns an error
// instead.
//
// The default is far larger than the range any realistic sketch produces — even
// one that combines a very wide value range with high relative accuracy stays
// orders of magnitude below it — while keeping the worst-case dense allocation
// well away from memory-exhaustion territory (on the order of half a gigabyte).
// Adjust it (ideally once, at startup) to trade off leniency against the
// maximum allocation a single decode may cause; set it to 0 to disable the
// check entirely and restore fully unbounded decoding.
var MaxDecodeIndexRange uint64 = 1 << 26

type Store interface {
	Add(index int)
	AddBin(bin Bin)
	AddWithCount(index int, count float64)
	// Bins returns a channel that emits the bins that are encoded in the store.
	// Note that this leaks a channel and a goroutine if it is not iterated to completion.
	Bins() <-chan Bin
	// ForEach applies f to all elements of the store or until f returns true.
	ForEach(f func(index int, count float64) (stop bool))
	Copy() Store
	// Clear empties the store while allowing reusing already allocated memory.
	// In some situations, it may be advantageous to clear and reuse a store
	// rather than instantiating a new one. Keeping reusing the same store again
	// and again on varying input data distributions may however ultimately make
	// the store overly large and may waste memory space.
	Clear()
	IsEmpty() bool
	MaxIndex() (int, error)
	MinIndex() (int, error)
	TotalCount() float64
	KeyAtRank(rank float64) int
	MergeWith(store Store)
	ToProto() *sketchpb.Store
	EncodeProto(builder *sketchpb.StoreBuilder)
	// Reweight multiplies all values from the store by w, but keeps the same global distribution.
	Reweight(w float64) error
	// Encode encodes the bins of the store and appends its content to the
	// provided []byte.
	// The provided FlagType indicates whether the store encodes positive or
	// negative values.
	Encode(b *[]byte, t enc.FlagType)
	// DecodeAndMergeWith decodes bins that have been encoded in the format of
	// the provided binEncodingMode and merges them within the receiver store.
	// It updates the provided []byte so that it starts immediately after the
	// encoded bins.
	DecodeAndMergeWith(b *[]byte, binEncodingMode enc.SubFlag) error
}

// FromProto returns an instance of DenseStore that contains the data in the provided protobuf representation.
func FromProto(pb *sketchpb.Store) *DenseStore {
	store := NewDenseStore()
	MergeWithProto(store, pb)
	return store
}

// MergeWithProto merges the distribution in a protobuf Store to an existing store.
// - if called with an empty store, this simply populates the store with the distribution in the protobuf Store.
// - if called with a non-empty store, this has the same outcome as deserializing the protobuf Store, then merging.
func MergeWithProto(store Store, pb *sketchpb.Store) {
	for idx, count := range pb.BinCounts {
		store.AddWithCount(int(idx), count)
	}
	for idx, count := range pb.ContiguousBinCounts {
		store.AddWithCount(idx+int(pb.ContiguousBinIndexOffset), count)
	}
}

// isValidIndex reports whether index is within the range that the index
// mappings can produce. All mappings derive their min/max indexable values so
// that indexes stay within [math.MinInt32, math.MaxInt32], so an index outside
// that range can only come from a corrupt or crafted payload. Rejecting it
// prevents such payloads from making a store (in particular an unbounded dense
// one) allocate an enormous amount of memory. See issue #85.
func isValidIndex(index int64) bool {
	return index >= math.MinInt32 && index <= math.MaxInt32
}

// exceedsMaxDecodeIndexRange reports whether the inclusive index span
// [minIndex, maxIndex] is wider than MaxDecodeIndexRange allows. Both bounds are
// expected to be valid indexes (see isValidIndex), so their difference fits in a
// uint64.
func exceedsMaxDecodeIndexRange(minIndex, maxIndex int64) bool {
	return MaxDecodeIndexRange != 0 && uint64(maxIndex-minIndex) >= MaxDecodeIndexRange
}

// indexRangeTracker tracks the smallest and largest index seen while decoding a
// store's bins and reports when their span grows past MaxDecodeIndexRange. The
// delta-based encodings allow arbitrary, non-monotonic jumps, so the span has to
// be checked incrementally rather than from the block's extremes; doing so lets
// a decoder bail out before a crafted payload makes a dense store allocate a
// slice covering the whole span. The first observed index seeds both bounds.
type indexRangeTracker struct {
	minIndex, maxIndex int64
	seeded             bool
}

// accept records index and reports whether the tracked span still fits within
// MaxDecodeIndexRange. Callers should validate index with isValidIndex first.
func (t *indexRangeTracker) accept(index int64) bool {
	if !t.seeded {
		t.minIndex, t.maxIndex, t.seeded = index, index, true
	} else if index < t.minIndex {
		t.minIndex = index
	} else if index > t.maxIndex {
		t.maxIndex = index
	}
	return !exceedsMaxDecodeIndexRange(t.minIndex, t.maxIndex)
}

// isDecodableContiguousBlock reports whether a contiguous block header is safe
// to decode, the shared admission rule applied by every decoder before reading
// the block's counts. A payload cannot describe more bins than the remaining
// buffer can hold (each bin's count takes at least one byte), and every index
// the block covers must be valid. The indexes are monotonic in the bin number,
// so it is enough to check the two extremes; doing so up front lets a crafted
// block be rejected before it makes a store grow incrementally to an enormous
// size. The span is bounded against the maximum int32 range to keep the
// multiplication overflow-free. See issue #85.
func isDecodableContiguousBlock(remaining int, firstIndex int64, numBins uint64, indexDelta int64) bool {
	if numBins > uint64(remaining) {
		return false
	}
	if numBins == 0 {
		return true
	}
	if !isValidIndex(firstIndex) {
		return false
	}
	const maxInt32Span uint64 = math.MaxInt32 - math.MinInt32
	steps := numBins - 1
	absIndexDelta := uint64(indexDelta)
	if indexDelta < 0 {
		absIndexDelta = -uint64(indexDelta)
	}
	if steps != 0 && absIndexDelta > maxInt32Span/steps {
		// The last index is necessarily outside the int32 range.
		return false
	}
	lastIndex := firstIndex + int64(steps)*indexDelta
	if !isValidIndex(lastIndex) {
		return false
	}
	lo, hi := firstIndex, lastIndex
	if lo > hi {
		lo, hi = hi, lo
	}
	return !exceedsMaxDecodeIndexRange(lo, hi)
}

func DecodeAndMergeWith(s Store, b *[]byte, binEncodingMode enc.SubFlag) error {
	switch binEncodingMode {

	case enc.BinEncodingIndexDeltasAndCounts:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		index := int64(0)
		var indexRange indexRangeTracker
		for i := uint64(0); i < numBins; i++ {
			indexDelta, err := enc.DecodeVarint64(b)
			if err != nil {
				return err
			}
			count, err := enc.DecodeVarfloat64(b)
			if err != nil {
				return err
			}
			index += indexDelta
			if !isValidIndex(index) || !indexRange.accept(index) {
				return errInvalidEncoding
			}
			s.AddWithCount(int(index), count)
		}

	case enc.BinEncodingIndexDeltas:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		index := int64(0)
		var indexRange indexRangeTracker
		for i := uint64(0); i < numBins; i++ {
			indexDelta, err := enc.DecodeVarint64(b)
			if err != nil {
				return err
			}
			index += indexDelta
			if !isValidIndex(index) || !indexRange.accept(index) {
				return errInvalidEncoding
			}
			s.Add(int(index))
		}

	case enc.BinEncodingContiguousCounts:
		numBins, err := enc.DecodeUvarint64(b)
		if err != nil {
			return err
		}
		index, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		indexDelta, err := enc.DecodeVarint64(b)
		if err != nil {
			return err
		}
		if !isDecodableContiguousBlock(len(*b), index, numBins, indexDelta) {
			return errInvalidEncoding
		}
		for i := uint64(0); i < numBins; i++ {
			count, err := enc.DecodeVarfloat64(b)
			if err != nil {
				return err
			}
			s.AddWithCount(int(index), count)
			index += indexDelta
		}

	default:
		return errors.New("unknown bin encoding")
	}
	return nil
}
