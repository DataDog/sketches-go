// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package gk

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
)

const DefaultEpsilon float64 = 0.01

// Entry is an element of the sketch. For the definition of g and delta, see the original paper
// http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf
type Entry struct {
	v     float64 `json:"v"`
	g     uint32  `json:"g"`
	delta uint32  `json:"d"`
}

//Entries is a slice of Entry
type Entries []Entry

func (slice Entries) Len() int           { return len(slice) }
func (slice Entries) Less(i, j int) bool { return slice[i].v < slice[j].v }
func (slice Entries) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }

// GKArray is a version of GK with a buffer for the incoming values.
// epsilon is the accuracy of the sketch.
// Once Merge() is called, the sketch has a 2*epsilon rank-accuracy guarantee.
type GKArray struct {
	epsilon float64
	// the last item of Entries will always be the max inserted value
	entries  Entries   `json:"entries"`
	incoming []float64 `json:"buf"`
	min      float64   `json:"min"`
	max      float64   `json:"max"`
	count    int64     `json:"cnt"`
	sum      float64   `json:"sum"`
}

// NewGKArray allocates a new GKArray summary.
func NewGKArray(epsilon float64) *GKArray {
	return &GKArray{
		epsilon: epsilon,
		// preallocate the incoming array for better insert throughput (5% faster)
		incoming: make([]float64, 0, int(1/epsilon)+1),
		min:      math.Inf(1),
		max:      math.Inf(-1),
	}
}

func NewDefaultGKArray() *GKArray {
	return NewGKArray(DefaultEpsilon)
}

// Add a new value to the summary.
func (s *GKArray) Add(v float64) {
	s.count++
	s.sum += v
	s.incoming = append(s.incoming, v)
	if v < s.min {
		s.min = v
	}
	if v > s.max {
		s.max = v
	}
	if s.count%(int64(1/s.epsilon)+1) == 0 {
		s.compressWithIncoming(nil)
	}
}

// Quantile returns an epsilon estimate of the element at q.
func (s *GKArray) Quantile(q float64) float64 {
	if q < 0 || q > 1 || s.count == 0 {
		return math.NaN()
	}

	if len(s.incoming) > 0 {
		s.compressWithIncoming(nil)
	}

	rank := int64(q*float64(s.count-1) + 1)
	spread := int64(s.epsilon * float64(s.count-1))
	gSum := int64(0)
	i := 0
	for ; i < len(s.entries); i++ {
		gSum += int64(s.entries[i].g)
		if gSum+int64(s.entries[i].delta) > rank+spread {
			break
		}
	}
	if i == 0 {
		return s.min
	}
	return s.entries[i-1].v
}

// Merge another GKArray into this in-place.
//
// Here is one way to merge summaries so that the sketch is one-way mergeable: we extract an epsilon-approximate
// distribution from one of the summaries (o) and we insert this distribution into the other summary (s). More
// specifically, to extract the approximate distribution, we can query for all the quantiles i/(o.count-1) where i
// is between 0 and o.count-1 (included). Then we insert those values into s as usual. This way, when querying a
// quantile from the merged summary, the returned quantile has a rank error from the inserted values that is lower
// than epsilon, but the inserted values, because of the merge process, have a rank error from the actual data that
// is also lower than epsilon, so that the total rank error is bounded by 2*epsilon.
// However, querying and inserting each value as described above has a complexity that is linear in the number of
// values that have been inserted in o rather than in the number of entries in the summary. To tackle this issue, we
// can notice that each of the quantiles that are queried from o is a v of one of the entry of o. Instead of actually
// querying for those quantiles, we can count the number of times each v will be returned (when querying the quantiles
//     i/(o.valcount-1)); we end up with the values n below. Then instead of successively inserting each v n times,
// we can actually directly append them to s.incoming as new entries where g = n. This is possible because the
// values of n will never violate the condition n <= int(s.eps * (s.count+o.count-1)).
func (s *GKArray) Merge(o *GKArray) {
	if o.epsilon != s.epsilon {
		panic("Can't merge two GKArrays with different epsilons!")
	}
	if o.count == 0 {
		return
	}
	if s.count == 0 {
		s.Copy(o)
		return
	}
	o.compressWithIncoming(nil)
	spread := uint32(o.epsilon * float64(o.count-1))

	incomingEntries := make([]Entry, 0, len(o.entries)+1)
	if n := o.entries[0].g + o.entries[0].delta - spread - 1; n > 0 {
		incomingEntries = append(incomingEntries,
			Entry{
				v:     o.min,
				g:     n,
				delta: 0,
			},
		)
	}
	for i := 0; i < len(o.entries)-1; i++ {
		incomingEntries = append(incomingEntries,
			Entry{
				v:     o.entries[i].v,
				g:     o.entries[i+1].g + o.entries[i+1].delta - o.entries[i].delta,
				delta: 0,
			},
		)
	}
	incomingEntries = append(incomingEntries,
		Entry{
			v:     o.entries[len(o.entries)-1].v,
			g:     spread + 1,
			delta: 0,
		},
	)

	s.count += o.count
	s.sum += o.sum
	if o.min < s.min {
		s.min = o.min
	}
	if o.max > s.max {
		s.max = o.max
	}
	s.compressWithIncoming(incomingEntries)
}

func (s *GKArray) Compress() {
	s.compressWithIncoming(nil)
}

// compressWithIncoming merges an optional incomingEntries and incoming buffer into
// entries and compresses.
func (s *GKArray) compressWithIncoming(incomingEntries Entries) {
	if len(s.incoming) > 0 {
		incomingCopy := make([]Entry, len(incomingEntries), len(incomingEntries)+len(s.incoming))
		copy(incomingCopy, incomingEntries)

		incomingEntries = incomingCopy
		for _, v := range s.incoming {
			incomingEntries = append(incomingEntries, Entry{v: v, g: 1, delta: 0})
		}
	}
	sort.Sort(incomingEntries)

	// Copy entries slice so as not to change the original
	entriesCopy := make([]Entry, len(s.entries), len(s.entries))
	copy(entriesCopy, s.entries)
	s.entries = entriesCopy

	removalThreshold := 2 * uint32(s.epsilon*float64(s.count-1))
	merged := make([]Entry, 0, len(s.entries)+len(incomingEntries)/3)

	for i, j := 0, 0; i < len(incomingEntries) || j < len(s.entries); {
		if j == len(s.entries) {
			// done with sketch; now only considering incoming
			if i+1 < len(incomingEntries) &&
				incomingEntries[i].g+incomingEntries[i+1].g <= removalThreshold {
				// removable from incoming
				incomingEntries[i+1].g += incomingEntries[i].g
			} else {
				merged = append(merged, incomingEntries[i])
			}
			i++
		} else if i < len(incomingEntries) && incomingEntries[i].v < s.entries[j].v {
			if incomingEntries[i].g+s.entries[j].g+s.entries[j].delta <= removalThreshold {
				// removable from incoming
				s.entries[j].g += incomingEntries[i].g
			} else {
				incomingEntries[i].delta = s.entries[j].g + s.entries[j].delta - incomingEntries[i].g
				merged = append(merged, incomingEntries[i])
			}
			i++
		} else {
			if j+1 < len(s.entries) &&
				s.entries[j].g+s.entries[j+1].g+s.entries[j+1].delta <= removalThreshold {
				// removable from sketch
				s.entries[j+1].g += s.entries[j].g
			} else {
				merged = append(merged, s.entries[j])
			}
			j++
		}
	}
	s.entries = merged
	// allocate incoming
	s.incoming = make([]float64, 0, int(1/s.epsilon)+1)
}

func (s *GKArray) Sum() float64 {
	return s.sum
}

func (s *GKArray) Avg() float64 {
	return s.sum / float64(s.count)
}

func (s *GKArray) Count() int64 {
	return s.count
}

func (s *GKArray) Copy(o *GKArray) {
	if len(o.entries) == 0 {
		s.entries = nil
	} else {
		s.entries = make([]Entry, len(o.entries))
		copy(s.entries, o.entries)
	}
	s.incoming = append(s.incoming, o.incoming...)
	s.min = o.min
	s.max = o.max
	s.count = o.count
	s.sum = o.sum
}

func (s *GKArray) MakeCopy() *GKArray {
	entries := make([]Entry, len(s.entries))
	copy(entries, s.entries)
	incoming := make([]float64, len(s.incoming), cap(s.incoming))
	copy(incoming, s.incoming)
	return &GKArray{
		epsilon:  s.epsilon,
		min:      s.min,
		max:      s.max,
		count:    s.count,
		sum:      s.sum,
		entries:  entries,
		incoming: incoming,
	}
}

func (s *GKArray) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("count: %d ", s.count))
	buffer.WriteString(fmt.Sprintf("sum: %g ", s.sum))
	buffer.WriteString(fmt.Sprintf("min: %g ", s.min))
	buffer.WriteString(fmt.Sprintf("max: %g ", s.max))
	buffer.WriteString(fmt.Sprintf("entries: {"))
	for i := 0; i < len(s.entries); i++ {
		buffer.WriteString(fmt.Sprintf("[%g, %d, %d], ", s.entries[i].v, s.entries[i].g, s.entries[i].delta))
	}
	buffer.WriteString(fmt.Sprintf("}, incoming: %v\n", s.incoming))
	return buffer.String()
}

func (s *GKArray) MemorySize() int {
	return int(reflect.TypeOf(*s).Size()) +
		cap(s.entries)*int(reflect.TypeOf(s.entries).Elem().Size()) +
		cap(s.incoming)*int(reflect.TypeOf(s.incoming).Elem().Size())
}
