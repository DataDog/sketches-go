package dogsketch

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
)

// DogSketch is a contiguous (non-sparse) implementation of DogSketch.
type DogSketch struct {
	store *Store
	min   float64
	max   float64
	count int64
	sum   float64
	avg   float64
}

// NewDogSketch allocates a new DogSketch summary with relative accuracy alpha.
func NewDogSketch(c *Config) *DogSketch {
	return &DogSketch{
		store: NewStore(c),
		min:   math.Inf(1),
		max:   math.Inf(-1),
		count: 0,
		sum:   0,
		avg:   0,
	}
}

// Add a new value to the summary.
func (s *DogSketch) Add(c *Config, v float64) {
	key := c.Key(v)
	s.store.add(c, key)

	// Keep track of summary stats
	if v < s.min {
		s.min = v
	}
	if s.max < v {
		s.max = v
	}
	s.count++
	s.sum += v
	s.avg += (v - s.avg) / float64(s.count)
}

// Merge another sketch (with the same binLimit and gamma) in place.
func (s *DogSketch) Merge(c *Config, o *DogSketch) {
	if o.count == 0 {
		return
	}
	if s.count == 0 {
		*s.store = o.store.makeCopy()
		s.count = o.count
		s.sum = o.sum
		s.avg = o.avg
		s.min = o.min
		s.max = o.max
		return
	}

	// Merge the bins
	s.store.merge(c, o.store)

	// Merge summary stats
	s.count += o.count
	s.sum += o.sum
	s.avg = s.avg + (o.avg-s.avg)*float64(o.count)/float64(s.count)
	if o.min < s.min {
		s.min = o.min
	}
	if o.max > s.max {
		s.max = o.max
	}
}

// Quantile returns the estimate of the element at q.
func (s *DogSketch) Quantile(c *Config, q float64) float64 {
	switch {
	case q < 0, q > 1, s.count == 0:
		return math.NaN()
	case q == 0:
		return s.min
	case q == 1:
		return s.max
	}

	rank := int(q*float64(s.count-1) + 1)
	var n int
	for i, b := range s.store.bins {
		n += int(b)
		if n >= rank {
			key := i + s.store.minKey
			if key < 0 {
				key += c.offset
				return -0.5 * (1 + c.gamma) * c.powGamma(-key)
			} else if key > 0 {
				key -= c.offset
				return 0.5 * (1 + c.gamma) * c.powGamma(key-1)
			} else {
				return 0
			}
		}
	}
	return s.max
}

func (s *DogSketch) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("count: %d ", s.count))
	buffer.WriteString(fmt.Sprintf("avg: %g ", s.avg))
	buffer.WriteString(fmt.Sprintf("sum: %g ", s.sum))
	buffer.WriteString(fmt.Sprintf("min: %g ", s.min))
	buffer.WriteString(fmt.Sprintf("max: %g ", s.max))
	buffer.WriteString(fmt.Sprintf("bins: %d {", s.store.count))
	for i := 0; i < len(s.store.bins); i++ {
		key := i + s.store.minKey
		buffer.WriteString(fmt.Sprintf("%d: %d, ", key, s.store.bins[i]))
	}
	buffer.WriteString(fmt.Sprintf(", minKey: %d, maxKey: %d}\n", s.store.minKey, s.store.maxKey))
	return buffer.String()
}

func (s *DogSketch) Size() int {
	return int(reflect.TypeOf(*s).Size()) + s.store.size()
}

func (s *DogSketch) Length() int {
	return s.store.length()
}
