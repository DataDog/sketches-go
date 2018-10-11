// under the BSD-3-Clause License.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package dogsketch

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
)

// DogSketch is an implementation of DogSketch.
type DogSketch struct {
	config *Config
	store  *Store
	min    float64
	max    float64
	count  int64
	sum    float64
}

// NewDogSketch allocates a new DogSketch summary with relative accuracy alpha.
func NewDogSketch(c *Config) *DogSketch {
	return &DogSketch{
		config: c,
		store:  NewStore(c.maxNumBins),
		min:    math.Inf(1),
		max:    math.Inf(-1),
	}
}

// Add a new value to the summary.
func (s *DogSketch) Add(v float64) {
	key := s.config.Key(v)
	s.store.Add(key)

	// Keep track of summary stats
	if v < s.min {
		s.min = v
	}
	if s.max < v {
		s.max = v
	}
	s.count++
	s.sum += v
}

// Quantile returns the estimate of the element at q.
func (s *DogSketch) Quantile(q float64) float64 {
	if q < 0 || q > 1 || s.count == 0 {
		return math.NaN()
	}

	if q == 0 {
		return s.min
	} else if q == 1 {
		return s.max
	}

	rank := int(q*float64(s.count-1) + 1)
	key := s.store.KeyAtRank(rank)
	var quantile float64
	if key < 0 {
		key += s.config.offset
		quantile = -0.5 * (1 + s.config.gamma) * s.config.powGamma(-key-1)
	} else if key > 0 {
		key -= s.config.offset
		quantile = 0.5 * (1 + s.config.gamma) * s.config.powGamma(key-1)
	} else {
		quantile = 0
	}
	// Check that the returned value is larger than the minimum
	// since for q close to 0 (key in the smallest bin) the midpoint
	// of the bin boundaries could be smaller than the minimum
	if quantile < s.min {
		quantile = s.min
	}
	return quantile
}

// Merge another sketch (with the same maxNumBins and gamma) in place.
func (s *DogSketch) Merge(o *DogSketch) {
	if o.count == 0 {
		return
	}
	if s.count == 0 {
		s.store.Copy(o.store)
		s.count = o.count
		s.sum = o.sum
		s.min = o.min
		s.max = o.max
		return
	}

	// Merge the bins
	s.store.Merge(o.store)

	// Merge summary stats
	s.count += o.count
	s.sum += o.sum
	if o.min < s.min {
		s.min = o.min
	}
	if o.max > s.max {
		s.max = o.max
	}
}

func (s *DogSketch) Sum() float64 {
	return s.sum
}

func (s *DogSketch) Avg() float64 {
	return s.sum / float64(s.count)
}

func (s *DogSketch) Count() int64 {
	return s.count
}

func (s *DogSketch) MakeCopy() *DogSketch {
	store := s.store.MakeCopy()
	config := &Config{
		maxNumBins: s.config.maxNumBins,
		gamma:      s.config.gamma,
		gammaLn:    s.config.gammaLn,
		minValue:   s.config.minValue,
		offset:     s.config.offset,
	}
	return &DogSketch{
		config: config,
		store:  store,
		min:    s.min,
		max:    s.max,
		count:  s.count,
		sum:    s.sum,
	}
}

func (s *DogSketch) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("offset: %d ", s.config.offset))
	buffer.WriteString(fmt.Sprintf("count: %d ", s.count))
	buffer.WriteString(fmt.Sprintf("sum: %g ", s.sum))
	buffer.WriteString(fmt.Sprintf("min: %g ", s.min))
	buffer.WriteString(fmt.Sprintf("max: %g ", s.max))
	buffer.WriteString(fmt.Sprintf("bins: %s\n", s.store))
	return buffer.String()
}

func (s *DogSketch) MemorySize() int {
	return int(reflect.TypeOf(*s).Size()) + s.store.Size() + s.config.Size()
}
