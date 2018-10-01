package dataset

import (
	"math"
	"math/rand"
	"sort"
)

type Generator interface {
	Generate() float64
}

type Dataset struct {
	Values []float64
	Count  int64
	sorted bool
}

func NewDataset() *Dataset { return &Dataset{} }

func (d *Dataset) Add(v float64) {
	d.Values = append(d.Values, v)
	d.Count++
	d.sorted = false
}

func (d *Dataset) Quantile(q float64) float64 {
	if q < 0 || q > 1 {
		panic("Quantile out of bounds")
	}
	d.Sort()
	if d.Count == 0 {
		return math.NaN()
	}

	rank := q * float64(d.Count-1)
	return d.Values[int64(rank)]
}

func (d *Dataset) Rank(v float64) int64 {
	d.Sort()
	i := int64(0)
	for ; i < d.Count; i++ {
		if d.Values[i] > v {
			break
		}
	}
	return i
}

func (d *Dataset) Min() float64 {
	d.Sort()
	return d.Values[0]
}

func (d *Dataset) Max() float64 {
	d.Sort()
	return d.Values[len(d.Values)-1]
}

func (d *Dataset) Sum() float64 {
	s := float64(0)
	for _, v := range d.Values {
		s += v
	}
	return s
}

func (d *Dataset) Avg() float64 {
	return d.Sum() / float64(d.Count)
}

func (d *Dataset) Sort() {
	if d.sorted {
		return
	}
	sort.Float64s(d.Values)
	d.sorted = true
}

// Constant stream
type Constant struct{ constant float64 }

func NewConstant(constant float64) *Constant { return &Constant{constant: constant} }

func (s *Constant) Generate() float64 { return s.constant }

// Uniform distribution
type Uniform struct{ currentVal float64 }

func NewUniform() *Uniform { return &Uniform{0} }

func (g *Uniform) Generate() float64 {
	value := g.currentVal
	g.currentVal++
	return value
}

// Normal distribution
type Normal struct{ mean, stddev float64 }

func NewNormal(mean, stddev float64) *Normal { return &Normal{mean: mean, stddev: stddev} }

func (g *Normal) Generate() float64 { return rand.NormFloat64()*g.stddev + g.mean }

// Exponential distribution
type Exponential struct{ rate float64 }

func NewExponential(rate float64) *Exponential { return &Exponential{rate: rate} }

func (g *Exponential) Generate() float64 { return rand.ExpFloat64() / g.rate }
