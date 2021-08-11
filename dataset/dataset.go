// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package dataset

import (
	"math"
	"sort"

	"github.com/DataDog/sketches-go/ddsketch/stat"
)

type Dataset struct {
	Values []float64
	Count  float64
	sorted bool
}

func NewDataset() *Dataset { return &Dataset{} }

func (d *Dataset) Add(v float64) {
	d.Values = append(d.Values, v)
	d.Count++
	d.sorted = false
}

// Quantile returns the lower quantile of the dataset
func (d *Dataset) Quantile(q float64) float64 {
	return d.LowerQuantile(q)
}

func (d *Dataset) LowerQuantile(q float64) float64 {
	if q < 0 || q > 1 || d.Count == 0 {
		return math.NaN()
	}

	d.sort()
	rank := q * (d.Count - 1)
	return d.Values[int(math.Floor(rank))]
}

func (d *Dataset) UpperQuantile(q float64) float64 {
	if q < 0 || q > 1 || d.Count == 0 {
		return math.NaN()
	}

	d.sort()
	rank := q * (d.Count - 1)
	return d.Values[int(math.Ceil(rank))]
}

func (d *Dataset) Min() float64 {
	d.sort()
	return d.Values[0]
}

func (d *Dataset) Max() float64 {
	d.sort()
	return d.Values[len(d.Values)-1]
}

func (d *Dataset) Sum() float64 {
	summaryStatistics := stat.NewSummaryStatistics()
	for _, v := range d.Values {
		summaryStatistics.Add(v, 1)
	}
	return summaryStatistics.Sum()
}

func (d *Dataset) Merge(o *Dataset) {
	for _, v := range o.Values {
		d.Add(v)
	}
}

func (d *Dataset) sort() {
	if d.sorted {
		return
	}
	sort.Float64s(d.Values)
	d.sorted = true
}
