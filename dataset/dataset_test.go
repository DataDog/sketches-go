// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package dataset

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuantiles(t *testing.T) {
	d := NewDataset()
	d.Add(11.0)
	d.Add(12.0)
	d.Add(13.0)
	d.Add(13.0)
	d.Add(15.0)

	assert.True(t, math.IsNaN(d.LowerQuantile(-0.5/(d.Count-1))))
	assert.Equal(t, 11.0, d.LowerQuantile(0.0/(d.Count-1)))
	assert.Equal(t, 11.0, d.LowerQuantile(0.5/(d.Count-1)))
	assert.Equal(t, 12.0, d.LowerQuantile(1.0/(d.Count-1)))
	assert.Equal(t, 12.0, d.LowerQuantile(1.5/(d.Count-1)))
	assert.Equal(t, 13.0, d.LowerQuantile(2.0/(d.Count-1)))
	assert.Equal(t, 13.0, d.LowerQuantile(2.5/(d.Count-1)))
	assert.Equal(t, 13.0, d.LowerQuantile(3.0/(d.Count-1)))
	assert.Equal(t, 13.0, d.LowerQuantile(3.5/(d.Count-1)))
	assert.Equal(t, 15.0, d.LowerQuantile(4.0/(d.Count-1)))
	assert.True(t, math.IsNaN(d.LowerQuantile(4.5/(d.Count-1))))

	assert.True(t, math.IsNaN(d.UpperQuantile(-0.5/(d.Count-1))))
	assert.Equal(t, 11.0, d.UpperQuantile(0.0/(d.Count-1)))
	assert.Equal(t, 12.0, d.UpperQuantile(0.5/(d.Count-1)))
	assert.Equal(t, 12.0, d.UpperQuantile(1.0/(d.Count-1)))
	assert.Equal(t, 13.0, d.UpperQuantile(1.5/(d.Count-1)))
	assert.Equal(t, 13.0, d.UpperQuantile(2.0/(d.Count-1)))
	assert.Equal(t, 13.0, d.UpperQuantile(2.5/(d.Count-1)))
	assert.Equal(t, 13.0, d.UpperQuantile(3.0/(d.Count-1)))
	assert.Equal(t, 15.0, d.UpperQuantile(3.5/(d.Count-1)))
	assert.Equal(t, 15.0, d.UpperQuantile(4.0/(d.Count-1)))
	assert.True(t, math.IsNaN(d.UpperQuantile(4.5/(d.Count-1))))
}
