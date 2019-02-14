// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package dataset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRanks(t *testing.T) {
	d := NewDataset()
	d.Add(1)
	d.Add(3)
	d.Add(3)
	d.Add(3)
	d.Add(5)

	assert.Equal(t, int64(0), d.MinRank(0))
	assert.Equal(t, int64(0), d.MaxRank(0))
	assert.Equal(t, int64(0), d.MinRank(1))
	assert.Equal(t, int64(1), d.MaxRank(1))
	assert.Equal(t, int64(1), d.MinRank(2))
	assert.Equal(t, int64(1), d.MaxRank(2))
	assert.Equal(t, int64(1), d.MinRank(3))
	assert.Equal(t, int64(4), d.MaxRank(3))
	assert.Equal(t, int64(4), d.MinRank(4))
	assert.Equal(t, int64(4), d.MaxRank(4))
	assert.Equal(t, int64(4), d.MinRank(5))
	assert.Equal(t, int64(5), d.MaxRank(5))
	assert.Equal(t, int64(5), d.MinRank(6))
	assert.Equal(t, int64(5), d.MaxRank(6))
}
