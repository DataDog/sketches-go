// Unless explicitly stated otherwise all files in this repository are licensed
// under the BSD-3-Clause License.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package dogsketch

import (
	"math"
)

const (
	defaultMaxBins  = 2048
	defaultAlpha    = 0.01
	defaultMinValue = 1.0e-9
)

type Config struct {
	binLimit int
	gamma    float64
	gamma_ln float64
	minValue float64
	offset   int
}

// Config contains an offset for the bin keys which ensures that keys for positive
// numbers that are larger than minValue are greater than or equal to 1 while the
// keys for negative numbers are less than or equal to -1.
func NewDefaultConfig() *Config {
	c := &Config{
		binLimit: defaultMaxBins,
		gamma:    1 + 2*defaultAlpha,
		gamma_ln: math.Log1p(2 * defaultAlpha),
		minValue: defaultMinValue,
	}
	c.offset = -int(c.logGamma(c.minValue)) + 1
	return c
}

func NewConfig(alpha float64, maxBins int, minValue float64) *Config {
	c := &Config{
		binLimit: maxBins,
		gamma:    1 + 2*alpha,
		gamma_ln: math.Log1p(2 * alpha),
		minValue: minValue,
	}
	c.offset = -int(c.logGamma(c.minValue)) + 1
	return c
}

func (c *Config) Key(v float64) int {
	if v < -c.minValue {
		return -int(math.Ceil(c.logGamma(-v))) - c.offset
	} else if v > c.minValue {
		return int(math.Ceil(c.logGamma(v))) + c.offset
	} else {
		return 0
	}
}

func (c *Config) logGamma(v float64) float64 {
	return math.Log(v) / c.gamma_ln
}

func (c *Config) powGamma(k int) float64 {
	return math.Pow(c.gamma, float64(k))
}
