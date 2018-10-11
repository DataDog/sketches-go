// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2018 Datadog, Inc.

package dataset

import (
	"math"
	"math/rand"
)

type Generator interface {
	Generate() float64
}

// Constant stream
type Constant struct{ constant float64 }

func NewConstant(constant float64) *Constant { return &Constant{constant: constant} }

func (s *Constant) Generate() float64 { return s.constant }

// Linearly increasing stream
type Linear struct{ currentVal float64 }

func NewLinear() *Linear { return &Linear{0} }

func (g *Linear) Generate() float64 {
	value := g.currentVal
	g.currentVal++
	return value
}

// Normal distribution
type Normal struct{ mean, stddev float64 }

func NewNormal(mean, stddev float64) *Normal { return &Normal{mean: mean, stddev: stddev} }

func (g *Normal) Generate() float64 { return rand.NormFloat64()*g.stddev + g.mean }

// Lognormal distribution
type Lognormal struct{ mu, sigma float64 }

func NewLognormal(mu, sigma float64) *Lognormal { return &Lognormal{mu: mu, sigma: sigma} }

func (g *Lognormal) Generate() float64 {
	r := rand.NormFloat64()
	return math.Exp(r*g.sigma + g.mu)
}

// Exponential distribution
type Exponential struct{ rate float64 }

func NewExponential(rate float64) *Exponential { return &Exponential{rate: rate} }

func (g *Exponential) Generate() float64 { return rand.ExpFloat64() / g.rate }

// Pareto distribution
type Pareto struct{ shape, scale float64 }

func NewPareto(shape, scale float64) *Pareto { return &Pareto{shape: shape, scale: scale} }

func (g *Pareto) Generate() float64 {
	r := rand.ExpFloat64() / g.shape
	return math.Exp(math.Log(g.scale) + r)
}
