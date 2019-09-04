# sketches-go

This repo contains Go implementations of the distributed quantile sketch algorithms GKArray [1] and DDSketch [2]. Both sketches are fully mergeable, meaning that multiple sketches from distributed systems can be combined in a central node.

## GKArray

GKArray provides a sketch with a rank error guarantee of espilon (without merge) or 2\*epsilon (with merge). The default value of epsilon is 0.01.

### Usage

```go
import "github.com/DataDog/sketches-go/gkarray"

sketch := gk.NewDefaultGKArray()
```

Add some values to the sketch.

```go
import "math/rand"

for i := 0; i < 500; i++ {
  v := rand.NormFloat64()
  sketch.Add(v)
}
```

Find the quantiles to within epsilon of rank.

```go
qs := []float64{0.5, 0.75, 0.9, 1}
quantiles := make([]float64, len(qs))
for i, q := range qs {
  quantiles[i] = sketch.Quantile(q)
}
```

Merge another `GKArray` into `sketch`.

```go
anotherSketch := gk.NewDefaultGKArray()
for i := 0; i < 500; i++ {
  v := rand.NormFloat64()
  anotherSketch.Add(v)
}
sketch.Merge(anotherSketch)
```

Now the quantiles will be accurate to within 2\*epsilon of rank.

## DDSketch

DDSketch has a relative error guarantee of alpha for any quantile q in [0, 1] that is not too small. Concretely, the q-quantile will be accurate up to a relative error of alpha as long as it belongs to one of the m bins kept by the sketch. The default values of alpha and m are 0.01 and 2048, repectively. In addition, a value that is smaller than min_value in magnitude is indistinguishable from 0. The default min_value is 1.0e-9. For more details, refer to [2].

### Usage

```go
import "github.com/DataDog/sketches-go/ddsketch"

c := ddsketch.NewDefaultConfig()
sketch := ddsketch.NewDDSketch(c)
```

Add values to the sketch.

```go
import "math/rand"

for i := 0; i < 500; i++ {
  v := rand.NormFloat64()
  sketch.Add(v)
}
```

Find the quantiles to within alpha relative error.

```go
qs := []float64{0.5, 0.75, 0.9, 1}
quantiles := make([]float64, len(qs))
for i, q := range qs {
  quantiles[i] = sketch.Quantile(q)
}
```

Merge another `DDSketch` into `sketch`.

```go
anotherSketch := ddsketch.NewDDSketch(c)
for i := 0; i < 500; i++ {
  v := rand.NormFloat64()
  anotherSketch.Add(v)
}
sketch.Merge(anotherSketch)
```

The quantiles are still accurate to within alpha relative error.

## References

[1] Michael B. Greenwald and Sanjeev Khanna. Space-efficient online computation of quantile summaries. In Proc. 2001 ACM
SIGMOD International Conference on Management of Data, SIGMOD ’01, pages 58–66. ACM, 2001.

[2] Charles Masson, Jee Rim and Homin K. Lee. All the nines: a fully mergeable quantile sketch with relative-error guarantees for arbitrarily large quantiles. 2018.
