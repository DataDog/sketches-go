# sketches-go 

This repo contains Go implementations of the distributed quantile sketch algorithm
DDSketch [1]. DDSketch has relative-error guarantees for any quantile q in [0, 1].
That is if the true value of the qth-quantile is `x` then DDSketch returns a value `y` 
such that `|x-y| / x < e` where `e` is the relative error parameter. DDSketch is also 
fully mergeable, meaning that multiple sketches from distributed systems can be combined 
in a central node.

Our default implementation, returned from `NewDefaultDDSketch(relativeAccuracy)`, is
guaranteed [1] not to grow too large in size for any data that can be described by a
distribution whose tails are sub-exponential.

We also provide implementations, returned by `LogCollapsingLowestDenseDDSketch(relativeAccuracy, maxNumBins)`
and `LogCollapsingHighestDenseDDSketch(relativeAccuracy, maxNumBins)`, where the q-quantile
will be accurate up to the specified relative error for q that is not too small (or large).
Concretely, the q-quantile will be accurate up to the specified relative error as long as it
belongs to one of the `m` bins kept by the sketch. For instance, If the values are time in seconds, 
`maxNumBins = 2048` covers a time range from 80 microseconds to 1 year.

### Usage

```go
import "github.com/DataDog/sketches-go/ddsketch"

relativeAccuracy := 0.01
sketch := ddsketch.NewDefaultDDSketch(relativeAccuracy)
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
quantiles, err := sketch.GetValuesAtQuantiles(qs)
```

Merge another `DDSketch` into `sketch`.

```go
anotherSketch := ddsketch.NewDefaultDDSketch(relativeAccuracy)
for i := 0; i < 500; i++ {
  v := rand.NormFloat64()
  anotherSketch.Add(v)
}
sketch.MergeWith(anotherSketch)
```

The quantiles are in `sketch` are still accurate to within `relativeAccuracy`.

## References

[1] Charles Masson and Jee E Rim and Homin K. Lee. DDSketch: A fast and fully-mergeable quantile sketch with 
relative-error guarantees. PVLDB, 12(12): 2195-2205, 2019. (The code referenced in the paper, including our 
implementation of the the Greenwald-Khanna (GK) algorithm, can be found at: 
https://github.com/DataDog/sketches-go/releases/tag/v0.0.1 )
