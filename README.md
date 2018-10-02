# sketches-go

This repo contains go implementations of the distributed quantile sketch algorithms GKArray [1]  and DogSketch [2]. Both sketches are fully mergeable, meaning that multiple sketches from distributed systems can be combined in a central node.

## GKArray

GKArray provides a sketch with a rank error guarantee of espilon (without merge) or 2\*epsilon (with merge). The default value of epsilon is 0.005.

### Usage
```
import "github.com/DataDog/sketches-go/gkarray

sketch := gkarray.NewGKArray()
```
Add some values to the sketch. 
```
import "math"

for i := 0; i < 500; i++ {
  v := math.NormFloat64()
  sketch = sketch.Add(v)
}
```
Find the quantiles to within epsilon of rank.
```
quantiles := sketch.Quantiles([0.5, 0.75, 0.9, 1])
```
Merge another `GKArray` into `sketch`.
```
another_sketch = gkarray.NewGKArray()
for i := 0; i < 500; i++ {
  v := math.NormFloat64()
  another_sketch = another_sketch.Add(v)
}
sketch = sketch.Merge(another_sketch)
```
Now the quantiles will be accurate to within 2\*epsilon of rank.
```
quantiles = sketch.Quantiles([0.5, 0.75, 0.9, 1])
```

## DogSketch

DogSketch has a relative error guarantee of alpha for any quantile q in [0, 1] that is not too small. Concretely, the q-quantile will be accurate up to a relative error of alpha as long as it belongs to one of the m buckets kept by the sketch. The default values of alpha and m are 0.01 and 2048, repectively. In addition, a value that is smaller than min_value in magnitude is indistinguishable from 0. The default min_value is 1.0e-9.

### Usage
```
import "github.com/DataDog/sketches-go/dogsketch

c := dogsketch.NewDefaultConfig()
sketch := dogsketch.DogSketch(c)
```
Add values to the sketch
```
import "math"

for i := 0; i < 500; i++ {
  v := math.NormFloat64()
  sketch.Add(v)
}
```
Find the quantiles to within alpha relative error.
```
qs := [0.5, 0.75, 0.9, 1]
quantiles := make([]float64, len(qs))
for i, q := range qs {
  quantiles[i] = sketch.Quantile(q)
}
```
Merge another `DogSketch` into `sketch`.
```
another_sketch := dogsketch.DogSketch(c)
for i := 0; i < 500; i++ {
  v := math.NormFloat64()
  another_sketch.Add(v)
}
sketch.Merge(another_sketch)
```
The quantiles are still accurate to within alpha relative error.
```
qs := [0.5, 0.75, 0.9, 1]
quantiles := make([]float64, len(qs))
for i, q := range qs {
  quantiles[i] = sketch.Quantile(q)
}
```

## References
[1] Michael B. Greenwald and Sanjeev Khanna. Space-efficient online computation of quantile summaries. In Proc. 2001 ACM
SIGMOD International Conference on Management of Data, SIGMOD ’01, pages 58–66. ACM, 2001.

[2] Charles-Phillip Masson, Jee Rim and Homin K. Lee. All the nines: a fully mergeable quantile sketch with relative-error guarantees for arbitrarily large quantiles. 2018.
