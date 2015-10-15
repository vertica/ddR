---
title: "distributedR.dds examples"
author: "Edward Ma"
date: "2015-09-08"
output: rmarkdown::html_vignette
vignette: >
  %\VignetteIndexEntry{Vignette Title}
  %\VignetteEngine{knitr::rmarkdown}
  \usepackage[utf8]{inputenc}
---

Quick examples using the DDS API (note: currently some of these examples only work withe Distributed R backend)

Starting it up:

```r
library(distributedR.dds)
useBackend(distributedR)
```

```
## Switching out of using Distributed R. Shutting it down...
## Backend switched to Distributed R. Starting it up...
```

```
## Master address:port - 127.0.0.1:50000
```

Init'ing a DList:

```r
a <- dmapply(function(x) { x }, rep(3,5))
collect(a)
```

```
## [[1]]
## [1] 3
## 
## [[2]]
## [1] 3
## 
## [[3]]
## [1] 3
## 
## [[4]]
## [1] 3
## 
## [[5]]
## [1] 3
```

Printing `a`:

```r
a
```

```
## 
## Type: DListClass
## No. of Partitions: 5
## nparts: 5,1
## psize: [1], [1], [1], [1], [1]
## dim: 5
## Backend Type: Distributed R
```

`a` is now a distributed object in DDS. Note that we did not specify the number of partitions of the output, but by default it went to the length of the inputs (5). If we wanted to specify how the output should be partitioned, we can use the `nparts` parameter to `dmapply`:

Adding 1 to first element of `a`, 2 to the second, etc.


```r
b <- dmapply(function(x,y) { x + y }, a, 1:5,nparts=1)
```

```
## Warning in do_dmapply(dds.env$driver, func = match.fun(FUN), ..., MoreArgs = MoreArgs, : A repartitioning of an input variable has been triggered.
## For better performance, please try to partition your inputs compatibly.
```

```r
b
```

```
## 
## Type: DListClass
## No. of Partitions: 1
## nparts: 1,1
## psize: [5]
## dim: 5
## Backend Type: Distributed R
```

As you can see, `b` only has one partition of 5 elements.


```r
collect(b)
```

```
## [[1]]
## [1] 4
## 
## [[2]]
## [1] 5
## 
## [[3]]
## [1] 6
## 
## [[4]]
## [1] 7
## 
## [[5]]
## [1] 8
```
Some other operations:
`

Adding `a` to `b`, then subtracting a constant value

```r
addThenSubtract <- function(x,y,z) {
  x + y - z
}
c <- dmapply(addThenSubtract,a,b,MoreArgs=list(z=5))
```

```
## Warning in do_dmapply(dds.env$driver, func = match.fun(FUN), ..., MoreArgs = MoreArgs, : A repartitioning of an input variable has been triggered.
## For better performance, please try to partition your inputs compatibly.
```

```r
collect(c)
```

```
## [[1]]
## [1] 2
## 
## [[2]]
## [1] 3
## 
## [[3]]
## [1] 4
## 
## [[4]]
## [1] 5
## 
## [[5]]
## [1] 6
```

Accessing dobjects by parts:


```r
d <- dmapply(function(x) length(x),parts(a))
collect(d)
```

```
## [[1]]
## [1] 1
## 
## [[2]]
## [1] 1
## 
## [[3]]
## [1] 1
## 
## [[4]]
## [1] 1
## 
## [[5]]
## [1] 1
```

We partitioned `a` with 5 parts and it had 5 elements, so the length of each partition is of course 1.

However, `b` only had one partition, so that one partition should be of length 5:


```r
e <- dmapply(function(x) length(x),parts(b))
collect(e)
```

```
## [[1]]
## [1] 5
```

Note that `parts()` and non-parts arguments can be used in any combination to dmapply. `parts(dobj)` returns a list of the partitions of that dobject, which can be passed into dmapply like any other list. `parts(dobj,index)`, where `index` is a list, vector, or scalar, returns a specific partition or range of partitions of `dobj`.

We also have support for `darrays` and `dframes`. Their APIs are a bit more complex, and this guide will be updated shortly with that content.

For a more detailed example, you may view (and run) example_mat_mul.R (matrix multiplication) in the top-level directory.
