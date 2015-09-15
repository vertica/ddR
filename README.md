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

Quick examples using the DDS API (note: currently some of these examples only work when using the Distributed R backend)

Starting it up:

```r
library(dds)
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

You also try the parallel K-means clustering example by first installing the HPdcluster package under scripts/ (via R CMD INSTALL HPdcluster), and running scripts/run_kmeans.R.

## Using the Distributed R backend

Use the Distributed R library for dds:
```r
library(distributedR.dds)
```

```
## Loading required package: distributedR
## Loading required package: Rcpp
## Loading required package: RInside
## Loading required package: XML
## Loading required package: dds
## 
## Attaching package: 'dds'
## 
## The following objects are masked from 'package:distributedR':
## 
##     darray, dframe, dlist, is.dlist
```

```r
useBackend(distributedR)
```

```
## Master address:port - 127.0.0.1:50000
```

Now you can try the different list examples which were used with the 'parallel' backend.

## How to Contribute

You can help us in different ways:

1. Reporting [issues](https://github.com/vertica/Standard-R-API/issues).
2. Contributing code and sending a [Pull Request](https://github.com/vertica/Standard-R-API/pulls).

In order to contribute the code base of this project, you must agree to the Developer Certificate of Origin (DCO) 1.1 for this project under GPLv2+:

    By making a contribution to this project, I certify that:
    
    (a) The contribution was created in whole or in part by me and I have the 
        right to submit it under the open source license indicated in the file; or
    (b) The contribution is based upon previous work that, to the best of my 
        knowledge, is covered under an appropriate open source license and I 
        have the right under that license to submit that work with modifications, 
        whether created in whole or in part by me, under the same open source 
        license (unless I am permitted to submit under a different license), 
        as indicated in the file; or
    (c) The contribution was provided directly to me by some other person who 
        certified (a), (b) or (c) and I have not modified it.
    (d) I understand and agree that this project and the contribution are public and
        that a record of the contribution (including all personal information I submit 
        with it, including my sign-off) is maintained indefinitely and may be 
        redistributed consistent with this project or the open source license(s) involved.

To indicate acceptance of the DCO you need to add a `Signed-off-by` line to every commit. E.g.:

    Signed-off-by: John Doe <john.doe@hisdomain.com>

To automatically add that line use the `-s` switch when running `git commit`:

    $ git commit -s
