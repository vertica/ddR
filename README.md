---
title: "distributedR.dds examples"
author: "Edward Ma"
date: "2015-07-20"
output: rmarkdown::html_vignette
vignette: >
  %\VignetteIndexEntry{Vignette Title}
  %\VignetteEngine{knitr::rmarkdown}
  \usepackage[utf8]{inputenc}
---

Quick examples using the parallel backend with the API

Starting it up:

```r
library(dds)
```
```r
Loading required package: parallel
```

You can optionally specify the number of cores that should be used:
```r
useBackend(parallel, inst=5)
```

Init'ing a DList:

```r
a <- dlist(nparts=5)
a <- dmapply(function(x) { list(3) }, parts(a))
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

Note that we had to use `parts(a)` instead of just `a` for now. Also, we needed to do an `dmapply` to initialize data inside of a. Since this is distributed R, which has strict type safety, we had to make sure each dlist partitition got a list, so we couldn't just return `3`, but `list(3)`.

Some other operations:

Adding 1 to first partition of `a`, 2 to the second, etc.

```r
b <- dmapply(function(x,y) { list(x[[1]] + y ) }, parts(a), y = as.list(1:5))
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

Adding `a` to `b`, then subtracting a constant value

```r
addThenSubtract <- function(x,y,z) {
  list(x[[1]] + y[[1]] - z)
}
c <- dmapply(addThenSubtract,parts(a),parts(b),MoreArgs=list(z=5))
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

Pulling only two parts from each `a` and `b`, and one part from `c` and using them together:

```r
d <- dmapply(addThenSubtract,parts(a,1:2),parts(b,c(2,4)),MoreArgs=list(z=collect(c,1)[[1]]))
collect(d)
```

```
## [[1]]
## [1] 6
## 
## [[2]]
## [1] 8
```

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
