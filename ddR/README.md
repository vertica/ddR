---
title: "ddR README"
author: "Edward Ma, Indrajit Roy, Michael Lawrence"
date: "2015-10-22"
output: rmarkdown::html_vignette
vignette: >
  %\VignetteIndexEntry{Vignette Title}
  %\VignetteEngine{knitr::rmarkdown}
  \usepackage[utf8]{inputenc}
---

The 'ddR' package aims to provide an unified R interface for writing
parallel and distributed applications.  Our goal is to ensure that R
programs written using the 'ddR' API work across different distributed
backends, therefore, reducing the effort required by users to
understand and program on different backends.  Currently 'ddR'
programs can be executed on R's default 'parallel' package as well as
the open source HP Distributed R.  We plan to add support for
SparkR. This package is an outcome of feedback and collaboration
across different companies and R-core members!


'ddR' is an API, and includes a default execution engine, to express
and execute distributed applications. Users can declare distributed
objects (i.e., `dlist`, `dframe`, `darray`), and execute parallel
operations on these data structures using R-style `apply`
functions. It also allows different backends (that support ddR, and
have ddR "drivers" written for them) to be dynamically activated in
the R user's environment to execute applications

Please refer to the user guide under vignettes/ for a detailed description on how to use the package.

### Some quick examples


```r
library(ddR)
```
By default, the `parallel` backend is used with all the cores present on the machine. You can switch backends or specify the number of cores to use with the `useBackend` function. For example, you can specify that the `parallel` backend should be used with only 4 cores by executing `useBackend(parallel, executors=4)`.

Initializing a distributed list (dlist):


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
## ddR Distributed Object
## Type: dlist
## # of partitions: 5
## Partitions per dimension: 5x1
## Partition sizes: [1], [1], [1], [1], [1]
## Length: 5
## Backend: parallel
```
`a` is a distributed object in ddR. Note that we did not specify the number of partitions of the output, but by default it is equal to the length of the inputs (5). Use the parameter `nparts` to specify how the output should be partitioned:

Below is the code to add 1 to the first element of `a`, 2 to the second, etc. The syntax of `dmapply` is similar to R's standard `mapply` function.


```r
b <- dmapply(function(x,y) { x + y }, a, 1:5,nparts=1)
b
```

```
## 
## ddR Distributed Object
## Type: dlist
## # of partitions: 1
## Partitions per dimension: 1x1
## Partition sizes: [5]
## Length: 5
## Backend: parallel
```
Since we specified `nparts=1` in `dmapply`, `b` only has one partition of 5 elements. Note that the argument `nparts` is optional, and a user can always ignore it.


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

Adding `a` to `b`, and then subtracting a constant value

```r
addThenSubtract <- function(x,y,z) {
  x + y - z
}
c <- dmapply(addThenSubtract,a,b,MoreArgs=list(z=5))
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
We can also process distributed objects partitionwise. Below is an example where we calculate the length of each partition:


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

We partitioned `a` with 5 parts and it had 5 elements, so the length of each partition is 1.

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

We also have support for `darrays` and `dframes`. Check vignettes/ on how to use them.

For more interesting parallel machine learning algorithms, you may view (and run) the example scripts under /examples.

## Using the Distributed R backend

To use the Distributed R library for ddR, first install `distributedR.ddR` and then load it:

```r
library(distributedR.ddR)
```

```
## Loading required package: distributedR
## Loading required package: Rcpp
## Loading required package: RInside
## Loading required package: XML
## Loading required package: ddR
## 
## Attaching package: 'ddR'
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

1. Reporting issues.
2. Contributing code and sending a pull request.

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
