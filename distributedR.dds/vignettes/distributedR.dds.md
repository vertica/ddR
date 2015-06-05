---
title: "distributedR.dds examples"
author: "Edward Ma"
date: "2015-06-05"
output: rmarkdown::html_vignette
vignette: >
  %\VignetteIndexEntry{Vignette Title}
  %\VignetteEngine{knitr::rmarkdown}
  \usepackage[utf8]{inputenc}
---

Quick examples using distributedR.dds with the API

Starting it up:

```r
library(distributedR.dds)
```

```
## Loading required package: distributedR
## Loading required package: Rcpp
## Loading required package: RInside
## Loading required package: XML
## Loading required package: dds
## Loading required package: assertthat
## 
## Attaching package: 'dds'
## 
## The following objects are masked from 'package:distributedR':
## 
##     darray, dframe, dlist
```

```r
useBackend(distributedR)
```

```
## Master address:port - 192.168.139.128:50000
```

Init'ing a DList:

```r
a <- dlist(nparts=5)
a <- dmapply(function(x) { list(3) }, parts(a))
```

```
## function (x = splits(.tempVar1, ids[[1]][[index]]), .funct = function (x) 
## {
##     list(3)
## }, .newDObj = splits(.outObj, index), .dimObj = splits(.dimsObj, 
##     index)) 
## {
##     .newDObj <- .funct(x)
##     update(.newDObj)
##     .dimObj <- list(ifelse(is.list(.newDObj), length(.newDObj), 
##         dim(.newDObj)))
##     update(.dimObj)
## }
## <environment: 0x7c9a098>
## [1] ".newDObj <- .funct(x)"
## function(x) { list(3) }
```

```r
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
```

```
## function (x = splits(.tempVar1, ids[[1]][[index]]), y = ids[[2]][[index]], 
##     .funct = function (x, y) 
##     {
##         list(x[[1]] + y)
##     }, .newDObj = splits(.outObj, index), .dimObj = splits(.dimsObj, 
##         index)) 
## {
##     .newDObj <- .funct(x, y)
##     update(.newDObj)
##     .dimObj <- list(ifelse(is.list(.newDObj), length(.newDObj), 
##         dim(.newDObj)))
##     update(.dimObj)
## }
## <environment: 0x78ca608>
## [1] ".newDObj <- .funct(x, y)"
## function(x,y) { list(x[[1]] + y ) }
```

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

Adding `a` to `b`, then subtracting a constant value

```r
addThenSubtract <- function(x,y,z) {
  list(x[[1]] + y[[1]] - z)
}
c <- dmapply(addThenSubtract,parts(a),parts(b),MoreArgs=list(z=5))
```

```
## function (x = splits(.tempVar1, ids[[1]][[index]]), y = splits(.tempVar2, 
##     ids[[2]][[index]]), z = 5, .funct = function (x, y, z) 
## {
##     list(x[[1]] + y[[1]] - z)
## }, .newDObj = splits(.outObj, index), .dimObj = splits(.dimsObj, 
##     index)) 
## {
##     .newDObj <- .funct(x, y, z = 2L)
##     update(.newDObj)
##     .dimObj <- list(ifelse(is.list(.newDObj), length(.newDObj), 
##         dim(.newDObj)))
##     update(.dimObj)
## }
## <environment: 0x6ec23d0>
## [1] ".newDObj <- .funct(x, y, z=z)"
## function(x,y,z) {
##   list(x[[1]] + y[[1]] - z)
## }
```

```r
collect(c)
```

```
## [[1]]
## [1] 5
## 
## [[2]]
## [1] 6
## 
## [[3]]
## [1] 7
## 
## [[4]]
## [1] 8
## 
## [[5]]
## [1] 9
```

Pulling only two parts from each `a` and `b`, and one part from `c` and using them together:

```r
d <- dmapply(addThenSubtract,parts(a,1:2),parts(b,c(2,4)),MoreArgs=list(z=collect(c,1)[[1]]))
```

```
## function (x = splits(.tempVar1, ids[[1]][[index]]), y = splits(.tempVar2, 
##     ids[[2]][[index]]), z = 5, .funct = function (x, y, z) 
## {
##     list(x[[1]] + y[[1]] - z)
## }, .newDObj = splits(.outObj, index), .dimObj = splits(.dimsObj, 
##     index)) 
## {
##     .newDObj <- .funct(x, y, z = 2L)
##     update(.newDObj)
##     .dimObj <- list(ifelse(is.list(.newDObj), length(.newDObj), 
##         dim(.newDObj)))
##     update(.dimObj)
## }
## <environment: 0x78f9f98>
## [1] ".newDObj <- .funct(x, y, z=z)"
## function(x,y,z) {
##   list(x[[1]] + y[[1]] - z)
## }
```

```r
collect(d)
```

```
## [[1]]
## [1] 6
## 
## [[2]]
## [1] 8
```
