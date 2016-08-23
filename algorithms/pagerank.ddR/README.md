---
title: "pagerank.ddR README"
author: "Arash Fard, Vishrut Gupta"
date: "2015-11-11"
---


### Start the parallel backend with 2 instances
```
library(ddR)
library(pagerank.ddR)
nInst = 2
useBackend("parallel",executors = nInst)
```



### Generate some data:
#### Generate an example graph with 6 vertices
```
library(Matrix)
mygraph <- Matrix(0, 6,6)
mygraph[2,1] <- 1L;mygraph[2,3] <- 1L;mygraph[3,1] <- 1L;mygraph[3,2] <- 1L;
mygraph[3,4] <- 1L;mygraph[4,5] <- 1L;mygraph[4,6] <- 1L;mygraph[5,4] <- 1L; 
mygraph[5,6] <- 1L;mygraph[6,4] <- 1L
```

#### Generate the distributed copy of the graph
```
generateDgraph <- function(id, graph, npartitions) {
	nColumns <- ceiling(ncol(graph) / npartitions)
    start <- (id -1) * nColumns + 1
    end <- min(ncol(graph), start + nColumns -1)

    graph[, start:end, drop=FALSE]
}

dgraph <- dmapply(generateDgraph, id = 1:nInst,
                MoreArgs = list(graph = mygraph, npartitions = nInst),
		output.type = "sparse_darray", 
		combine = "cbind", nparts = c(1,nInst))
```


### Example of dpagerank function on distributed graph
```
pr <- dpagerank(dgraph)
cat("the vertex with the highest rank: ", dwhich.max(pr),"\n")
```




## How to Contribute

You can help us in different ways:

1. Reporting [issues](https://github.com/vertica/ddR/issues).
2. Contributing code and sending a [Pull Request](https://github.com/vertica/ddR/pulls).

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
