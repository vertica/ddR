---
title: "kmeans.ddR README"
author: "Vishrut Gupta, Arash Fard"
date: "2015-10-26"
---


### Start the parallel backend with 2 instances
```
library(ddR)
library(kmeans.ddR)
nInst = 2
useBackend(parallel,executors = nInst)
```



### Generate some data:
#### K = 10 clusters, ncol = 10 features, nrow ~ 1000 observations
```
ncol = 100
nrow = 1000000
nrow = as.integer(nrow/nInst)
K = 10
```
#### Generate centers of clusters
```
centers = 100*matrix(rnorm(K*ncol),nrow = K)
```
#### Generate observations of features
```
generateRFData <- function(id, centers, nrow, ncol) {
	offsets = matrix(rnorm(nrow*ncol),nrow = nrow,ncol = ncol)
	cluster_ids = sample.int(nrow(centers),nrow,replace = TRUE)
	feature_obs = centers[cluster_ids,] + offsets
	features <- data.frame(cluster_ids, feature_obs)
}
features <- dmapply(generateRFData,id = 1:nInst,
                MoreArgs = list(centers = centers, nrow = nrow, ncol = ncol),
		output.type = "dframe", 
		combine = "rbind", nparts = c(nInst,1))
colnames(features) <- paste("X",1:ncol(features),sep="")
```



### Example of dkmeans function on distributed data
```
training_time <- system.time({model <- dkmeans(features,K)})[3]
cat("training dkmeans model on distributed data: ", training_time,"\n")
```



#### Locally gathering data to run normal kmeans on centralized data
```
feature <- collect(feature)
```
### Example of kmeans on centralized data
```
training_time <- system.time({model <- kmeans(feature, K, algorithm = "Lloyd")})[3]
cat("training normal kmeans model on centralized data: ", training_time,"\n")
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
