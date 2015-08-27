library(HPdcluster)

library(dds)

#Uncomment the following lines to use Distributed R 
#library(distributedR.dds)
#useBackend(distributedR)

nInst = 4  #determines the no. of  partitions (and hence max cores utilized)
ncol = 10
nrow = 1000000
K = 10
centers = 100*matrix(rnorm(K*ncol),nrow = K)
cat("Generating data with rows=",nrow," and cols=", ncol,"\n")
nrow = as.integer(nrow/nInst)

generateKMeansData <- function(id,centers, nrow, ncol) {
offsets = matrix(rnorm(nrow*ncol),nrow = nrow,ncol = ncol)
cluster_ids = sample.int(nrow(centers),nrow,replace = TRUE)
feature_obs = centers[cluster_ids,] + offsets
list(feature_obs)
}


feature <- dmapply(generateKMeansData, as.list(1:nInst), 
                MoreArgs = list(centers = centers, nrow = nrow, ncol = ncol))

model <- hpdkmeans(feature,K, trace = TRUE) 



