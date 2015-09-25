library(dds)
library(methods)
library(dds.kmeans)
library(distributedR.dds)

args <- commandArgs(TRUE)
if(length(args) > 0 & args[1] == "DR")
	useBackend(distributedR)

#Uncomment the following lines to use Distributed R 
useBackend(distributedR)

nInst = 2  #determines the no. of  partitions (and hence max cores utilized)
ncol = 10
nrow = 1000000
K = 10
centers = 100*matrix(rnorm(K*ncol),nrow = K)
cat("Generating data with rows=",nrow," and cols=", ncol,"\n")
nrow = as.integer(nrow/nInst)

generateKMeansData <- function(id, centers, nrow, ncol) {
offsets = matrix(rnorm(nrow*ncol),nrow = nrow,ncol = ncol)
cluster_ids = sample.int(nrow(centers),nrow,replace = TRUE)
feature_obs = centers[cluster_ids,] + offsets
feature_obs
}

feature <- dmapply(generateKMeansData,id = 1:nInst,
                MoreArgs = list(centers = centers, nrow = nrow, ncol = ncol),
		output.type = "DArrayClass", 
		combine = "row", nparts = c(nInst,1))

# model <- hpdkmeans(feature,K, trace = TRUE)  # For version using dlists
model <- hpdkmeansv2(feature,K, trace = TRUE)  # For version using darrays
