library(ddR)
library(kmeans.ddR)

nInst = 2 # Change level of parallelism
useBackend(parallel, executors = nInst)
# Uncomment the following lines to use Distributed R 
#library(distributedR.ddR)
#useBackend(distributedR)

# Set up data size
numcols = 100
numrows = 100000
K = 10

set.seed(37)
centers = matrix(rnorm(K * numcols, sd = 10), nrow = K)
dnumrows = as.integer(numrows/nInst)

generateKMeansData <- function(id, centers, nrow, ncol) {
    offsets = matrix(rnorm(dnumrows * numcols), nrow = nrow, ncol = ncol)
    cluster_ids = sample.int(nrow(centers),dnumrows,replace = TRUE)
    feature_obs = centers[cluster_ids,] + offsets
    feature_obs
}

cat(sprintf("Generating %d x %d matrix for clustering with %d means\n",
            numrows, numcols, K))

dfeature <- dmapply(generateKMeansData,id = 1:nInst,
  MoreArgs = list(centers = centers, nrow = dnumrows, ncol = numcols),
		output.type = "darray", 
		combine = "rbind", nparts = c(nInst,1))

cat("training dkmeans model on distributed data\n")
dtraining_time <- system.time(
    dmodel <- dkmeans(dfeature, K, iter.max = 100)
)[3]
cat(dtraining_time, "\n")

feature <- collect(dfeature)
cat("training normal kmeans model on centralized data\n")
training_time <- system.time(
    model <- kmeans(feature, K, iter.max = 100, algorithm = "Lloyd")
)[3]
cat(training_time, "\n")
