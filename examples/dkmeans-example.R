library(ddR)
library(kmeans.ddR)

nInst = 2 # Change level of parallelism
useBackend(parallel,executors = nInst)
# Uncomment the following lines to use Distributed R 
#library(distributedR.ddR)
#useBackend(distributedR)

# Set up data size
ncol = 100
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
		output.type = "darray", 
		combine = "rbind", nparts = c(nInst,1))

training_time <- system.time({model <- dkmeans(feature,K)})[3]
cat("training dkmeans model on distributed data: ", training_time,"\n")

feature <- collect(feature)
training_time <- system.time({model <- kmeans(feature, K, algorithm = "Lloyd")})[3]
cat("training normal kmeans model on centralized data: ", training_time,"\n")

