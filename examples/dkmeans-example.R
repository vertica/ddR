library(ddR)
library(kmeans.ddR)

set.seed(319804)

N_EXEC = 2 # Change level of parallelism
useBackend(parallel, executors = N_EXEC)
# Uncomment the following lines to use Distributed R 
#library(distributedR.ddR)
#useBackend(distributedR)

# Set up data size
NCOL = 8 # Must be at least 2
NROW = as.integer(2e5)
K = 6
ITERMAX = 300

# The true centers for the data generating process
#centers = matrix(rnorm(K * NCOL, sd = 10), nrow = K)
# Easy to check correctness if we know where these centers should be
centers = cbind(10 * seq.int(K), matrix(0, nrow = K, ncol = NCOL - 1))
dnumrows = as.integer(NROW/N_EXEC)

generateKMeansData <- function(id, centers, nrow, ncol) {
    offsets = matrix(rnorm(nrow * ncol), nrow = nrow, ncol = ncol)
    cluster_ids = sample.int(K, size = nrow, replace = TRUE)
    feature_obs = centers[cluster_ids, ] + offsets
    feature_obs
}

cat(sprintf("Generating %d x %d matrix for clustering with %d means\n",
            NROW, NCOL, K))

dfeature <- dmapply(generateKMeansData, id = 1:N_EXEC,
  MoreArgs = list(centers = centers, nrow = dnumrows, ncol = NCOL),
		output.type = "darray", 
		combine = "rbind", nparts = c(N_EXEC,1))

cat("training dkmeans model on distributed data\n")
dtraining_time <- system.time(
    dmodel <- dkmeans(dfeature, K, iter.max = ITERMAX)
)[3]
cat(dtraining_time, "\n")

feature <- collect(dfeature)
cat("training kmeans model on centralized data\n")
training_time <- system.time(
    model <- kmeans(feature, K, iter.max = ITERMAX, algorithm = "Lloyd")
)[3]
cat(training_time, "\n")

# Compare results
sortfirstcol = function(m) m[order(m[, 1]), ]

allcenters = lapply(list(true = centers, 
                         distributed = dmodel$centers,
                         centralized = model$centers),
                    sortfirstcol)

print(allcenters)

# TODO: shutdown
