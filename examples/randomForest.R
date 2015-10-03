library(dds)
library(dds.randomForest)
nInst = 4 # Change level of parallelism

# Uncomment the following lines to use Distributed R
#library(distributedR.dds)
#useBackend(distributedR)

# Set up data size
ncol = 10
nrow = 1000
K = 10
centers = 100*matrix(rnorm(K*ncol),nrow = K)
cat("Generating data with rows=",nrow," and cols=", ncol,"\n")
nrow = as.integer(nrow/nInst)

generateRFData <- function(id, centers, nrow, ncol) {
offsets = matrix(rnorm(nrow*ncol),nrow = nrow,ncol = ncol)
cluster_ids = sample.int(nrow(centers),nrow,replace = TRUE)
feature_obs = centers[cluster_ids,] + offsets
features <- cbind(cluster_ids, feature_obs)
}


features <- dmapply(generateRFData,id = 1:nInst,
                MoreArgs = list(centers = centers, nrow = nrow, ncol = ncol),
		output.type = "dframe", 
		combine = "rbind", nparts = c(nInst,1))
colnames(features) <- paste("X",1:ncol(features),sep="")



cat("\nStarting random forest using formula interface:")
training <- system.time(model <- hpdRF_parallelForest(X1 ~ ., features))[3]
predicting <- system.time(predictions <- predict(model, features))[3]
cat("\n\ttraining model took: \t", training," seconds \n\tpredictions took: \t",predicting," seconds\n")

cat("\nStarting random forest using x,y darray interface")
features_x = dmapply(function(x) x[,-1], parts(features),
		output.type = "darray", combine = "rbind", 
		nparts = nparts(features))
features_y = dmapply(function(x) matrix(x[,1],ncol = 1), parts(features),
		output.type = "darray", combine = "rbind", 
		nparts = nparts(features))

training <- system.time(model <- hpdRF_parallelForest(x = features_x, y = features_y))[3]
predicting <- system.time(predictions <- predict(model, features_x))[3]
cat("\n\ttraining model took: \t", training," seconds\n\tpredictions took: \t",predicting," seconds\n")

cat("\nStarting random forest using x,y matrix interface")
features_x = collect(features_x)
features_y = collect(features_y)
training <- system.time(model <- hpdRF_parallelForest(x = features_x, y = features_y))[3]
predicting <- system.time(predictions <- predict(model, features_x))[3]
cat("\n\ttraining model took: \t", training," seconds\n\tpredictions took: \t",predicting," seconds\n")

