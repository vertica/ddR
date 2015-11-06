library(HPdregression)
library(ddR)

nInst = 2 # Change level of parallelism
useBackend(parallel,executors = nInst)
# Uncomment the following lines to use Distributed R 
#library(distributedR.ddR)
#useBackend(distributedR)

# Set up data size
ncol = 200
nrow = 1000000
coefficients = 10*matrix(rnorm(ncol),nrow = ncol)
cat("Generating data with rows=",nrow," and cols=", ncol,"\n")
nrow = as.integer(nrow/nInst)

generateGLMFeatures <- function(id, nrow, ncol) {
	matrix(rnorm(nrow*ncol),nrow = nrow,ncol = ncol)
}
generateGLMResponses <- function(features, coefficients) {
	matrix(features %*% coefficients, ncol = 1)
}

x <- dmapply(generateGLMFeatures,id = 1:nInst,
                MoreArgs = list(nrow = nrow, ncol = ncol),
		output.type = "darray", 
		combine = "rbind", nparts = c(nInst,1))
y <- dmapply(generateGLMResponses,features = parts(x),
                MoreArgs = list(coefficients = coefficients),
		output.type = "darray", 
		combine = "rbind", nparts = c(nInst,1))

training_time <- system.time({model <- hpdglm(responses = y, predictors = x, completeModel = TRUE)})[3]
cat("training dglm model on distributed data: ", training_time," sec \n")
prediction_time <- system.time({predictions <- predict(model, x)})[3]
cat("predicting from dglm model on same distributed data: ", prediction_time," sec \n")


x <- collect(x)
y <- collect(y)
data <- data.frame(y = y, x)

training_time <- system.time({model <- glm(y ~ ., data = data)})[3]
cat("training normal glm model on centralized data: ", training_time,"\n")

prediction_time <- system.time({predictions <- predict(model, data.frame(x))})[3]
cat("predicting from dglm model on same distributed data: ", prediction_time," sec \n")
