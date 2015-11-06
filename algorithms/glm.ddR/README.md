---
title: "glm.ddR README"
author: "Vishrut Gupta, Arash Fard"
date: "2015-11-03"
---

### Start the parallel backend with 2 instances
```
library(ddR)
library(glm.ddR)
nInst = 2 # Change level of parallelism
useBackend(parallel,executors = nInst)
```

### Generate some data:
#### ncol =200 features, nrow ~ 100000 observations
```
ncol = 200
nrow = 100000
nrow = as.integer(nrow/nInst)
coefficients = 10*matrix(rnorm(ncol),nrow = ncol)
```

#### Generate observations of features and responses
```
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
```



### Example of dglm function using responses,predictors interface on darray data
```
training_time <- system.time({model <- dglm(responses = y, predictors = x, completeModel = TRUE)})[3]
cat("training dglm model on distributed data: ", training_time," sec \n")
prediction_time <- system.time({predictions <- predict(model, x)})[3]
cat("predicting from dglm model on same distributed data: ", prediction_time," sec \n")
```

#### Reformatting data to match formula interface on data.frame data
```
x <- collect(x)
y <- collect(y)
data <- data.frame(y = y, x)
```

### Example of glm function using formula interface on data.frame data
```
training_time <- system.time({model <- glm(y ~ ., data = data)})[3]
cat("training normal glm model on centralized data: ", training_time,"\n")

prediction_time <- system.time({predictions <- predict(model, data.frame(x))})[3]
cat("predicting from dglm model on same distributed data: ", prediction_time," sec \n")
```