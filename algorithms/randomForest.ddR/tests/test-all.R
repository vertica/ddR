library(testthat)
library(ddR)
library(randomForest.ddR)
library(randomForest)
nInst = 2
useBackend(parallel,executors = nInst)
test_check("randomForest.ddR")

