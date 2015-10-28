library(testthat)
library(ddR)
library(kmeans.ddR)
nInst = 2
useBackend(parallel,executors = nInst)
test_check("kmeans.ddR")

