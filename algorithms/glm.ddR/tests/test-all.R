library(testthat)
library(ddR)
library(glm.ddR)
nInst = 2
useBackend(parallel,executors = nInst)
test_check("glm.ddR")

