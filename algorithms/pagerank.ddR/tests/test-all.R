library(testthat)
library(ddR)
library(pagerank.ddR)
nInst = 2
useBackend(parallel,executors = nInst)
test_check("pagerank.ddR")

