# Code to test the entire package as documented here:
# http://stackoverflow.com/questions/17595796/how-to-setup-testthat-for-r-cmd-check

library(testthat)
library(ddR)
useBackend(parallel, executors=2)
test_check("ddR")
