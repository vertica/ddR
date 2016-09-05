# To make sure that this works on the other possible types of snow clusters
# we should run all the tests on each of the cluster types.
# This file is a non automated way to do that, ie call
# > source("snow.R")
# from an R session since you'll need to provide passwords.
# If the script doesn't throw any errors then it worked.

library(ddR)
library(testthat)

# PSOCK cluster on multiple machines
# Change this to the DNS names of the slave machines
hostnames = c("server1", "server2")
#b = useBackend("parallel", hostnames, type="PSOCK")

# MPI cluster
b = useBackend("parallel", 2, type="MPI")

source("test-dmapply.R")
source("test-dlist.R")
source("test-darray.R")
source("test-dframe.R")
source("test-repartition.R")
