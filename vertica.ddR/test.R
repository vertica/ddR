library(vertica.ddR)
library(testthat)
useBackend(vertica)
vertica.ddR.env$vertica_dsn <- src_vertica("VerticaDSN")

#system.time(test_dir("/home/dbadmin/vertica/ddR/ddR/tests/testthat"))

dim = 3
mat <- matrix(rnorm(dim*dim),dim,dim)
as.darray(mat)
