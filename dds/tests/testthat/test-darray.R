library(Matrix)

context("DArray metadata and dmapply")

 a <- dmapply(function(x) {
             matrix(x, nrow=2, ncol=4)
             }, 1:2, output.type="DArrayClass", combine="row",nparts=c(2,1))

test_that("DArray dimensions and collect are correct",{

  expect_equal(collect(a,2),matrix(2,nrow=2, ncol=4))
  expect_equal(prod(a@nparts),2)

  expect_equal(collect(a), do.call(cbind,rep(list(c(1,1,2,2)),4)))
  expect_equal(dim(a), c(4L,4L))
})

test_that("DArray-based dmapplies work and throw errors accordingly", {

   # A DArray that has partition dimensions consistent with 'a'
   b <- dmapply(function(x) {
                  matrix(2*x+5, nrow=2, ncol=4)
               }, 1:2, output.type="DArrayClass", combine="row",nparts=c(2,1)) 

   # A DArray that has partition dimensions not consistent with 'a'
   c <- dmapply(function(x) {
                  matrix(2*x+5, nrow=4, ncol=2)
               }, 1:2, output.type="DArrayClass", combine="row",nparts=c(2,1))

   # Partition-wise addition
   d <- dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(b), MoreArgs=list(z=3), output.type="DArrayClass",combine="row",nparts=c(2,1))

   expect_equal(collect(d), do.call(cbind,rep(list(c(11,11,14,14)),4)))

   # Same partition-wise addition, this time with incompatible partitioning
   expect_error(dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(c), MoreArgs=list(z=3), output.type="DArrayClass",combine="row",nparts=c(2,1)))

})

context("Darray argument check")

mx <- matrix(c(1:16), nrow=4,ncol=4)
test_that("Darray: Invalid arguments", {
  mx <- matrix(c(1:16), nrow=4,ncol=4)
  expect_error(darray(dim=c(1,1), psize=c(2,2)))
  expect_error(darray(dim=c(10,10),psize=c(-1,5)))
  expect_error(darray(dim=c("a","b"), psize=c(4,4)))
  expect_error(darray(dim=c(1,2,3),psize=c(4,2)))
  expect_error(darray(dim=c(10,10),psize=c(10,0)))
  #expect_error(darray(wrongarg=c(2,4))) TODO: should we allow initialization from any args?
})


