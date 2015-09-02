context("DArray metadata and dmapply")

 a <- dmapply(function(x) {
             matrix(x, nrow=2, ncol=4)
             }, as.list(1:2), FUN.VALUE=matrix())

test_that("An error is thrown when there is a data-type mismatch", {
  expect_error(dmapply(function(x) {
                 data.frame()
               }, as.list(1:2), FUN.VALUE=matrix()))
})

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
               }, as.list(1:2), FUN.VALUE=matrix())

   # A DArray that has partition dimensions not consistent with 'a'
   c <- dmapply(function(x) {
                  matrix(2*x+5, nrow=4, ncol=2)
               }, as.list(1:2), FUN.VALUE=matrix())

   # Partition-wise addition
   d <- dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(b), MoreArgs=list(z=3), FUN.VALUE=matrix())

   expect_equal(collect(d), do.call(cbind,rep(list(c(11,11,14,14)),4)))

   # Same partition-wise addition, this time with incompatible partitioning
   expect_error(dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(c), MoreArgs=list(z=3), FUN.VALUE=matrix()))


   # However, element-wise addition should still work
#   e <- dmapply(function(x,y,z) {
#                  x + y + z
#                }, a,c, MoreArgs=list(z=3), FUN.VALUE=matrix())

#   expect_equal(collect(d), do.call(cbind,rep(list(c(11,11,14,14)),4)))
})
