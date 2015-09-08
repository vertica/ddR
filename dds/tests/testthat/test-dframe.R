context("DFrame metadata and dmapply")

 a <- dmapply(function(x) {
             data.frame(matrix(x, nrow=2, ncol=4))
             }, 1:2, output.type="DFrameClass", combine="row",nparts=c(2,1))

test_that("DFrame dimensions and collect are correct",{

  expect_equal(collect(a,2),data.frame(matrix(2,nrow=2, ncol=4)))
  expect_equal(prod(a@nparts),2)

  expect_equal(collect(a), data.frame(do.call(cbind,rep(list(c(1,1,2,2)),4))))
  expect_equal(dim(a), c(4L,4L))
})

test_that("DArray-based dmapplies work and throw errors accordingly", {

   # A DArray that has partition dimensions consistent with 'a'
   b <- dmapply(function(x) {
                  data.frame(matrix(2*x+5, nrow=2, ncol=4))
               }, 1:2, output.type="DFrameClass", combine="row",nparts=c(2,1)) 

   # A DArray that has partition dimensions not consistent with 'a'
   c <- dmapply(function(x) {
                  data.frame(matrix(2*x+5, nrow=4, ncol=2))
               }, 1:2, output.type="DFrameClass", combine="row",nparts=c(2,1))

   # Partition-wise addition
   d <- dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(b), MoreArgs=list(z=3), output.type="DFrameClass",combine="row",nparts=c(2,1))

   expect_equal(collect(d), data.frame(do.call(cbind,rep(list(c(11,11,14,14)),4))))

   # Same partition-wise addition, this time with incompatible partitioning
   expect_error(dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(c), MoreArgs=list(z=3), output.type="DFrameClass",combine="row",nparts=c(2,1)))

})
