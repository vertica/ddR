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
  expect_error(darray(wrongarg=c(2,4))) 
})


context("Dense Darray")

da2 <- darray(dim=c(4,4), psize=c(2,4), data=1)
da3 <- darray(dim=c(4,4), psize=c(4,2), data=2)

test_that("Create and Fetch works", {
  da2gp <- collect(da2)
  da2gp2 <- collect(da2, 2)
  da3gp2 <- collect(da3, 2)

  expect_that(da2gp, is_a("matrix"))
  expect_equal(c(4,4), dim(da2gp))
  expect_equal(da2gp[4],1)
  expect_equal(c(2,4), dim(da2gp2))
  expect_equal(2, totalParts(da2))

  expect_equal(2, totalParts(da3))
  expect_equal(da3gp2[2],2)
  expect_equal(c(4,2), dim(da3gp2))

})

context("as.darray()")

mtx<-matrix(c(1:100), nrow=20)

test_that("Column-partitioned darray: works", {
  da<-as.darray(mtx, psize=dim(mtx))  
  expect_equal(dim(da), c(20,5))
  expect_equal(psize(da), c(20,5))
  expect_equal(collect(da), mtx)

  da<-as.darray(mtx,c(20,1))
  expect_equal(dim(da), c(20,5))
  expect_equal(psize(da)[1,], c(20,1))
  expect_equal(as.numeric(collect(da,2)), c(21:40))
  expect_equal(as.matrix(collect(da)), mtx)
})

test_that("Row-partitioned darray: works", {
  da<-as.darray(mtx,c(5,5))
  expect_equal(dim(da), c(20,5))
  expect_equal(psize(da)[1,], c(5,5))
  expect_equal(as.numeric(collect(da,3)), c(11:15, 31:35, 51:55, 71:75, 91:95))
  expect_equal(as.matrix(collect(da)), mtx)
})

test_that("Block-partitioned darray: works", {
  da<-as.darray(mtx, c(3,3))
  expect_equal(dim(da), c(20,5))
  expect_equal(psize(da)[1,], c(3,3))
  expect_equal(as.matrix(collect(da,2)), matrix(c(61:63,81:83), nrow=3))
  expect_equal(as.matrix(collect(da,13)), matrix(c(19,20,39,40,59,60), nrow=2))
  expect_equal(as.matrix(collect(da,14)), matrix(c(79,80,99,100), nrow=2))
  expect_equal(as.matrix(collect(da)), mtx)
})

test_that("More tests", {
  da<-as.darray(mtx, c(2,5))
  expect_equal(dim(da), c(20,5))
  expect_equal(psize(da)[1,], c(2,5))
  expect_equal(as.matrix(collect(da,10)), matrix(c(19,20,39,40,59,60,79,80,99,100), nrow=2))
  expect_equal(as.matrix(collect(da)), mtx)
  da<-as.darray(mtx, c(20,1))
  expect_equal(dim(da), c(20,5))
  expect_equal(psize(da)[1,], c(20,1))
  expect_equal(as.matrix(collect(da,3)), matrix(c(41:60), ncol=1))
  expect_equal(as.matrix(collect(da)), mtx)
})

test_that("Expected errors returned", {
  da<-darray(dim=c(10,5), psize=c(2,2), data=0)
  expect_error(as.darray(mtx, c(30,1))) #desired block dimension does not match
  expect_error(as.darray(mtx, c(20,10))) #darray block dim does not match
  expect_error(as.darray(mtx, c(20,10)))
})

test_that("as.darray support for input sparse matrix", {
  input1 <- Matrix(data=10, nrow=5, ncol=4, sparse=TRUE)
  input2 <- Matrix(data=10, nrow=5, ncol=5, sparse=TRUE)

  output <- as.darray(input1, psize=c(1,4))
  expect_equal(dim(output), c(5,4))
  expect_equal(psize(output)[1,], c(1,4))
  #expect_equal(output@sparse, TRUE)

  output <- as.darray(input2, psize=c(1,5))
  expect_equal(dim(output), c(5,5))
  expect_equal(psize(output)[1,], c(1,5))
  #expect_equal(output@sparse, TRUE)
})

context("as.darray() with large data")

large_mat <- matrix(runif(3000*3000), 3000, 3000)
test_that("Dense darrays: works", {
  da<-as.darray(large_mat, psize=dim(large_mat))
  gpa <- collect(da)

  expect_equal(dim(da), c(3000,3000))
  expect_equal(psize(da), c(3000,3000))
  expect_equal(gpa, large_mat)

  da<-as.darray(large_mat,c(2900,2800))  # mixture of external transfer and protobuf msg
  gpa <- collect(da)
  expect_equal(gpa, large_mat)
})


context("Partitionsize")

r <- sample(1:100, 1)
c <- sample(1:100, 1)
rpart <- sample(1:r, 1)
cpart <- sample(1:c, 1)
test_that("Dense darrays: works", {
  da<-darray(dim=c(r,c), psize=c(rpart, cpart))
  expect_equal(nrow(da), r, info="check darray nrow")
  expect_equal(ncol(da), c, info="check darray col")
  expect_equal(psize(da,1), c(rpart,cpart), info="check size of partition 1")
})

context("Invalid darrays")
val<-runif(1,0:1)
mat<-array(val, dim=c(r,c))

test_that("Dense darrays: works", {
  da<-darray(dim=c(r,c), psize=c(rpart, cpart), data=val)

  expect_equal(collect(da), mat, info="check uninitialized darray")
  expect_equal(nrow(da), r, info="check darray nrow")
  expect_equal(ncol(da), c, info="check darray col")
})


