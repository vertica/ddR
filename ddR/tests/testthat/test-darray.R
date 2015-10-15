library(Matrix)

context("DArray metadata and dmapply")

 a <- dmapply(function(x) {
             matrix(x, nrow=2, ncol=4)
             }, 1:2, output.type="darray", combine = "rbind",nparts=c(2,1))

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
               }, 1:2, output.type="darray", combine = "rbind",nparts=c(2,1)) 

   # A DArray that has partition dimensions not consistent with 'a'
   c <- dmapply(function(x) {
                  matrix(2*x+5, nrow=4, ncol=2)
               }, 1:2, output.type="darray", combine = "rbind",nparts=c(2,1))

   # Partition-wise addition
   d <- dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(b), MoreArgs=list(z=3), output.type="darray",combine = "rbind",nparts=c(2,1))

   expect_equal(collect(d), do.call(cbind,rep(list(c(11,11,14,14)),4)))

   # Same partition-wise addition, this time with incompatible partitioning
   expect_error(dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(c), MoreArgs=list(z=3), output.type="darray",combine = "rbind",nparts=c(2,1)))

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
  expect_equal(psize(da)[1,], c(20,5))
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
  expect_equal(psize(da)[1,], c(3000,3000))
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
  expect_equal(psize(da)[1,], c(rpart,cpart), info="check size of partition 1")
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


context("Dense darray declared with nparts")

rblocks <- sample(1:8, 1)
cblocks <- sample(1:8, 1)
rsize <- sample(1:10, rblocks)
csize <- sample(1:10, cblocks)

da<-darray(nparts=c(rblocks,cblocks))
db<-darray(nparts=c(rblocks,cblocks))

test_that("Creation and Fetch works", {
  expect_equal(nrow(da), 0, info="check darray nrow")
  expect_equal(ncol(da), 0, info="check darray col")
})

#Update flex darray
rblocks<-8
cblocks<-2
rsize<-c(9, 1, 2, 4, 7, 5, 6, 3)
csize<-c(10,7)

mat=NULL
  mat2=NULL
  matsize=NULL
  for(rid in 0:(rblocks-1)){
    cmat=NULL
    cmat2=NULL
    for(cid in 0:(cblocks-1)){
            index <- (rid*cblocks)+cid

            cmat <-cbind(cmat,matrix(index, nrow=rsize[floor(index/cblocks)+1],ncol=csize[(index%%cblocks)+1]))
            matsize<-rbind(matsize,c(rsize[floor(index/cblocks)+1],csize[(index%%cblocks)+1]))

            #For mat2
            v<-NULL
            if(index%%3 ==0){v<-TRUE}
            if(index%%3 ==1){v<-NA}
            if(index%%3 ==2){v<-22}
            cmat2 <-cbind(cmat2,matrix(v, nrow=rsize[floor(index/cblocks)+1],ncol=csize[(index%%cblocks)+1]))
    }
    mat <-rbind(mat,cmat)
    mat2 <-rbind(mat2,cmat2)
}

da <- dmapply(function(index, rs, cs, cb) {
      	     matrix(index, nrow=rs[floor(index/cb)+1],ncol=cs[(index%%cb)+1])
             }, 0:((rblocks*cblocks)-1), MoreArgs=list(rs=rsize, cs=csize, cb=cblocks), output.type="darray", combine = "rbind",nparts=c(rblocks,cblocks))

db <- dmapply(function(index, rs, cs, cb) {
     v<-NULL
     if(index%%3 ==0){v<-TRUE}
     if(index%%3 ==1){v<-NA}
     if(index%%3 ==2){v<-22}
     matrix(v, nrow=rs[floor(index/cb)+1],ncol=cs[(index%%cb)+1])
     }, 0:((rblocks*cblocks)-1), MoreArgs=list(rs=rsize, cs=csize, cb=cblocks), output.type="darray", combine = "rbind",nparts=c(rblocks,cblocks))

test_that("Creation and Fetch works", {
  expect_equal(nrow(da), nrow(mat), info="check nrow of flex darray")
  expect_equal(ncol(da), ncol(mat), info="check ncol of flex darray")
  expect_equal(collect(da), mat, info="check flex darray contents")
  expect_equal(psize(da,1), matrix(c(rsize[1],csize[1]),nrow=1), info="check size of first partition")
  expect_equal(psize(da), matsize, info="check size of all partitions")
  expect_equal(dim(da), dim(mat), info="check dimension of flex darray")
})

context("Dense darray when passed in MoreArgs")
test_that("MoreArgs works", {
  L1 <- dmapply(function(x,y){list(d = dim(x), m = median(x))}, 1, MoreArgs = list(x=da))

  expect_equal(collect(L1)[[1]]$d, dim(mat), info="check dim of darray with MoreArgs")
  expect_equal(collect(L1)[[1]]$m, median(mat), info="check median of darray with MoreArgs")
})


context("Dense darray ops when declared with nparts")

test_that("Operatons: max, min, head, tail works", {
  expect_equal(max(da), max(mat), info="check max of flex darray")
  expect_equal(min(da), min(mat), info="check min of flex darray")
  expect_equal(sum(da), sum(mat), info="check sum of flex darray")
  expect_equal(mean(da), mean(mat), info="check mean of flex darray")
  expect_equal(colSums(da), colSums(mat), info="check colSums of flex darray")
  expect_equal(rowSums(da), rowSums(mat), info="check rowSums of flex darray")
  expect_equal(colMeans(da), colMeans(mat), info="check colMeans of flex darray")
  expect_equal(rowMeans(da), rowMeans(mat), info="check rowMeans of flex darray")
  expect_equal(head(da), Matrix::head(mat), info="check head operator on flex darray")
  expect_equal(as.numeric(tail(da)), as.numeric(Matrix::tail(mat)), info="check tail operator on flex darray")
  #expect_equal(norm(da), norm(mat,"F"), info="check norm operator on flex darray")
})


val<-42
mat2<-array(val, dim=c(nrow(mat),ncol(mat)))

d1 <- dmapply(function(y, v) {
            matrix(v, nrow=nrow(y),ncol=ncol(y))
             }, parts(da), MoreArgs=list(v=val), output.type="darray", combine = "rbind",nparts=nparts(da))

#test_that("Operations: sum, minus works", {
#  expect_equal(collect(da1+da1), (mat+mat), info="check self + operator on flex darray")
#  expect_equal(collect(da), mat2, info="check flex darray contents after update to full array")
#  expect_equal(collect(da+da1), (mat+mat2), info="check + operator on flex darray")
#  expect_equal(collect(da1-da), (mat-mat2), info="check - operator on flex darray")
#  expect_equal(collect(da1+22), (mat+22), info="check - operator on flex darray")
#})


context("Sparse darray with nparts")

rblocks <- sample(1:8, 1)
cblocks <- sample(1:8, 1)
rsize <- sample(1:10, rblocks)
csize <- sample(1:10, cblocks)

da <- dmapply(function(index, rs, cs, cb) {
        library(Matrix)
        nrow=rs[floor(index/cb)+1]
        ncol=cs[(index%%cb)+1]
        y<-sparseMatrix(i=1,
                        j=1,
                        x=index+1, 
                        dims=c(nrow,ncol))
             }, 0:((rblocks*cblocks)-1), MoreArgs=list(rs=rsize, cs=csize, cb=cblocks), output.type="sparse_darray", combine = "rbind",nparts=c(rblocks,cblocks))


rindex<-c(1,(cumsum(rsize)+1)[1:length(rsize)-1])
rindex<-rep(rindex, each=cblocks)
cindex<-c(1,(cumsum(csize)+1)[1:length(csize)-1])
cindex<-rep(cindex, rblocks)
mat<- sparseMatrix(i=rindex,
                        j=cindex,
                        x=1:length(rindex), 
                        dims=c(sum(rsize),sum(csize)))

test_that("Sparse creation and Fetch works", {
  expect_equal(nrow(da), nrow(mat), info="check nrow of flex sparse darray")
  expect_equal(ncol(da), ncol(mat), info="check ncol of flex sparse darray")
  expect_equal(collect(da), mat, info="check flex sparse darray contents")
  expect_equal(dim(da), dim(mat), info="check dimension of flex sparse darray")
  expect_equal(dim(da), dim(mat), info="check dimension of flex sparse darray")
})

context("Sparse darray ops with nparts")

test_that("Operatons: max, min, head, tail works", {
  expect_equal(max(da), max(mat), info="check max of flex sparse darray")
  expect_equal(min(da), min(mat), info="check min of flex sparse darray")
  expect_equal(sum(da), sum(mat), info="check sum of flex sparse darray")
  expect_equal(mean(da), mean(mat), info="check mean of flex sparse darray")
  expect_equal(colSums(da), colSums(mat), info="check colSums of flex sparse darray")
  expect_equal(rowSums(da), rowSums(mat), info="check rowSums of flex sparse darray")
  expect_equal(colMeans(da), colMeans(mat), info="check colMeans of flex sparse darray")
  expect_equal(rowMeans(da), rowMeans(mat), info="check rowMeans of flex sparse darray")
  expect_equal(head(da), Matrix::head(mat), info="check head operator on flex sparse darray")
  expect_equal(as.numeric(tail(da)), as.numeric(Matrix::tail(mat)), info="check tail operator on flex sparse darray")
})
