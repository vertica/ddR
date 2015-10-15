context("DFrame metadata and dmapply")

 a <- dmapply(function(x) {
             data.frame(matrix(x, nrow=2, ncol=4))
             }, 1:2, output.type="dframe", combine = "rbind",nparts=c(2,1))

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
               }, 1:2, output.type="dframe", combine = "rbind",nparts=c(2,1)) 

   # A DArray that has partition dimensions not consistent with 'a'
   c <- dmapply(function(x) {
                  data.frame(matrix(2*x+5, nrow=4, ncol=2))
               }, 1:2, output.type="dframe", combine = "rbind",nparts=c(2,1))

   # Partition-wise addition
   d <- dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(b), MoreArgs=list(z=3), output.type="dframe",combine = "rbind",nparts=c(2,1))

   expect_equal(collect(d), data.frame(do.call(cbind,rep(list(c(11,11,14,14)),4))))

   # Same partition-wise addition, this time with incompatible partitioning
   expect_error(dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(c), MoreArgs=list(z=3), output.type="dframe",combine = "rbind",nparts=c(2,1)))

})

context("Invalid dframe constructor arguments")

test_that("Dframes: Invalid arguments creation", {
 expect_error(dframe(1,1))
 expect_error(dframe(dim=c(1,1),psizec(2,2)))
 expect_error(dframe(dim=c(-10,-10),psize=c(1,5)))
 expect_error(dframe(dim=c("a",1),psize=c(4,"c")))
 expect_error(dframe(dim=c(1,2,3),psize=c(4,2)))
 expect_error(dframe(dim=c(10,10),psize=c(0,10)))
 expect_error(dframe(wrongarg=c(2,4)))
 expect_error(dframe(dim=c(4,4), psize=c(2,4), sparse=TRUE))
})

context("Testing valid creation of dframes and basic checks")

test_that("Dframes: Valid argument creation",{
 mdf<- data.frame()
 nr <- 4
 nc <- 4
 df1 <- dframe(dim=c(nr,nc), psize=c(2,4))
 df1gp <- collect(df1)
 df2gp <- collect(df1,2)
 expect_error(collect(df1,3))
 expect_that(df1gp, is_a("data.frame"))
 expect_equal(nc, length(names(df1gp)))
 expect_equal(nr, length(row.names(df1gp)))
 expect_equal(4, length(names(df2gp)))
 expect_equal(2, length(row.names(df2gp)))
 expect_error(collect(df1, 0))
 expect_error(collect(df1, 3))
 expect_error(collect(df1, -100))
 expect_error(collect(df1, mdf))
 expect_error(parts(df1,0))
 expect_error(parts(df1,3))
})

context("Testing row/column name and value setup")

test_that("Dframes: Row/column name and value setup", {
  df2 <- dframe(dim=c(10,10), psize=c(4,3),data=10)
  df2gp <- collect(df2)
  df2gp3 <- collect(df2, 3)
  df2gp12 <- collect(df2, 12)
  expect_equal(10, length(names(df2gp)))
  expect_equal(10, length(row.names(df2gp)))
  expect_equal(3, length(names(df2gp3)))
  expect_equal(4, length(row.names(df2gp3)))
  expect_equal(1, length(names(df2gp12)))
  expect_equal(2, length(row.names(df2gp12)))
  expect_true(all(df2gp==10))

  wrong_name <- as.character(sample(1:9))
  expect_error(dimnames(df2)[[2]] <- wrong_name)
  expect_error(dimnames(df2)[[1]] <- wrong_name)
  wrong_name <- as.character(sample(1:11))
  expect_error(dimnames(df2)[[2]] <- wrong_name)
  expect_error(dimnames(df2)[[1]] <- wrong_name)

  name_sample <- as.character(sample(1:10))
  dimnames(df2)[[2]] <- name_sample
  expect_true(all(dimnames(df2)[[2]]==name_sample))
  df2gp <- collect(df2)
  expect_true(all(names(df2gp)==name_sample))
  df2gp6 <- collect(df2, 6)
  expect_true(all(names(df2gp6)==name_sample[4:6]))
  df2gp12 <- collect(df2, 12)
  expect_true(all(names(df2gp12)==name_sample[10]))

  dimnames(df2)[[1]] <- name_sample
  expect_true(all(dimnames(df2)[[1]]==name_sample))
  df2gp <- collect(df2)
  expect_true(all(names(df2gp)==name_sample))
  expect_true(all(row.names(df2gp)==name_sample))

  df2gp6 <- collect(df2, 6)
  expect_true(all(row.names(df2gp6)==name_sample[5:8]))
  df2gp12 <- collect(df2, 12)
  expect_true(all(row.names(df2gp12)==name_sample[9:10]))

})

context("check data.frame operations such as colSums on mixed col. types")

test_that("Dframe: check data.frame operations such as colSums on mixed col. types",{
  df4<-dmapply(function(x) {
                if(x==3) 
                  return(data.frame(v=c(1,2,3)))
                else 
                  return(data.frame(v=c(TRUE,FALSE,as.integer(4))))
               }, 1:4, output.type="dframe", combine="cbind",nparts=c(4,1))

  df4gp <- collect(df4)
  expect_equal(as.numeric(colSums(df4gp)), as.numeric(colSums(df4)))
  expect_equal(as.numeric(rowSums(df4gp)), as.numeric(rowSums(df4)))
  expect_equal(as.numeric(colMeans(df4gp)), as.numeric(colMeans(df4)))
  expect_equal(as.numeric(rowMeans(df4gp)), as.numeric(rowMeans(df4)))
})

  rblocks <- sample(1:8, 1)
  cblocks <- sample(1:8, 1)
  rsize <- sample(1:10, rblocks)
  csize <- sample(1:10, cblocks)

context("Fill a matrix partition-by-partition for later comparison with darray. We fill from left-right and then top-bottom.")

test_that("Dframe: Fill a matrix partition-by-partition for later comparison with darray. We fill from left-right and then top-bottom.",{

  mat=NULL
  matsize=NULL
  for(rid in 0:(rblocks-1)){
    cmat=NULL
    for(cid in 0:(cblocks-1)){
            index <- (rid*cblocks)+cid
            cmat <-cbind(cmat,matrix(index, nrow=rsize[floor(index/cblocks)+1],ncol=csize[(index%%cblocks)+1]))
            matsize<-rbind(matsize,c(rsize[floor(index/cblocks)+1],csize[(index%%cblocks)+1]))
    }
    mat <-rbind(mat,cmat)
  }
  mat<-data.frame(mat)

  da <- dmapply(function(index,rs,cs,cb) { 
                  data.frame(matrix(index, nrow=rs[floor(index/cb)+1],ncol=cs[(index%%cb)+1]))
                }, 1:(rblocks*cblocks)-1, MoreArgs=list(rs=rsize,cs=csize,cb=cblocks),output.type="dframe", combine = "rbind", nparts=c(rblocks,cblocks))

  cnames<-as.character(sample(1:ncol(mat)))
  expect_equal(nrow(da), nrow(mat))
  expect_equal(ncol(da), ncol(mat))
  expect_true(all(collect(da)== mat))
  expect_equal(psize(da,1), matrix(c(rsize[1],csize[1]),nrow=1))
  expect_equal(psize(da), matsize)
  colnames(da)<-cnames
  expect_true(all(colnames(collect(da))== cnames))

  expect_equal(dim(da), dim(mat))
  expect_equal(max(da), max(mat))
  expect_equal(min(da), min(mat))
  expect_equal(sum(da), sum(mat))
  expect_equal(as.numeric(colSums(da)), as.numeric(colSums(mat)))
  expect_equal(as.numeric(rowSums(da)), as.numeric(rowSums(mat)))
  expect_equal(as.numeric(rowMeans(da)), as.numeric(rowMeans(mat)))
  expect_equal(as.numeric(colMeans(da)), as.numeric(colMeans(mat)))

  expect_true(all(head(da)== head(mat)))
  expect_true(all(tail(da)== tail(mat)))
})

context("Testing as.dframe")

test_that("Dframe: Testing as.dframe",{
mtx <- matrix(c(1:100),nrow=20)

  #checking base case with giving dframe dimensions
  df <- as.dframe(mtx,psize=dim(mtx))
  expect_equal(dim(df), c(20,5))
  expect_true(all(as.matrix(collect(df))==mtx))
  expect_error(as.dframe(mtx,psize=c(20,10)))

  #checking base case without giving dframe dimensions
  df <- as.dframe(mtx)
  expect_equal(dim(df), c(20,5))
  expect_true(all(as.matrix(collect(df))==mtx))

  #testing creating of dframe with data.frame
  dfa <- c(2,3,4)
  dfb <- c("aa","bb","cc")
  dfc <- c(TRUE,FALSE,TRUE)
  df <- data.frame(dfa,dfb,dfc)

  # creating dframe with default block size
  ddf <- as.dframe(df)
  expect_equal(dim(ddf), c(3,3))
  expect_equal(colnames(ddf), colnames(df))

  # creating dframe with 1x1 block size
  ddf <- as.dframe(df,psize=c(1,1))
  expect_equal(dim(ddf), c(3,3))
  expect_equal(psize(ddf)[1,], c(1,1))
  expect_equal(colnames(ddf), colnames(df))

  # testing large darray
  large_mat <- matrix(runif(3000*3000), 3000,3000)
  df <- as.dframe(large_mat, psize=dim(large_mat))
  gpdf <- collect(df)

  expect_equal(dim(df), c(3000,3000))
  expect_equal(psize(df)[1,], c(3000,3000))
  expect_true(all(as.matrix(gpdf)== large_mat))

  df <- as.dframe(large_mat, c(2900,2800))
  gpdf <- collect(df)
  expect_true(all(as.matrix(gpdf)== large_mat))
})
