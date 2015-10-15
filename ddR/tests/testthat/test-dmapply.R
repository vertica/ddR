context("test dlist dmapply")

  # Initialize a dlist going from 1 to 10, equal partitions of 2 each
  a <- dmapply(function(x) x, 1:10,nparts=5)
 
  expect_equal(totalParts(a),5)
  expect_equal(collect(a),as.list(1:10))

  # dlist going from 1 to 10, 10 partitions 
  b <- dmapply(function(x) x, 5:14,nparts=10)

  expect_equal(totalParts(b),10)
  expect_equal(collect(b),as.list(5:14))

test_that("DList parts-wise dmapply works", {
  c <- dmapply(function(x) {
                 length(x)
               }, parts(a))

  expect_equal(totalParts(c),5)
  expect_equal(collect(c),as.list(rep(2,5)))

  d <- dmapply(function(x,y,z) {
                 length(x) + y - z
               }, parts(a),as.list(10:6),MoreArgs=list(z=3))

  expect_equal(totalParts(d),5)
  expect_equal(collect(d),as.list(9:5))
}) 

test_that("DList elementwise dmapply works", {
  e <- dmapply(function(x) {
                 length(x)
               }, a)
  
  expect_equal(collect(e),as.list(rep(1,length(a))))

  f <- dmapply(function(x,y) {
                 x + y
               },a,b)

  expect_equal(collect(f),as.list(seq(6,24,by=2)))
}) 
 
# TODO(etduwx): add more tests
context("test multimodal (mixture of different dobject types) dmapply")

  # Two partitions, going from 1 to 4, 2 elements each
  test_dlist <- dmapply(function(x) x, 1:4, nparts=2)

  # Two partitions, going from 1 to 4, 1 row (2 elem) each
  test_darray <- dmapply(function(x) {
                   start <- 2*(x-1)+1
                   t(as.matrix(c(start,start+1)))
                }, output.type="darray",1:2,combine = "rbind",nparts=c(2,1))

  # Two partitions, going from 1 to 8, 1 row (4 elem) each
  test_dframe <- dmapply(function(x) {
                   start <- 4*(x-1)+1
                   end <- start + 3 
                   data.frame(t(as.matrix(start:end)))
                }, output.type="dframe",1:2,combine = "rbind",nparts=c(2,1))

test_that("parts-wise multimodal dmapply works", {
  answer <- dmapply(function(x,y,z) {
                      list(is.list(x),is.matrix(y),is.data.frame(z),
                           length(x), sum(y), sum(z))
                    }, parts(test_dlist), parts(test_darray), parts(test_dframe))

  expect_equal(collect(answer),list(list(TRUE,TRUE,TRUE,2,3,10), list(TRUE,TRUE,TRUE,2,7,26)))

  #Now check the case with "combine=c"
  answer <- dmapply(function(x,y,z) {
                      list(is.list(x),is.matrix(y),is.data.frame(z),
                           length(x), sum(y), sum(z))
                    }, parts(test_dlist), parts(test_darray), parts(test_dframe), combine="c")

  expect_equal(collect(answer),list(TRUE,TRUE,TRUE,2,3,10, TRUE,TRUE,TRUE,2,7,26))
})

# We will apply columnwise for test_dframe, elementWise (column-major order) for test_darray
test_that("element-wise multimodal dmapply works", {
  answer <- dmapply(function(x,y,z) {
                      x + y + sum(z)
                    }, test_dlist, test_darray, test_dframe)

  expect_equal(collect(answer),list(8,13,15,20))
})

  foo <- data.frame(cbind(c(1,2),c(4,5),c(7,8),c(10,11)))
  bar <- list(13,14,15,16)
  baz <- cbind(c(1,2),c(3,4))

  # these mimic the test_dobjects above, but in vanilla form
  tlist <- list(1,2,3,4)
  tframe <- data.frame(rbind(c(1,2,3,4),c(5,6,7,8)))
  tarray <- rbind(c(1,2),c(3,4))
  
# When standard R objects (lists,arrays, and/or data.frames) are 
# passed into dmapply (with or without other dobject inputs) -- 
# output should be DObject but behavior should be the same
test_that("element-wise dmapply works with vanilla-R variables", {
  
  # Using standard mapply
  vanilla_answer1 <- mapply(function(x,y,z) {
                               x + y + z
                            }, foo, bar, baz, SIMPLIFY=FALSE)

  # Using dmapply
  test_answer1 <- dmapply(function(x,y,z) {
                               x + y + z
                            }, foo, bar, baz)

  expect_equal(collect(test_answer1),vanilla_answer1)

  vanilla_answer2 <- mapply(function(a,b,c,d,e,f) {
                       a + b + c + d + e + f
                     }, foo, bar, baz, 
                      tlist,tframe,tarray, SIMPLIFY=FALSE)

  test_answer2 <- dmapply(function(a,b,c,d,e,f) {
                      a + b + c + d + e + f
                     }, foo, bar, baz,
                        test_dlist,
                        test_dframe,
                        test_darray)
  
  expect_equal(collect(test_answer2),vanilla_answer2)
  
})

context("darray dmapply tests")

library(Matrix)

test_that("dmapply with dense darray: works", {
  da <- dmapply(function(x) matrix(5,10,1), 1:1, output.type="darray", combine = "rbind",nparts=c(1,1))
  db <- dmapply(function(x) matrix(10,1,10), 1:1, output.type="darray", combine = "rbind",nparts=c(1,1))

  expect_equal(dim(da), c(10,1))
  expect_equal(nparts(da), c(1,1))
  expect_equal(dim(db), c(1,10))
  expect_equal(nparts(db), c(1,1))
  expect_equal(collect(da),matrix(5,10,1))
  expect_equal(collect(db),matrix(10,1,10))
})

test_that("dmapply with sparse matrices : works", {
  w <- runif(15)
  vNum <- 10
  nColBlock <- 3
  el <- matrix(nrow=15, ncol=2, c(1,1,1,2,2,2,3,3,4,4,4,4,5,7,9,6,9,2,5,7,3,4,8,1,2,6,9,3,2,1))
  wGF <- dmapply(function(i,el,v,w) { 
                     library(Matrix) 
                     sparseMatrix(i=el[,1],j=el[,2], dims=c(v,v), x=w)
                     }, 1, output.type="sparse_darray",combine = "rbind",MoreArgs=list(el=el,w=w,v=vNum))

  y <- sparseMatrix(i=el[,1], j=el[,2], dims=c(vNum,vNum), x=w)
  gy <- collect(wGF)
  expect_equal(y, gy)

})

context("dmapply error checking")

a <- darray(nparts=5)

test_that("Errors are thrown when they are supposed to be", {
  expect_error(dmapply(1:10))
  expect_error(dmapply(NULL,"a"))
  expect_error(dmapply(function(x) x, parts(a,7)))

  expect_error(dmapply(function(x) matri(5,2,4), 1,output.type="darray"))
})

context("grouping partitions in dmapply")

a <- darray(data=2,psize=c(2,2),dim=c(4,4))

test_that("Partitions can be grouped in a list", {
  b <- dlapply(list(parts(a,1:3),list(), parts(a,4)), function(x) do.call(rbind,x))
  expect_equal(totalParts(b),3)
  expect_equal(collect(b,1)[[1]],matrix(2,6,2))
  expect_null(collect(b,2)[[1]])
  expect_equal(collect(b,3)[[1]],matrix(2,2,2))
})
