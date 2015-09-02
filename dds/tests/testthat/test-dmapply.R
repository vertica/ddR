context("test dlist dmapply")

a <- dmapply(function(x) {
                 if(x==1) {
                   list(1,2,3,4)
                 } else if(x==2) {
                   list(5,6,7)
                 } else if(x==3) {
                   list(8,9)
                 } else {
                   list(10)
                 }
               }, as.list(1:4))
 
  expect_equal(nparts(a),4)
  expect_equal(collect(a),as.list(1:10))

  # dlist going from 1 to 10, equal partition 
  b <- dmapply(function(x) { 
                 start <- 2*(x-1)+1 
                 list(start,start+1) }, as.list(1:5))

  expect_equal(nparts(b),5)
  expect_equal(collect(b),as.list(1:10))

test_that("DList parts-wise dmapply works", {
  c <- dmapply(function(x) {
                 list(length(x))
               }, parts(a))

  expect_equal(nparts(c),4)
  expect_equal(collect(c),as.list(4:1))

  d <- dmapply(function(x,y,z) {
                 list(length(x) + y - z)
               }, parts(a),as.list(10:7),MoreArgs=list(z=3))

  expect_equal(nparts(d),4)
  expect_equal(collect(d),list(11,9,7,5))
}) 

test_that("DList elementwise dmapply works", {
  e <- dmapply(function(x) {
                 length(x)
               }, a)
  
  expect_equal(collect(e),as.list(rep(1,length(a))))

  f <- dmapply(function(x,y) {
                 x + y
               },a,b)

  expect_equal(collect(f),as.list(seq(2,20,by=2)))
}) 
 
# TODO(etduwx): add more tests
context("test multimodal (mixture of different dobject types) dmapply")

  # Two partitions, going from 1 to 4, 2 elements each
  test_dlist <- dmapply(function(x) {
                  start <- 2*(x-1)+1
                  list(start,start+1) 
                }, as.list(1:2))

  # Two partitions, going from 1 to 4, 1 row (2 elem) each
  test_darray <- dmapply(function(x) {
                   start <- 2*(x-1)+1
                   t(as.matrix(c(start,start+1)))
                }, as.list(1:2),FUN.VALUE=matrix())

  # Two partitions, going from 1 to 8, 1 row (4 elem) each
  test_dframe <- dmapply(function(x) {
                   start <- 4*(x-1)+1
                   end <- start + 3 
                   data.frame(t(as.matrix(start:end)))
                }, as.list(1:2),FUN.VALUE=data.frame())

test_that("parts-wise multimodal dmapply works", {
  answer <- dmapply(function(x,y,z) {
                      list(is.list(x),is.matrix(y),is.data.frame(z),
                           length(x), sum(y), sum(z))
                    }, parts(test_dlist), parts(test_darray), parts(test_dframe))

  expect_equal(collect(answer),list(TRUE,TRUE,TRUE,2,3,10,TRUE,TRUE,TRUE,2,7,26))
})

# We will apply columnwise for test_dframe, elementWise (column-major order) for test_darray
test_that("element-wise multimodal dmapply works", {
  answer <- dmapply(function(x,y,z) {
                      x + y + sum(z)
                    }, test_dlist, test_darray, test_dframe)

  expect_equal(collect(answer),list(8,13,15,20))
})
