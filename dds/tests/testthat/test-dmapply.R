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
                 as.list(length(x))
               }, a)
  
  expect_equal(collect(e),as.list(rep(1,length(a))))

  f <- dmapply(function(x,y) {
                 as.list(x + y)
               },a,b)

  expect_equal(collect(f),as.list(seq(2,20,by=2)))

}) 
 
