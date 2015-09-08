context("DList metadata")

test_that("DList dimensions are correct",{
## TODO: Figure out why this is needed. For some reason, the first time this is invoked, 
## it doesn't work, but the second time it does.
## See: http://r.789695.n4.nabble.com/Reference-class-finalize-fails-with-attempt-to-apply-non-function-td4174697.html
#  a <- try(dlist(0,1,2,3,c(4,5),list(6,7,8),9),silent=TRUE)
#  a <- try(dlist(0,1,2,3,c(4,5),list(6,7,8),9),silent=TRUE)
#  dims <- dim(a)

#  expect_equal(length(dims),1)
#  expect_equal(dims,7)
})
