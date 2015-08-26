useBackend(distributedR)

context("DList metadata")

test_that("DList dimensions are correct",{
  a <- dlist(0,1,2,3,c(4,5),list(6,7,8),9)
  dims <- dim(a)

  expect_equal(length(dims),1)
  expect_equal(dims,7)
})

context("dmapply return-type checking")

test_that("An error is thrown when the return type does not match FUN.VALUE",{
  # By default, FUN.VALUE is a list()
  expect_error(dmapply(function(x) {
                         return(2L)
                       }, 
                       as.list(1:3)))
})
