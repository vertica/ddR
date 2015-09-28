# Test the repartition.DObject function

#suppressWarnings(useBackend(distributedR))

context("Test repartitioning of DArrays")

test_that("DArrays repartition correctly",{
  
  # This should be a four-partition darray
  a <- darray(psize=c(2,2),dim=c(4,4),data=1)
  b <- darray(psize=c(4,1),dim=c(4,4),data=5)

  expect_equal(nparts(a), c(2,2))
  expect_equal(nparts(b), c(1,4))
  expect_equal(matrix(1,4,4),collect(a))
  expect_equal(matrix(5,4,4),collect(b))  

  # repartition a to be like b
  c <- repartition(a,b)

  # data should be same as a
  expect_equal(collect(c),collect(a))
  # partitioning should be same as b
  expect_equal(psize(c),psize(b))
  expect_equal(nparts(c),nparts(b))

})

context("Test repartitioning of DLists")

test_that("DLists repartition correctly",{
  
  # This should be a four-partition dlist 
  a <- dlist(1,2,3,4)
  expect_equal(totalParts(a),4L)
  
  # This should be a two-partition dlist with the same 
  # overall contents
  b <- dmapply(function(y) { 
                  y
               }
               , 1:4,nparts=2)
  expect_equal(totalParts(b),2L)
  expect_equal(collect(a),collect(b))

  # repartition a to be like b
  c <- repartition(a,b)

  expect_equal(collect(c),collect(b))
  expect_equal(collect(c,1),collect(b,1))
  expect_equal(collect(c,2),collect(b,2))
  expect_equal(dim(c),dim(b))
  expect_equal(totalParts(c),totalParts(b))

})
