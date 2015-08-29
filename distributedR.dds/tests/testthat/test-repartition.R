# Test the repartition.DObject function

#suppressWarnings(useBackend(distributedR))

context("Test repartitioning of DArrays")

test_that("DArrays repartition correctly",{
  
  # This should be a four-partition darray
  a <- darray(psize=c(3,1),dim=c(3,4))

  ## Cheat...keep the same partition layout in the dobject by using foreach API
  foreach(i,1:nparts(a),function(a=splits(a@DRObj,i),index=i) {
    a <- matrix(index,3,1)
    update(a)
  }, progress=FALSE)

  expect_equal(nparts(a),4L)
  expect_equal(matrix(1,3,1),collect(a,1))
  
  b <- darray(psize=c(3,2),dim=c(3,4))
  
  foreach(i,1:nparts(b),function(b=splits(b@DRObj,i),index=i) {
    value <- (index-1)*2+1
    b <- cbind(rep(value,3),rep(value+1,3))
    update(b)
  }, progress=FALSE)

  expect_equal(nparts(b),2L)
  expect_equal(cbind(rep(1,3),rep(2,3)),collect(b,1))

  expect_equal(collect(a),collect(b))

  ## Note: the goal is to have *each* partition of a match *each* partition of b, not to have the whole
  #  2d-structure of the overall objects match!

  # repartition a to be like b
  c <- repartition(a,b)

  expect_equal(collect(c,1),collect(b,1))
  expect_equal(collect(c,2),collect(b,2))
  expect_equal(nparts(c),nparts(b))

})

context("Test repartitioning of DLists")

test_that("DLists repartition correctly",{
  
  # This should be a four-partition dlist 
  a <- dlist(1,2,3,4)
  expect_equal(nparts(a),4L)
  
  # This should be a two-partition dlist with the same 
  # overall contents
  b <- dmapply(function(y) { 
                 start <- (y-1) * 2 + 1
                 end <- start + 1  
                 list(start,end)
               }
               , as.list(1:2))
  expect_equal(nparts(b),2L)
  expect_equal(collect(a),collect(b))

  # repartition a to be like b
  c <- repartition(a,b)

  expect_equal(collect(c),collect(b))
  expect_equal(collect(c,1),collect(b,1))
  expect_equal(collect(c,2),collect(b,2))
  expect_equal(dim(c),dim(b))
  expect_equal(nparts(c),nparts(b))

})
