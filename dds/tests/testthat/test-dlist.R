library(Matrix)
test_that("DList dimensions are correct",{
  a <- dlist(0,1,2,3,c(4,5),list(6,7,8),9)
  dims <- dim(a)

  expect_equal(length(dims),1)
  expect_equal(dims,7)
})

context("checking DList invalid operations")

test_that("Dlist invalid operations", {
  
  mdl <- list()
  dl1 <- dlist(nparts=5)
  dl1gp <- collect(dl1)
  expect_error(collect(dl1, 6))
  expect_true(class(dl1gp)=="list")
  expect_true(is.dlist(dl1))
  expect_error(collect(dl1, 0))
  expect_error(collect(dl1, 6))
  expect_error(collect(dl1, -100))
  expect_error(collect(dl1, mdl))
  expect_error(parts(dl1,0))
  expect_error(parts(dl1,6))
})

context("testing DList row/column name and value setup")

test_that("Dlist testing row/column name and value setup",{
  dl2 <- dlist(nparts=10)
  empty_list <- list()
  dl2gp <- collect(dl2)
  dl2gp5 <- collect(dl2, 5)
  dl2gp10 <- collect(dl2, 10)
  expect_true(all(dl2gp== empty_list))
  expect_true(all(dl2gp5== empty_list))
  expect_true(all(dl2gp10== empty_list))

  A <- list(2, c("fred", "mary",3), list(10, 20, 30, matrix(2,4,5)))
  A1 <- list(2)
  A2 <- list(c("fred", "mary", 3))
  A3 <- list(list(10, 20, 30, matrix(2,4,5)))

  dl3 <- dmapply(function(x) {
                  if(x==1) return(2)
                  else if(x==2) return(c("fred","mary",3))
                  else return(list(10,20,30, matrix(2,4,5)))
                }, 1:3)

  dl3gp <- collect(dl3)
  dl3gp1 <- collect(dl3, 1)
  dl3gp2 <- collect(dl3, 2)
  dl3gp3 <- collect(dl3, 3)
  expect_equal(A, dl3gp)
  expect_equal(A1, dl3gp1)
  expect_equal(A2, dl3gp2)
  expect_equal(A3, dl3gp3)

})
