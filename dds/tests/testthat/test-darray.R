context("DArray metadata and dmapply")

test_that("DArray dimensions and collect are correct",{

    a <- darray(dim=c(4,4), psize = c(2,4))
    a <- dmapply(function(x,y) {
                 matrix(y, nrow=2,ncol=4)
               }, parts(a), as.list(1:nparts(a)), FUN.VALUE=matrix())
  expect_equal(collect(a,2)[[1]],matrix(2,nrow=2, ncol=4))
  expect_equal(prod(a@nparts),2)
})

