context("DFrame metadata and dmapply")

 a <- dmapply(function(x) {
             data.frame(matrix(x, nrow=2, ncol=4))
             }, 1:2, output.type="DFrameClass", combine="row",nparts=c(2,1))

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
               }, 1:2, output.type="DFrameClass", combine="row",nparts=c(2,1)) 

   # A DArray that has partition dimensions not consistent with 'a'
   c <- dmapply(function(x) {
                  data.frame(matrix(2*x+5, nrow=4, ncol=2))
               }, 1:2, output.type="DFrameClass", combine="row",nparts=c(2,1))

   # Partition-wise addition
   d <- dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(b), MoreArgs=list(z=3), output.type="DFrameClass",combine="row",nparts=c(2,1))

   expect_equal(collect(d), data.frame(do.call(cbind,rep(list(c(11,11,14,14)),4))))

   # Same partition-wise addition, this time with incompatible partitioning
   expect_error(dmapply(function(x,y,z) {
                  x + y + z
                }, parts(a),parts(c), MoreArgs=list(z=3), output.type="DFrameClass",combine="row",nparts=c(2,1)))

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
