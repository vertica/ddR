context("Tests which should run with any driver")

# Replace this with any driver
driver <- parallel


test_that("init method", {

    initvalue <- ddR:::init(driver, executors = 2L)
    expect_identical(initvalue, 2L)
    expect_identical(ddR.env$executors, 2L)

})


test_that("Driver extends required classes", {

    expect_true(is(driver, "ddRDriver"))

    expect_true(extends(driver@DListClass, "DObject"))
    expect_true(extends(driver@DFrameClass, "DObject"))
    expect_true(extends(driver@DArrayClass, "DObject"))

})
