context("Tests which should run with any driver")

# Replace this with any driver
driver <- parallel.ddR

useBackend(driver)

test_that("init and shutdown methods", {

    # Explicit shutdown
    shutdown(driver)

    # Default shutdown argument 
    useBackend(driver)
    shutdown()
 
    # Return a positive integer
    initvalue <- ddR:::init_driver(driver)
    expect_is(initvalue, "integer")
    expect_gt(initvalue, 0)

})


test_that("Driver extends required classes", {

    expect_true(is(driver, "ddRDriver"))

    expect_true(extends(driver@DListClass, "DObject"))
    expect_true(extends(driver@DFrameClass, "DObject"))
    expect_true(extends(driver@DArrayClass, "DObject"))

})
