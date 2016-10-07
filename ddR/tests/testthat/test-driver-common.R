context("Tests which should run with any driver")

# Variable driver_name must be assigned in the testing script
driver <- useBackend(driver_name)


test_that("init and shutdown methods", {

    # Explicit shutdown
    shutdown(driver)

    # Default shutdown argument 
    useBackend(driver_name)
    shutdown()
 
    useBackend(driver_name)
})


test_that("Driver extends required classes", {

    expect_true(is(driver, "ddRDriver"))

    expect_true(extends(driver@DListClass, "DObject"))
    expect_true(extends(driver@DFrameClass, "DObject"))
    expect_true(extends(driver@DArrayClass, "DObject"))

})
