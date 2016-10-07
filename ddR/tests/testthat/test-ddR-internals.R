context("Tests for internal functionality in ddR.")


test_that("Bad driver errors", {

    bad_driver <- "just a string"
    expect_error(useBackend(bad_driver), regexp = "parallel")

})
