context("Tests specific to the spark driver")

test_that("Spark is indeed the default driver", {

  driver_name = "spark"
  useBackend(driver_name)
})
