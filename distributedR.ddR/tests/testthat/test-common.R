# file for testing common functionality between ddR and DR ddR

driver_name = "distributedR"

useBackend(driver_name, warn = FALSE)

context("testing common features")

# Test everything except those specific to the parallel driver
to_test <- list.files("../../../ddR/tests/testthat")
to_test <- to_test[to_test != "test-pdriver.R"]

lapply(to_test, test_file)
