context("Tests specific to the parallel driver")


test_that("Parallel is indeed the default driver", {

    useBackend()
    expect_s4_class(ddR.env$driver, "ParallelddR")

})


test_that("useBackend recovers from a bad call", {

    expect_error(useBackend(parallel, type="LASERWOLF_FAN_CLUB"))
    useBackend(parallel)

})


test_that("useBackend can switch and use PSOCK", {

    useBackend(parallel, type="PSOCK")

    dl1 <- dlist(1:4)
    dl2 <- dlist(5)

    out <- collect(dmapply(c, dl1, dl2))

    expect_equal(out, list(1:5))

    # Necessary so that remaining tests run using correct cluster type
    useBackend(parallel)

})
