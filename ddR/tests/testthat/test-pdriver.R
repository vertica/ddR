context("Tests specific to the parallel driver")

l <- list(1:10, letters, rnorm(5))


test_that("Parallel is indeed the default driver", {

    useBackend()
    expect_s4_class(ddR.env$driver, "parallel.ddR")

})


test_that("useBackend recovers from a bad call", {

    expect_error(useBackend(parallel.ddR, type="LASERWOLF_FAN_CLUB"))
    useBackend(parallel.ddR)

})


test_that("useBackend can switch and use PSOCK", {

    useBackend(parallel.ddR, type="PSOCK")

    dl1 <- dlist(1:4)
    dl2 <- dlist(5)

    out <- collect(dmapply(c, dl1, dl2))

    expect_equal(out, list(1:5))

    # Necessary so that remaining tests run using correct cluster type
    useBackend(parallel.ddR)

})


test_that("mapply with different parallel backends", {

    useBackend(parallel.ddR, executors = 2)
    dl2 <- do.call(dlist, l)

    # This sets up a different local cluster
    useBackend(parallel.ddR, type = "PSOCK", executors = 3)
    dl3 <- do.call(dlist, l)

    # TODO Clark: Currently this works, but maybe better to throw an
    # error if mixing clusters like this.
    out <- collect(dmapply(c, dl2, dl3))
    expect_equal(mapply(c, l, l), out)

    # Necessary so that remaining tests run using correct cluster type
    useBackend(parallel.ddR)

})
