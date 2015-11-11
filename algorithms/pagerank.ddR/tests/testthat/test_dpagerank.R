
graph <- matrix(0, 6,6)
graph[2,1] <- 1L;graph[2,3] <- 1L;graph[3,1] <- 1L;graph[3,2] <- 1L;
graph[3,4] <- 1L;graph[4,5] <- 1L;graph[4,6] <- 1L;graph[5,4] <- 1L; 
graph[5,6] <- 1L;graph[6,4] <- 1L

wrong_dgraph <- as.darray(graph)
dgraph <- as.darray(graph, c(6,3))

graph[6,5] <- NA
dgraph_na <- as.darray(graph, c(6,3))

wrong_personalized1 <- rep(1,nrow(graph))
wrong_personalized2 <- darray(dim=c(2,6), psize=c(2,3), data=1/6)
wrong_personalized3 <- darray(dim=c(1,6), psize=c(1,2), data=1/6)
correct_personalized <- darray(dim=c(1,6), psize=c(1,3), data=1/6)

wrong_weights1 <- darray(dim=c(6,3),psize=c(2,3), data=1)
wrong_weights2 <- darray(dim=c(1,6), psize=c(1,3), data=1)
wrong_weights3 <- darray(dim=c(6,6), psize=c(3,6), data=1)
correct_weights <- darray(dim=c(6,6),psize=c(6,3), data=1)

########## General Tests for input validation ##########
context("Checking the input validation in dpagerank")

test_that("the inputes are validated", {
    expect_error(dpagerank(graph))
    expect_error(dpagerank(wrong_dgraph))
    expect_error(dpagerank(dgraph, niter = 0))
    expect_error(dpagerank(dgraph, eps = 0))
    expect_error(dpagerank(dgraph, damping = 2))
    expect_error(dpagerank(dgraph, personalized = wrong_personalized1))
    expect_error(dpagerank(dgraph, personalized = wrong_personalized2))
    expect_error(dpagerank(dgraph, personalized = wrong_personalized3))
    expect_error(dpagerank(dgraph, weights = wrong_weights1))
    expect_error(dpagerank(dgraph, weights = wrong_weights2))
    expect_error(dpagerank(dgraph, weights = wrong_weights3))
    expect_error(dpagerank(dgraph, na_action = "what?"))
    expect_error(dpagerank(dgraph_na, na_action = "pass"))
    expect_error(dpagerank(dgraph_na, na_action = "fail"))
})

########## Evaluate results ##########
context("Checking the results of dpagerank for a dense graph")

test_that("the returns result is correct despite available NA", {
    expect_warning(pg <- dpagerank(dgraph_na, na_action = "exclude"))

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(collect(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(dwhich.max(pg), 4)
})

test_that("the returns result is correct with available personalized darray", {
    pg <- dpagerank(dgraph, personalized = correct_personalized)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(collect(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(dwhich.max(pg), 4)
})

test_that("the returns result is correct with available weights darray", {
    pg <- dpagerank(dgraph, weights = correct_weights)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(collect(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(dwhich.max(pg), 4)
})

context("Checking the results of dpagerank for a sparse graph")

library(Matrix)
mygraph <- Matrix(0, 6,6)
mygraph[2,1] <- 1L;mygraph[2,3] <- 1L;mygraph[3,1] <- 1L;mygraph[3,2] <- 1L;
mygraph[3,4] <- 1L;mygraph[4,5] <- 1L;mygraph[4,6] <- 1L;mygraph[5,4] <- 1L; 
mygraph[5,6] <- 1L;mygraph[6,4] <- 1L

generateDgraph <- function(id, graph, npartitions) {
	nColumns <- ceiling(ncol(graph) / npartitions)
    start <- (id -1) * nColumns + 1
    end <- min(ncol(graph), start + nColumns -1)

    graph[, start:end, drop=FALSE]
}

Sdgraph <- dmapply(generateDgraph, id = 1:nInst,
                MoreArgs = list(graph = mygraph, npartitions = nInst),
		output.type = "sparse_darray", 
		combine = "cbind", nparts = c(1,nInst))

test_that("the returns result is correct", {

    pg <- dpagerank(Sdgraph)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(collect(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(dwhich.max(pg), 4)
})

test_that("the returns result is correct with weights", {
    expect_error(dpagerank(Sdgraph, weights = correct_weights))

    Sweights <- dmapply(function(x) x, parts(Sdgraph), 
		output.type = Sdgraph@type, 
		combine = "cbind", nparts = nparts(Sdgraph))

    pg <- dpagerank(Sdgraph, weights = Sweights)

    ## the result calculated by the old method of igraph 0.7.1, which uses exactly the same algorithm
    # library(igraph)
    # ig <- graph.adjacency(graph)
    # (rpg <- page.rank.old(ig))
    # 0.05704882 0.03939800 0.04393000 0.38021881 0.19588465 0.28351973
    expect_equivalent(collect(pg), matrix(c(0.05704882, 0.03939800, 0.04393000, 0.38021881, 0.19588465, 0.28351973), nrow=1))
    expect_equal(dwhich.max(pg), 4)
})


