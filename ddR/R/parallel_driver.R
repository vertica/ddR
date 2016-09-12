###################################################################
# Copyright 2015 Hewlett-Packard Development Company, L.P.
# This program is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public
# License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330,
# Boston, MA 02111-1307 USA.
###################################################################

#' @include ddR.R
NULL

setOldClass("SOCKcluster")
setOldClass("cluster")
setOldClass("spawnedMPIcluster")
setClassUnion("parallelCluster", c("SOCKcluster", "cluster",
                    "spawnedMPIcluster"))


#' Class for parallel driver
#'
#' @slot type character "FORK", "PSOCK", "MPI"
#' @slot cluster As returned from \link[parallel]{makeCluster}
setClass("parallel.ddR", contains = "ddRDriver",
        slots = c(type = "character", cluster = "parallelCluster"))


#' Initialize the no. of cores in parallel backend
#'
#' The FORK method of parallel works only on UNIX environments. The "PSOCK"
#' method requires SNOW but works on all OSes.
#'
#' This function is a wrapper around \link[parallel]{makeCluster}
#'
#' @param spec Number of cores to run with, or character vector of
#'      hostnames for SNOW cluster. If NULL defaults to the number of
#'      physical cores on machine.
#' @param type If "FORK", will use UNIX fork(). If "PSOCK", will use SNOW.
#' @param ... Additional arguments to \link[parallel]{makeCluster}
#' @return Object of class \code{\linkS4class{parallel.ddR}} representing a running parallel
#'      cluster
#'
#' @examples
#' \dontrun{
#' # Cluster on 2 cores with default type:
#' useBackend("parallel", 2)
#'
#' # PSOCK cluster with 3 slaves on 2 machines (assuming you can ssh user@server1):
#' servers <- c("server1", "server1", "server2")
#' useBackend("parallel", servers, type = "PSOCK")
#'
#' # MPI cluster (assuming Rmpi is installed)
#' useBackend("parallel", type = "MPI")
#' }
init_parallel <- function(spec = NULL, type = "PSOCK", ...){

    # executors is a positive integer
    if(is.null(spec)){
        executors <- parallel::detectCores(logical=FALSE)
        spec <- executors
    } 
    if(is.numeric(spec)){
        executors <- spec
    }
    if(is.character(spec)){
        # Character vector of hostnames for PSOCK cluster
        executors <- length(spec)
    }
    if(is.null(executors) || is.na(executors) || executors < 1 ||
            length(executors) > 1){
        message("Executors should be a single positive integer. Defaulting to 1.")
        executors <- 1L
    }
    executors <- as.integer(executors)

    # Handle cluster types
    if(!(type %in% c("PSOCK", "FORK", "SOCK", "MPI"))){
        # Safer to stop, but this facilitates experimentation
        warning("ddR hasn't been tested with this cluster type. Proceed at your own risk.")
    }
    if(.Platform$OS.type == "windows" && type == "FORK"){
        warning("type = 'FORK' is unsupported on Windows. Defaulting to type = 'PSOCK'")
        type <- "PSOCK"
    }

    cluster <- parallel::makeCluster(spec, type, ...)

    new("parallel.ddR",
        DListClass = "ParallelObj",
        DFrameClass = "ParallelObj",
        DArrayClass = "ParallelObj",
        name = "parallel",
        executors = executors,
        type = type,
        cluster = cluster
        )
}


setMethod("shutdown","parallel.ddR",
function(x) {
    parallel::stopCluster(x@cluster)
})


setMethod("do_dmapply", signature = c(driver = "parallel.ddR"),
function(driver, func, MoreArgs, dots)
{
    allargs <- c(list(cl = driver@cluster,
                      fun = func,
                      MoreArgs = MoreArgs,
                      RECYCLE = FALSE, SIMPLIFY = FALSE),
                 dots)
    answer <- do.call(parallel::clusterMap, allargs)
    answer
})
