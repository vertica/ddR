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

#' @useDynLib ddR
#' @importFrom Rcpp sourceCpp

#' @importFrom methods setClass setGeneric setMethod callNextMethod extends is new rbind2
#' @importFrom utils head tail 
NULL

# Current driving backend package. Package global variable
ddR.env <- new.env(emptyenv())
ddR.env$RminorVersion <- R.version$minor
#Track no. of executors in the backend
ddR.env$nexecutors <- 1

#' Sets the active backend driver. Functions exported by the 'ddR' package 
#' are dispatched to the backend driver.
#' Backend-specific initialization parameters may be passed into the ellipsis (...) part of the 
#' function arguments. 
#'
#' The default driver uses R's 'parallel' as the backend.
#'
#' @param driver driver object for the backend that will be used. This object should extend class 'ddRDriver', and the S4 methods for do_dmapply, do_collect, and get_parts should be defined in the class of the driver object. 
#' @param ... additional parameters to pass to the initialization function of the driver.
#' @details
#' After successfully registering a new backend with useBackend(), all subsequent dmapply, collect, and parts operations will dispatch on that driver object's class. Note that distributed objects created with a different backend prior to switching will be incompatible with these backend-specific functions of the new driver.
#' @references 
#' Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu, V., Hsu, M., Roy, I. 
#' Large scale predictive analytics in Vertica: Fast data transfer, distributed model creation 
#' and in-database prediction. _Sigmod 2015_, 1657-1668.
#'
#' Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and
#' Schreiber, R. (2013) Presto: Distributed Machine Learning and
#' Graph Processing with Sparse Matrices. _EuroSys 2013_, 197-210.
#'
#' Homepage: https://github.com/vertica/ddR
#' @examples
#' \dontrun{
#' useBackend(parallel,executors=2)
#' library(distributedR.ddR); useBackend(distributedR)
#' } 
#' @export
useBackend <- function(driver, ...) {

  if(!is.null(ddR.env$driver)) shutdown(ddR.env$driver)

  if(!extends(class(driver)[[1]],"ddRDriver")) stop("Invalid driver object specified")

  if(!extends(driver@DListClass,"DObject")) stop("The driver DList class does not extend ddR::Dobject")
  if(!extends(driver@DFrameClass,"DObject")) stop("The driver DFrame class does not extend ddR::Dobject")
  if(!extends(driver@DArrayClass,"DObject")) stop("The driver DArray class does not extend ddR::Dobject")

  nexecutors<-init(driver, ...)
  if(!is.null(nexecutors) && is.numeric(nexecutors) && (nexecutors > 1)) {ddR.env$nexecutors <- nexecutors}
      
  ddR.env$driver <- driver
}

#' The base S4 class for backend driver classes to extend.
#' 
#' @slot DListClass A character vector naming the class-name for dlists.
#' @slot DArrayClass A character vector naming the class-name for darrays.
#' @slot DFrameClass A character vector naming the class-name for dframes.
#' @slot backendName A character vector naming the backend.
#' @export
setClass("ddRDriver", representation(DListClass = "character", DFrameClass = "character", DArrayClass = "character", backendName = "character"))

#' Called when the backend driver is initialized.
#'
#' @param x The driver object to initialize the backend for.
#' @param ... Other parameters to pass to the initialization routine.
#' @export
setGeneric("init", function(x,...) standardGeneric("init"))

#' Called when the backend driver is shutdown.
#'
#' @param x The driver object to shutdown.
#' @export
setGeneric("shutdown", function(x) standardGeneric("shutdown")) 

#' @describeIn init Default backend initialization message.
#' @export
setMethod("init","ddRDriver",
  function(x,...) {
    message(paste0("Activating the ",x@backendName," backend."))
  }
)

#' @describeIn shutdown Default backend shutdown message.
#' @export
setMethod("shutdown","ddRDriver",
  function(x) {
    message(paste0("Deactivating the ",x@backendName," backend."))
  }
)

#' Backend-specific dmapply logic. This is a required override for all
#' backends to implement so dmapply works.
#' @param driver The driver that the logic dispatches on.
#' @param func The function to execute
#' @param ... Iterable arguments from dmapply.
#' @param MoreArgs A list of more arguments to the funciton.
#' @param output.type The type of output (can be 'dlist', 'darray',
#' 'sparse_darray', or 'dframe').
#' @param nparts A 2d-vector indicating how the output is partitioned.
#' @param combine One of 'default', 'rbind', 'cbind', or 'c', which 
#' specifies how the results from each partition should be combined.
#' @return An object specific to the backend, with the nparts and psize
#' fields filled.
#' @export
# dispatches on ddRDriver
setGeneric("do_dmapply",
           function(driver, func, ..., MoreArgs=list(), output.type="dlist",
                    nparts=NULL, combine="default")
               standardGeneric("do_dmapply"),
           signature=c("driver", "func"))


#' Combines a list of partitions into a single distributed 
#' object. (can be implemented by a frontend wrapper without actually 
#' combining data in storage).
#' @param driver The driver on which combine is dispatched.
#' @param items A list of partitions to combine.
#' @return A new distributed object made form the items list.
#' @export
# dispatches on ddRDriver
setGeneric("combine", function(driver, items) standardGeneric("combine"))

#' Gets the partitions to a distributed object, given an index.
#' @param x The distributed object to dispatch on.
#' @param index The index or indices of the partitions to fetch.
#' @param ... Other options (not in use currently)
#' @return A list containing the partitions of 'x'.
#' @export
# dispatches on backend
setGeneric("get_parts", function(x, index, ...) standardGeneric("get_parts"))

#' Backend implemented function to move data from storage to the calling
#' context (node). 
#' @param x The distributed object to fetch data from.
#' @param parts The parts (indices) of the distributed object to fetch.
#' @return The data returned as a list, matrix, or data.frame.
#' @export
setGeneric("do_collect", function(x, parts) standardGeneric("do_collect"))

#' Distributed version of 'lapply'. Similar to \code{\link{dmapply}}, but permits only one iterable argument, and output.type is 
#' always 'dlist'. 
#'
#' @param X vector, matrix, list, data.frame, dlist, darray, or dframe or other iterable object to supply to the function in FUN.
#' @param FUN the function to be applied to each element of 'X'.
#' @param ... optional arguments to 'FUN'.
#' @param nparts number of partitions in the output dlist.
#' @return a dlist with number of partitions specified in 'nparts'
#' @references 
#' Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu, V., Hsu, M., Roy, I. 
#' Large scale predictive analytics in Vertica: Fast data transfer, distributed model creation 
#' and in-database prediction. _Sigmod 2015_, 1657-1668.
#'
#' Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and
#' Schreiber, R. (2013) Presto: Distributed Machine Learning and
#' Graph Processing with Sparse Matrices. _EuroSys 2013_, 197-210.
#'
#' Homepage: https://github.com/vertica/ddR
#' @examples
#' \dontrun{
#' a <- dlapply(1:5,function(x) x, nparts=3) 
#' # A DList with 3 partitions, 
#' # which in the aggregate contains the elements 1 through 5.
#' b <- dlapply(a,function(x) x+3) # AddR 3 to each element of dlist a.
#' } 
#' @export
dlapply <- function(X,FUN,...,nparts=NULL) {
   dmapply(FUN,X,MoreArgs=list(...),output.type="dlist",nparts=nparts)
}

#' Distributed version of mapply. Similar to R's 'mapply', it allows a multivariate function, FUN, to be applied to several inputs. Unlike standard mapply, it always returns a distributed object.
#'
#' Though dmapply is modeled after mapply, there are several important differences, as evident in the parameters described below.
#'
#' @param FUN function to apply, found via 'match.fun'.
#' @param ...  arguments to vectorize over (vectors or lists of strictly positive length, or all of zero length). These may also be distributed objects, such as dlists, darrays, and dframes.
#' @param MoreArgs a list of other arguments to 'FUN'.
#' @param output.type the output type of the distributed object. The default value of "dlist" means that the result of dmapply will be stored in a distributed list. "darray" will make dmapply return a darray, just as "dframe" will make it return a dframe. "sparse_darray" results in a special version of darray where the elements are sparse.
#' @param nparts a 1d or 2d numeric value to specify how the output should be partitioned. dlists only have one-dimensional partitioning, whereas darrays and dframes have two (representing the number partitions across the vertical and horizontal dimensions). 
#' @param combine for dframes and darrays, it specifies how the results of dmapply are combined within each partition (if each partition contains more than one result). If "rbind", the results are stitched using rbind; if "cbind", cbind is used. If the value is "c", the results are flattened into one column, as is the case with simplify2array(). For dlists, "c" will first attempt to unlist each element of the dmapply result and then expand these items within the partition of the dlist. One may think of this as the function that is invoked on the resulting list after the dmapply, with 'do.call'. The default value is "default", which for darrays and dframes has identical behavior to "c". For dlists, no function is called if "default".
#' @return A dlist, darray, or dframe (depending on the value of output.type), with number of partitions equal to \code{\link{nparts}}
#' @references 
#' Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu, V., Hsu, M., Roy, I. 
#' Large scale predictive analytics in Vertica: Fast data transfer, distributed model creation 
#' and in-database prediction. _Sigmod 2015_, 1657-1668.
#'
#' Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and
#' Schreiber, R. (2013) Presto: Distributed Machine Learning and
#' Graph Processing with Sparse Matrices. _EuroSys 2013_, 197-210.
#'
#' Homepage: https://github.com/vertica/ddR
#' @examples
#' \dontrun{
#' ## A dlist created by adding two input vectors
#' a <- dmapply(function(x,y) x+y, 1:5, 2:6, nparts=3) 
#' collect(a)
#'
#' ##Create a darray with 4 partitions. Partitions are stitched in 2x2 fashion, 
#' # meaning the overall dims of the darray will be 4x4.
#' b <- dmapply(function(x) matrix(x,2,2), 1:4,output.type="darray",combine="rbind",nparts=c(2,2)) 
#' collect(b,1) #First partition
#' collect(b)
#' }
#' @export
dmapply <- function(FUN ,..., MoreArgs=list(),
                    output.type=c("dlist", "dframe", "darray", "sparse_darray"),
                    nparts=NULL, combine=c("default","c","rbind", "cbind"))
{
  if(!is.function(FUN)) stop("FUN needs to be a function")

  output.type <- match.arg(output.type)
  
  if(!is.null(nparts))
    if(!is.numeric(nparts) || length(nparts) < 1 || length(nparts) > 2) 
      stop("Invalid nparts vector provided. Must be one or two numbers.")

  combine <- match.arg(combine)

  if(output.type == "dlist" && (combine == "rbind" || combine == "cbind"))
    stop("'combine' options 'rbind' and 'cbind' are invalid for dlist outputs.")
  
  dargs <- list(...)

  if(length(dargs) == 0) stop("Need to supply at least one iterable item for the function.")

  # Ensure that ... arguments are of equal length. length() works correctly for data.frame,
  # arrays, and lists
  lens <- vapply(dargs,function(x) {
     if(is(x,"DObject") && x@backend != ddR.env$driver@backendName)
       stop(paste0("An argument passed in was created with 
            backend '",x@backend,"'; the currently loaded backend is '",
            ddR.env$driver@backendName,"'."))
        
     length(x)
   },FUN.VALUE=numeric(1))

  if(max(lens) != min(lens)) stop("The lengths of your iterable arguments need to be equal.")
  if(min(lens) == 0) stop("Zero-length arguments are not permitted.")
    
  partitioning <- getBestOutputPartitioning(ddR.env$driver,...,nparts=nparts,type=output.type)

  # simplify2array does not work well on data.frames, default to column instead
  if(output.type == "dframe" && combine == "c") combine = "default"
  if(output.type == "sparse_darray" && combine != "cbind" && combine != "rbind") 
    stop("sparse_darray outputs must have either 'rbind' or 'cbind' for combine")

  newobj <- do_dmapply(ddR.env$driver, func=match.fun(FUN), ..., MoreArgs=MoreArgs,
                       output.type=output.type,nparts=partitioning,combine=combine)                       

  checkReturnObject(partitioning,newobj)

  newobj@backend <- ddR.env$driver@backendName
  newobj@type <- output.type 

  newobj
}

# Check object returned by backend
checkReturnObject <- function(partitioning,result) {
  if(!identical(partitioning,nparts(result))) 
    stop("The backend returned a result with partitioning that does not match what is expected.")
}

# Given a list of arguments into dmapply, return a distributed object
# whose partitioning scheme we want to enforce the output to have
# this should be used by do_dmapply as well
#' This is an overrideable function that determines what the output
#' partitioning scheme of a dlapply or dmapply function should be.
#' It determines the 'ideal' nparts for the output if it is not supplied.
#' For API standard-enforcement, overriding this is not recommended.
#' @param driver The backend driver to dispatch on.
#' @param ... The arguments to this dmapply operation.
#' @param nparts The nparts argument, if any, supplied by the user.
#' @param type The output.type supplied by the user.
#' @return A 2d-vector, that will be passed into your backend's do_dmapply.
#' @export
getBestOutputPartitioning <- function(driver,...,nparts=NULL,type=NULL) {
  UseMethod("getBestOutputPartitioning")
}

# This has been recently changed to returning the "best" nparts configuration

# Currently, we naively choose the first DObject argument we find,
# or just use the length of the input arguments if none are found
# (i.e., when parts() is used for all args)
#' @describeIn getBestOutputPartitioning The default implementation for 
#' getBestOutputPartitioning.
#' @export
getBestOutputPartitioning.ddRDriver <- function(driver, ...,nparts=NULL,type=NULL) {

  margs <- list(...)
  # Change this value when we want to change default
  # TODO(etduwx): determine proper number. 
  # Should be either 1 or length or arguments?
  defaultNoOfPartitions <- length(margs[[1]])

  # if nparts is NULL, we run our heuristic algorithm here
  if(is.null(nparts)) {
    # first we prioritize matching nparts of one (the first) dobject, if any are dobjects
    for(i in seq(length(margs))) {
      if(is(margs[[i]],"DObject")) { 
        nparts  <- totalParts(margs[[i]])
        break
      }
    }
  }   

  # or if none are found, then we resort to making nparts == length of first argument
  
  # default to cbinding partitions if 2D
  # # TODO(etduwx): Assert that user-provided nparts (if any) is valid and acceptable.
  # If not, throw an error. Alternatively, resort to defaultValue as below:
  # if(error(nparts) || is.null(nparts)) nparts <- defaultNoOfPartitions
  if(is.null(nparts)) nparts <- defaultNoOfPartitions 

  if(length(nparts) == 1) {
    # default to cbinding partitions if 2D
    if(type == "dlist")
      nparts <- c(nparts, 1L)
    else
      nparts <- c(1L,nparts)
  }

  nparts 
}
