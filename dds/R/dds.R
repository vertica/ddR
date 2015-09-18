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

# Current driving backend package. Package global variable
dds.env <- new.env(emptyenv())
# Fix for weird methods bug
dds.env$gc_performed <- FALSE

#' Sets the active backend driver in the DDS environment. The driver object is
#' responsible for the dispatch of all backend-specific DDS operations, such as
#' dmapply, collect, and parts. Besides allowing the backend driver to be changed,
#' initialization parameters may be passed into the ellipsis (...) part of the 
#' function arguments. 
#'
#' The default loaded driver in DDS is parallel.
#'
#' @param driver The driver object to load as the active backend for DDS. This object should extend class 'DDSDriver', and the S4 methods for do_dmapply, do_collect, and get_parts should be defined for the class of the driver object. 
#' @param ... Additional parameters to pass to the initialization function of this driver.
#' After successfully setting the backend to a new backend with useBackend(), all subsequent dmapply, collect, and parts operations will dispatch on that driver object's class. Note that DObjects created with a different backend prior to switching will be incompatible with these backend-specific functions of the new driver.
#' @examples
#' \dontrun{
#' useBackend(parallel,inst=2)
#' library(distributedR.dds); useBackend(distributedR)
#' } 
#' @export
useBackend <- function(driver, ...) {

  if(!is.null(dds.env$driver)) shutdown(dds.env$driver)

  if(!extends(class(driver)[[1]],"DDSDriver")) stop("Invalid driver object specified")

  if(!extends(driver@DListClass,"DObject")) stop("The driver DList class does not extend DDS::Dobject")
  if(!extends(driver@DFrameClass,"DObject")) stop("The driver DFrame class does not extend DDS::Dobject")
  if(!extends(driver@DArrayClass,"DObject")) stop("The driver DArray class does not extend DDS::Dobject")

  init(driver, ...)
      
  # Fix for weird methods bug
  dds.env$gc_performed <- FALSE
  dds.env$driver <- driver
}

#' @export
setClass("DDSDriver", representation(DListClass = "character", DFrameClass = "character", DArrayClass = "character", backendName = "character"))

#' @export
setGeneric("init", function(x,...) {
  standardGeneric("init")
}) 

#' @export
setGeneric("shutdown", function(x) {
  standardGeneric("shutdown")
}) 

#' @export
setMethod("init","DDSDriver",
  function(x,...) {
    message(paste0("Activating the ",x@backendName," backend."))
  }
)

#' @export
setMethod("shutdown","DDSDriver",
  function(x) {
    message(paste0("Deactivating the ",x@backendName," backend."))
  }
)

#' @export
# dispatches on DDSDriver
setGeneric("do_dmapply", function(driver,func,...,MoreArgs=list(),output.type="DListClass",nparts=NULL,combine="flatten",.unlistEach=FALSE) {
  # Fix for weird methods bug
  ## See: http://r.789695.n4.nabble.com/Reference-class-finalize-fails-with-attempt-to-apply-non-function-td4174697.html
  if(!dds.env$gc_performed){
     gc()
     dds.env$gc_performed <- TRUE
  }
  standardGeneric("do_dmapply")
})

#' @export
# dispatches on DDSDriver
setGeneric("combine", function(driver,items) {
  standardGeneric("combine")
})

#' @export
# dispatches on backend
setGeneric("get_parts", function(x,index,...) {
  standardGeneric("get_parts")
})

#' Like dmapply, but permits only one iterable argument, and the output.type is 
#' always a dlist. This is the distributed version of 'lapply'. 
#'
#' Note that as in lapply vs mapply, the order of arguments (iterable argument and function) supplied to the functions is reversed.
#'
#' @param X The vector, matrix, list, data.frame, dlist, darray, or dframe or other iterable object to supply to the function in FUN.
#' @param FUN the function to be applied to each element of ‘X’: see ‘Details’.  In the case of functions like ‘+’, ‘%*%’, the function name must be backquoted or quoted.
#' @param ... optional arguments to 'FUN'.
#' @return a DList with nparts=nparts
#' @examples
#' \dontrun{
#' a <- dlapply(1:5,function(x) x, nparts=3) # A DList with 3 partitions, which in the aggregate contains the elements 1 through 5.
#' b <- dlapply(a,function(x) x+3) # Adds 3 to each element of dlist a.
#' } 
#' @export
dlapply <- function(X,FUN,...,nparts=NULL,.unlistEach=FALSE) {
   dmapply(FUN,X,MoreArgs=list(...),output.type="DListClass",nparts=nparts,.unlistEach=.unlistEach)
}

#' dmapply is the main 'workhorse' function of DDS. Like mapply in R, it allows a multivariate function, FUN, to be applied to several inputs. Unlike standard mapply, it always returns a DDS Distributed Object, or DObject.
#'
#' Though dmapply is modeled after mapply, there are several important differences, as are evident in the parameters described below.
#'
#' @param FUN function to apply, found via ‘match.fun’.
#' @param ...  arguments to vectorize over (vectors or lists of strictly positive length, or all of zero length). These may also be distributed objects, such as dlists, darrays, and dframes.
#' @param a list of other arguments to ‘FUN’.
#' @param output.type The output DObject type. By default, this is "dlist", which means that the result of dmapply will be stored in a dlist. "darray" will make dmapply return a darray, just as "dframe" will make it return a dframe. 
#' @param nparts A 1d or 2d numeric value to specify how the output should be partitioned. DLists only have one-dimensional partitioning, whereas DArrays and DFrames have two (representing the number partitions across the vertical and horizontal dimensions). Let 'P' be the number of partitions as determined by getBestOutputPartitioning. The default for DLists is equal to 'P'. The default for DArrays and DFrames is c(1L,P).
#' @param combine For DFrames and DArrays, this specifies what how the results of dmapply are combined within each partition (if each partition contains more than one result). If "row", the results are rbinded; if "col", they are cbinded. If the value is "flatten", the results are flattened into one column, as is the case with simplify2array(). The default value is "flatten".
#' @return A DList, DArray, or DFrame (depending on the value of output.type), with nparts=nparts
#' @examples
#' \dontrun{
#' a <- dmapply(function(x,y) x+y, 1:5, 2:6, nparts=3) # A DList adding two vectors of numbers.
#' b <- dmapply(function(x) matrix(x,2,2), 1:4,output.type="darray",combine="row",nparts=c(2,2)) # A DArray with each partition containing the values equal to its partition id (1 to 4). Since combine is set to "row", the contents of each partition will be be vectorized as would be the case if combine=="flatten". Since nparts=c(2,2), the the partitions of the DArray will be stitched in a 2x2 fashion, meaning the overall dims of the DArray will be 4x4.
#' }
#' @export
dmapply <- function(FUN,...,MoreArgs=list(),output.type="dlist",nparts=NULL,combine="flatten",.unlistEach=FALSE) {
  stopifnot(is.function(FUN))
  
  # Allow for multiple ways of expressing output.type
  da_types <- c("da","darray","darrayclass","daclass","a","matrix","dmatrix","array","darr")
  dl_types <- c("dl","l","dlist","dlistclass","list","dlclass")
  df_types <- c("df","f","dframe","dframeclass","data.frame","dataframe")

  if(tolower(output.type) %in% da_types) output.type <- "DArrayClass"
  else if (tolower(output.type) %in% dl_types) output.type <- "DListClass"
  else if (tolower(output.type) %in% df_types) output.type <- "DFrameClass"

  if(output.type != "DListClass" && output.type != "DArrayClass" && output.type != "DFrameClass")
    stop("Unrecognized value for output.type -- try one of: {'dlist', 'darray', 'dframe'}.")   

  if(!is.null(nparts))
    if(!is.numeric(nparts) || length(nparts) < 1 || length(nparts) > 2) 
      stop("Invalid nparts vector provided. Must be a 1d or 2d vector")

  # Allow for multiple ways of expressing combine
  flatten_types <- c("flatten","flat","default","f")
  row_types <- c("row","r","rbind")
  col_types <- c("col","column","c","cbind")

  if(tolower(combine) %in% flatten_types) combine <- "flatten"
  else if (tolower(combine) %in% row_types) combine <- "row"
  else if (tolower(combine) %in% col_types) combine <- "col"

  if(combine != "flatten" && combine != "row" && combine != "col")
    stop("Unrecognized option for combine -- try one of: {'flatten', 'row', 'col'}")
  
  dargs <- list(...)
  stopifnot(length(dargs) > 0)

  # Ensure that ... arguments are of equal length. length() works correctly for data.frame,
  # arrays, and lists
  lens <- vapply(dargs,function(x) {
     if(is(x,"DObject") && x@backend != dds.env$driver@backendName)
       stop(paste0("An argument passed in was created with 
            backend '",x@backend,"'; the currently loaded backend is '",
            dds.env$driver@backendName,"'."))
        
     length(x)
   },FUN.VALUE=numeric(1))

  stopifnot(max(lens) == min(lens))
    
  partitioning <- getBestOutputPartitioning(dds.env$driver,...,nparts=nparts,type=output.type)

  # simplify2array does not work well on data.frames, default to column instead
  if(output.type == "DFrameClass" && combine == "flatten") combine = "col"

  newobj <- do_dmapply(dds.env$driver, func=match.fun(FUN), ..., MoreArgs=MoreArgs,
                       output.type=output.type,nparts=partitioning,combine=combine,
                       .unlistEach=.unlistEach)

  checkReturnObject(partitioning,newobj)

  newobj@backend <- dds.env$driver@backendName
  newobj@type <- output.type 

  newobj
}

# Check object returned by backend
checkReturnObject <- function(partitioning,result) {
  stopifnot(partitioning == result@nparts)
}

# Given a list of arguments into dmapply, return a dobject
# whose partitioning scheme we want to enforce the output to have
# this should be used by do_dmapply as well
#' @export
getBestOutputPartitioning <- function(driver,...,nparts=NULL,type=NULL) {
  UseMethod("getBestOutputPartitioning")
}

# This has been recently changed to returning the "best" nparts configuration

# Currently, we naively choose the first DObject argument we find,
# or just use the length of the input arguments if none are found
# (i.e., when parts() is used for all args)
#' @export
getBestOutputPartitioning.DDSDriver <- function(driver, ...,nparts=NULL,type=NULL) {

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
    if(type == "DListClass")
      nparts <- c(nparts, 1L)
    else
      nparts <- c(1L,nparts)
  }

  nparts 
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nWelcome to DDS! You may want to read the user guide vignette under vignettes/ if this is your first time.")
}
