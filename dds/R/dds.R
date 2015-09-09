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

# Set Driver 
#' @export
useBackend <- function(driver, ...) {

  if(!is.null(dds.env$driver)) shutdown(dds.env$driver)

  if(!extends(class(driver)[[1]],"DDSDriver")) stop("Invalid driver object specified")

  if(!extends(driver@DListClass,"DObject")) stop("The driver DList class does not extend DDS::Dobject")
  if(!extends(driver@DFrameClass,"DObject")) stop("The driver DFrame class does not extend DDS::Dobject")
  if(!extends(driver@DArrayClass,"DObject")) stop("The driver DArray class does not extend DDS::Dobject")

  init(driver, ...)
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

#' @export
dlapply <- function(dobj,FUN,...,nparts=NULL,combine="flatten",.unlistEach=FALSE) {
   dmapply(FUN,dobj,MoreArgs=list(...),nparts=nparts,combine=combine,.unlistEach=.unlistEach)
}

#' @export
dmapply <- function(FUN,...,MoreArgs=list(),output.type="DListClass",nparts=NULL,combine="flatten",.unlistEach=FALSE) {
  stopifnot(is.function(FUN))

  if(output.type != "DListClass" && output.type != "DArrayClass" && output.type != "DFrameClass")
    stop("Unrecognized output type -- must be one of: {'DListClass', 'DArrayClass', 'DFrameClass'}.")   

  if(!is.null(nparts))
    if(!is.numeric(nparts) || length(nparts) < 1 || length(nparts) > 2) 
      stop("Invalid nparts vector provided. Must be a 1d or 2d vector")

  if(combine != "flatten" && combine != "row" && combine != "col")
    stop("Unrecognized option for combine -- must be one of: {'flatten', 'row', 'col'}")
  
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
