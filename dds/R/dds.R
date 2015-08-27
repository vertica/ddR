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

  # if the selected driver is already loaded, do nothing
  if(identical(dds.env$driver,driver)) {
    warning("Selected driver is the same as the one already loaded. Doing nothing.")
    return()
  }

  if(!is.null(dds.env$driver)) shutdown(dds.env$driver)

  if(!extends(class(driver)[[1]],"DDSDriver")) stop("Invalid driver object specified")

  if(!extends(driver@DListClass,"DObject")) stop("The driver DList class does not extend DDS::Dobject")
  if(!extends(driver@DFrameClass,"DObject")) stop("The driver DFrame class does not extend DDS::Dobject")
  if(!extends(driver@DArrayClass,"DObject")) stop("The driver DArray class does not extend DDS::Dobject")

  dds.env$driver <- driver
  init(driver, ...)
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
setGeneric("do_dmapply", function(driver,func,...,MoreArgs=list(),FUN.VALUE=NULL) {
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
dlapply <- function(dobj,FUN,...) {
   dmapply(FUN,dobj,MoreArgs=list(...))
}

#' @export
dmapply <- function(FUN,...,MoreArgs=list(),FUN.VALUE=NULL) {
  stopifnot(is.function(FUN))
  stopifnot(length(args) > 0)

  dargs <- list(...)

  # Ensure that ... arguments are of equal length
  lens <- lapply(dargs,function(x){
     length(x)
  })

  stopifnot(max(unlist(lens)) == min(unlist(lens)))
    
  #TODO: Use FUN.VALUE to drive proper selection of output type
  if(is.null(FUN.VALUE) || is.list(FUN.VALUE) && !is.data.frame(FUN.VALUE)){
    type = "DListClass"
  } else if(is.data.frame(FUN.VALUE)){
    type = "DFrameClass"
  } else if(is.matrix(FUN.VALUE)) {
    type = "DArrayClass"
  } else {
    stop("unrecognized return type for FUN.VALUE")
  }
 
  partitioning <- getBestOutputPartitioning(dds.env$driver,...)

  margs <- list(...)

  newobj <- do_dmapply(dds.env$driver, func=FUN, ..., MoreArgs=MoreArgs,
                       FUN.VALUE=FUN.VALUE)

  newobj@backend <- dds.env$driver@backendName
  newobj@type <- type
  newobj@nparts <- nparts(partitioning)

  # TODO: this check doesn't work
  stopifnot(is(newobj,slot(dds.env$driver,type)))

  newobj
}

# Given a list of arguments into dmapply, return a dobject
# whose partitioning scheme we want to enforce the output to have
# this should be used by do_dmapply as well
#' @export
getBestOutputPartitioning <- function(driver,...) {
  UseMethod("getBestOutputPartitioning")
}

# Currently, we naively choose the first DObject argument we find,
# or just use the length of the input arguments if none are found
# (i.e., when parts() is used for all args)
#' @export
getBestOutputPartitioning.DDSDriver <- function(driver, ...) {
  margs <- list(...)

  for(i in 1:length(margs)) {
    if(is(margs[[i]],"DObject")) { 
      return(margs[[i]])
    }
  }

  new("DObject",nparts=length(margs[[1]]))

}
