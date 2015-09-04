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
setGeneric("do_dmapply", function(driver,func,...,MoreArgs=list(),FUN.VALUE=NULL,.model=NULL) {
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
dlapply <- function(dobj,FUN,...,.model=NULL) {
   dmapply(FUN,dobj,MoreArgs=list(...),.model=.model)
}

#' @export
dmapply <- function(FUN,...,MoreArgs=list(),FUN.VALUE=NULL,.model=NULL) {
  stopifnot(is.function(FUN))
  stopifnot(length(args) > 0)

  if(!is.null(.model) && !is(.model,"DObject")) stop(".model object must be of type DObject")

  dargs <- list(...)

  # Ensure that ... arguments are of equal length
  lens <- vapply(dargs,function(x){
     if(is(x,"DObject") && x@backend != dds.env$driver@backendName)
       stop(paste0("An argument passed in was created with 
            backend '",x@backend,"'; the currently loaded backend is '",
            dds.env$driver@backendName,"'."))

     if((is(x,"DObject") && x@type == "DFrameClass") || is.data.frame(x)){
       ncol(x)
     } else if ((is(x,"DObject") && x@type == "DArrayClass") || is.matrix(x)) {
       prod(dim(x))
     } else {
       length(x)
     }
  },FUN.VALUE=numeric(1))

  stopifnot(max(lens) == min(lens))
    
  if(is.null(FUN.VALUE) || is.list(FUN.VALUE) && !is.data.frame(FUN.VALUE)){
    type = "DListClass"
  } else if(is.data.frame(FUN.VALUE)){
    type = "DFrameClass"
  } else if(is.matrix(FUN.VALUE)) {
    type = "DArrayClass"
  } else {
    stop("unrecognized type for FUN.VALUE")
  }
  
  modelObj <- getBestOutputPartitioning(dds.env$driver,...,.model=.model,type)

  newobj <- do_dmapply(dds.env$driver, func=match.fun(FUN), ..., MoreArgs=MoreArgs,
                       FUN.VALUE=FUN.VALUE,.model=modelObj)

  checkReturnObject(modelObj,newobj)

  newobj@backend <- dds.env$driver@backendName
  newobj@type <- type

  newobj
}

# Check object returned by backend
checkReturnObject <- function(model,result) {
  stopifnot(model@nparts == result@nparts)
}

# Given a list of arguments into dmapply, return a dobject
# whose partitioning scheme we want to enforce the output to have
# this should be used by do_dmapply as well
#' @export
getBestOutputPartitioning <- function(driver,...,.model=NULL,type=NULL) {
  UseMethod("getBestOutputPartitioning")
}

# Currently, we naively choose the first DObject argument we find,
# or just use the length of the input arguments if none are found
# (i.e., when parts() is used for all args)
#' @export
getBestOutputPartitioning.DDSDriver <- function(driver, ...,.model=NULL,type=NULL) {

  # TODO: If not NULL, check if .model is valid and acceptable...otherwise throw an error
  # or print a warning and return a model object using default logic instead
  # Insert checks here
  if(!is.null(.model)) return(.model) # If all tests pass, return original .model 

  # Otherwise, revert to default behavior below
  margs <- list(...)

  for(i in seq(length(margs))) {
    if(is(margs[[i]],"DObject")) { 
      return(margs[[i]])
    }
  }

  new("DObject",nparts=c(length(margs[[1]]), 1L))
}
