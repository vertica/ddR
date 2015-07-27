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

# Defines the base class for a distributed object, as well as their methods
# DList, DArray, and DFrame inherit from class DObject

#' @export
collect <- function(dobj, index=NULL) { 
  if(is.null(index)) {
    index <- 1:nparts(dobj)
  }

  index <- as.integer(unlist(index))
  stopifnot(max(index) <= nparts(dobj) && min(index) > 0)

  # Try to get data from backend all at once
  # If the backend does not support this, we'll have to stitch it together by ourselves
  # TODO: support DArrays and DFrames as well as DLists
  tryCatch({
    partitions <- do_collect(dobj, index)
    partitions
    },error = function(e){
      unlist(lapply(index,do_collect,x=dobj),recursive=FALSE)
  }) 
}

#' @export
setGeneric("do_collect", function(x,parts) {
  standardGeneric("do_collect")
})

#' @export
parts <- function(dobj, index=NULL) {
  if(is.null(index)) index = 1:nparts(dobj) 
  index <- unlist(index)
  stopifnot(is.numeric(index))
  index <- as.integer(index)

  if(max(index) > dobj@nparts)
    stop("Partition index must be smaller than total number of partitions.")
 
  if(min(index) < 1)
    stop("Partition index must be a positive value")

  partitions <- get_parts(dobj, index)

  psize <- lapply(1:nrow(dobj@psize),function(i) dobj@psize[i,])[index]

  partitions <- base::mapply(FUN=function(obj,psize) {
    obj@nparts <- 1L
    obj@backend <- dds.env$driver@backendName
    obj@type <- dobj@type 
    obj@psize <- matrix(psize,nrow=1,ncol=length(psize))
    obj@dim <- psize
    obj
}, partitions, psize)

  stopifnot(length(partitions) == length(index))
  partitions
}

#' @export
nparts <- function(dobj) {
  dobj@nparts
}

#' @export
# TODO: finish definitions, slots
setClass("DObject",
  representation(nparts = "integer", psize = "matrix",
          dim = "integer", dim.names = "list", backend = "character", type = "character"),
  prototype = prototype(nparts = 1L,psize = matrix(1,1),
              dim = c(1L), dim.names = list()))

#' @export
dlist <- function(...,nparts = 1L, psize=matrix(1,1)) {
  nparts = as.integer(nparts)
  psize = matrix(0L,nparts)
  initialize <- list(...)
  if(length(initialize) == 0) {
    new(dds.env$driver@DListClass,backend=dds.env$driver@backendName,type = "DListClass", nparts = nparts, psize = psize, dim = 0L)
  } else{
    dmapply(function(x){ list(x) }, initialize)
  }
}

#' @export
DList <- dlist

#' @export
as.dlist <- function(items) {
  if(!is.list(items))
  items <- as.list(items)

  # Currently, if this is used on a list of dobjects (i.e., with parts()), they must all belong to the same dobject. 
  # TODO: allow reconstituting of multiple dobject partitions into a new one.

  if(is.dobject(items[[1]])) {
    newobj <- combine(dds.env$driver,items)
    newobj@nparts <- length(items)
    newobj@backend <- dds.env$driver@backendName
    newobj@type <- "DListClass"
    return(newobj)
    }

   dmapply(function(x) { list(x) }, items)
}

#' @export
is.dlist <- function(x) {
  is(x,"DObject") && x@type == "DListClass"
}

#' @export
is.dobject <- function(x) {
  is(x,"DObject")
}

#' @export 
is.DObject <- is.dobject

#' @export
is.DList <- is.dlist

#' @export
as.DList <- as.dlist

#' @export
darray <- function(initialize=NULL,nparts = 1L, psize=matrix(1,1,2)) {
  nparts = as.integer(nparts)
  psize = matrix(1L,nparts,2)
  if(is.null(initialize)) {
    new(dds.env$driver@DArrayClass,backend=dds.env$driver@backendName,type = "DArrayClass", nparts = nparts, psize = psize)
  } else{
    dmapply(function(x){ list(x) }, list(initialize))
  }
}

#' @export
DArray <- darray

#' @export
dframe <- function(initialize=NULL,nparts = 1L, psize=matrix(1,1,2)) {
  nparts = as.integer(nparts)
  psize = matrix(1L,nparts,2)
  if(is.null(initialize)) {
    new(dds.env$driver@DFrameClass,backend=dds.env$driver@backendName,type = "DFrameClass", nparts = nparts, psize = psize)
  } else{
    dmapply(function(x){ list(x) }, list(initialize))
  }
}

#' @export
DFrame <- dframe

#' @export
setMethod("show",signature("DObject"),function(object) {

  partsStr <- ""

  limit <- min(10,dim(object@psize)[[1]])

  for(i in 1:limit) {
    if(i>1) partsStr <- paste0(partsStr,", ")
    partsStr <- paste0(partsStr,"[",object@psize[i,],"]")
  }

  if(limit < dim(object@psize)[[1]]){
    partsStr <- paste0(partsStr,", ...")
  }

  printStr <- paste0("\nType: ", object@type,"\nnparts: ", object@nparts,"\npsize: ", partsStr, "\ndim: ", paste(object@dim,collapse=","), "\nBackend Type: ", object@backend,"\n")
 cat(printStr) 
})

#' @export
length.DObject <- function(x) {
  if(is.dlist(x)) x@dim
}

#' @export
names.DObject <- function(x) {
   nobj <- dlapply(x,function(x) { as.list(names(x)) })
   unlist(collect(nobj))
}

#' @export
setReplaceMethod("names", signature(x = "DObject", value = "ANY"), definition = function(x,value) {
  stopifnot(length(value) == length(x))

  lens <- mapply(function(x) { prod(x) }, data.frame(t(x@psize)),SIMPLIFY=FALSE)

  limits <- cumsum(unlist(lens)) 
  limits <- c(0,limits) + 1
  limits <- limits[1:(length(limits)-1)]

  
  namesList <- mapply(function(x,y) {
      value[x:(x+y-1)]
   },
  limits,lens,SIMPLIFY=FALSE)


  dmapply(function(x,y) { names(x) <- y; x }, parts(x), namesList) 
})

#' @export
unlist.DObject <- function(x, recursive, use.names) {
  if(is.dlist(x)) unlist(collect(x),recursive,use.names)
}
