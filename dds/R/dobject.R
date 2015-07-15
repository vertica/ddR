# Defines the base class for a distributed object, as well as their methods
# DList, DArray, and DFrame inherit from class DObject

#' @export
length.DList <- function(x) {
  x@dim
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
unlist.DList <- function(x, recursive, use.names) {
  unlist(collect(x),recursive,use.names)
}

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
    partitions <- do_collect(dobj@backend, index)
    partitions
    },error = function(e){
      print(e)
      unlist(lapply(index,do_collect,x=dobj@backend),recursive=FALSE)
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

  type = class(dobj)[[1]]

  if(max(index) > dobj@nparts)
    stop("Partition index must be smaller than total number of partitions.")
 
  if(min(index) < 1)
    stop("Partition index must be a positive value")

  partitions <- get_parts(dobj@backend, index)

  psize <- lapply(1:nrow(dobj@psize),function(i) dobj@psize[i,])[index]

  partitions <- base::mapply(FUN=function(backend,psize) {
    obj <- new(type,nparts=1L,backend=backend)
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

# TODO: finish definitions, slots
setClass("DObject",
  representation(nparts = "integer", psize = "matrix",
          dim = "integer", dim.names = "list", backend = "Backend"))

# TODO: finish definitions, slots
setClass("DList",
  slots = list(),
  prototype = prototype(nparts = 1L,psize = matrix(1,1),
              dim = c(1L), dim.names = list()),
  contains = "DObject")

# TODO: finish definitions, slots
setClass("DArray",
  slots = list(),
  prototype = prototype(nparts = 1L,psize = matrix(1,1,1),
              dim = c(1L,1L), dim.names = list()),
  contains = "DObject")

# TODO: finish definitions, slots
setClass("DFrame",
  slots = list(),
  prototype = prototype(nparts = 1L,psize = matrix(1,1,1),
              dim = c(1L,1L), dim.names = list()),
  contains = "DObject")

#' @export
dlist <- function(initialize=NULL,nparts = 1L, psize=matrix(1,1)){
  nparts = as.integer(nparts)
  psize = matrix(1L,nparts)
  if(is.null(initialize)) {
    new("DList",backend=create.dobj(dds.env$driver,"DList",nparts=nparts,psize=psize),nparts = nparts, psize = psize)
  } else{
    dmapply(function(x){ list(x) }, list(initialize))
  }
}

#' @export
DList <- dlist

#' @export
as.dlist <- function(items) {
  items <- as.list(items)

  # Currently, if this is used on a list of dobjects (i.e., with parts()), they must all belong to the same dobject. 
  # TODO: allow reconstituting of multiple dobject partitions into a new one.

  if(is.dobject(items[[1]])) {
    backend <- combine(dds.env$driver,items)
    return(new("DList",backend=backend,nparts=length(items),psize=backend@psize,dim=backend@dim))
    }

   dmapply(function(x) { list(x) }, items)
}

#' @export
is.dlist <- function(x) {
  class(x) == "DList"
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
darray <- function(...,nparts = 1L, psize=matrix(1,1,1)){
  nparts = as.integer(nparts)
  new("DArray",backend=create.dobj(dds.env$driver,"DArray",nparts=nparts,psize=psize),nparts = nparts, psize = psize)
}

#' @export
DArray <- darray

#' @export
dframe <- function(...,nparts = 1L, psize=matrix(1,1,1)){
  nparts = as.integer(nparts)
  new("DFrame",backend=create.dobj(dds.env$driver,"DFrame",nparts=nparts,psize=psize),nparts = nparts, psize = psize)
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

  printStr <- paste0("\nType: ", class(object)[[1]],"\nnparts: ", nparts(object),"\npsize: ", partsStr, "\ndim: ", object@dim, "\nBackend Type: ", class(object@backend)[[1]],"\n")
 cat(printStr) 
})
