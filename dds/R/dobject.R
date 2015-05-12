# Defines the base class for a distributed object, as well as their methods
# DList, DArray, and DFrame inherit from class DObject

#' @export
collect <- function(dobj, index=NULL) { 
  if(is.null(index)) {
    index <- 1:nparts(dobj)
  }

  index <- as.integer(unlist(index))
    

  assert_that(max(index) <= nparts(dobj) && min(index) > 0)

  # Try to get data from backend all at once
  # If the backend does not support this, we'll have to stitch it together by ourselves
  # TODO: support DArrays and DFrames as well as DLists
  tryCatch({
    partitions <- do_collect(dobj@backend, index)
    assert_that(length(partitions) == length(index))
    partitions
    },error = function(e){
      lapply(index,do_collect,backend=dobj@backend)        
  }) 
}

#' @export
setGeneric("do_collect", function(x,parts=NULL) {
  standardGeneric("do_collect")
}

#' @export 
parts <- function(dobj, index=NULL) {
  if(is.null(index)) index = 1:nparts(dobj) 
  index <- unlist(index)
  assert_that(is.numeric(index))
  index <- as.integer(index)

  type = class(dobj)[[1]]

  if(max(index) > dobj@nparts)
    stop("Partition index must be smaller than total number of partitions.")
 
  if(min(index) < 1)
    stop("Partition index must be a positive value")

  partitions <- get_parts(dobj@backend, index)

  partitions <- lapply(partitions, function(backend) {
    # TODO: set psize and dim accordingly
    new(type,nparts=1L,backend=backend)
})

  assert_that(length(partitions) == length(index))
  partitions
}

#' @export
nparts <- function(dobj) {
  dobj@nparts
}

# TODO: finish definitions, slots
setClass("DObject",
  representation(nparts = "integer", psize = "integer",
          dim = "integer", dim.names = "list", backend = "Backend"))

# TODO: finish definitions, slots
setClass("DList",
  slots = list(),
  prototype = prototype(nparts = 1L,psize = c(1L,1L),
              dim = c(1L,1L), dim.names = list()),
  contains = "DObject")

# TODO: finish definitions, slots
setClass("DArray",
  slots = list(),
  prototype = prototype(nparts = 1L,psize = c(1L,1L),
              dim = c(1L,1L), dim.names = list()),
  contains = "DObject")

# TODO: finish definitions, slots
setClass("DFrame",
  slots = list(),
  prototype = prototype(nparts = 1L,psize = c(1L,1L),
              dim = c(1L,1L), dim.names = list()),
  contains = "DObject")

#' @export
dlist <- function(...,nparts = 1L, psize=c(1L,1L)){
  nparts = as.integer(nparts)
  psize = as.integer(psize)
  new("DList",backend=create.dobj(dds.env$driver,"DList",nparts=nparts,psize=psize),nparts = nparts, psize = psize)
}

#' @export
DList <- dlist

#' @export
DLIST <- dlist

#' @export
darray <- function(...,nparts = 1L, psize=c(1L,1L)){
  nparts = as.integer(nparts)
  psize = as.integer(psize)
  new("DArray",backend=create.dobj(dds.env$driver,"DArray",nparts=nparts,psize=psize),nparts = nparts, psize = psize)
}

#' @export
DArray <- darray
#' @export
DARRAY <- darray

#' @export
dframe <- function(...,nparts = 1L, psize=c(1L,1L)){
  nparts = as.integer(nparts)
  psize = as.integer(psize)
  new("DFrame",backend=create.dobj(dds.env$driver,"DFrame",nparts=nparts,psize=psize),nparts = nparts, psize = psize)
}

#' @export
DFrame <- dframe
#' @export
DFRAME <- dframe
