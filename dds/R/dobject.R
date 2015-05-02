# Defines the base class for a distributed object, as well as their methods
# DList, DArray, and DFrame inherit from class DObject

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
  new("DList",backend=create.dobj(dds.env$driver,"DList"),nparts = as.integer(nparts), psize = as.integer(psize))
}

#' @export
DList <- dlist

#' @export
DLIST <- dlist

#' @export
darray <- function(...,nparts = 1L, psize=c(1L,1L)){
  new("DArray",backend=create.dobj(dds.env$driver,"DArray"),nparts = as.integer(nparts), psize = as.integer(psize))
}

#' @export
DArray <- darray
#' @export
DARRAY <- darray

#' @export
dframe <- function(...,nparts = 1L, psize=c(1L,1L)){
  new("DFrame",backend=create.dobj(dds.env$driver,"DFrame"),nparts = as.integer(nparts), psize = as.integer(psize))
}

#' @export
DFrame <- dframe
#' @export
DFRAME <- dframe
