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

#' Moves the data stored in partitions of the distributed object to the local node, and reassembles the data, in the order of the partitions, into the local version of the object.
#'
#' Data in partitions are reassembled sequentially for all DObjects; for DArrays and DFrames, this is row-major order.
#'
#' @param dobj The DObject (DList, DArray, or DFrame) to collect on.
#' @param index A numeric value or vector indicating the partition index or indices to collect on. The resultant object will contain a subset of the original data of the dobject's partitions. If a vector, the result is assembled in the same order as the indices provided; though be aware that for DFrames and DArrays the data will lose its structure (it will be vectorized).
#' @return An R list, matrix, or data frame containing the data stored in all or a subset of the partitions of the input dobject.
#' @examples
#' \dontrun{
#' a <- darray(dim=c(9,9),psize=c(3,3),data=5)
#' b <- collect(a) # 9x9 matrix filled with 5s
#' c <- collect(a,1) # First partition of a, which contains a 3x3 matrix of 5s
#' }
#' @export
collect <- function(dobj, index=NULL) { 
  if(is.null(index)) {
    index <- seq(totalParts(dobj))
  }

  index <- as.integer(unlist(index))
  stopifnot(max(index) <= totalParts(dobj) && min(index) > 0)

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

#' Retrieves, as a list of independent objects of 'DObject' type, pointers to each individual 
#' partition of the input dobj.
#'
#' @param dobj The DObject to retrieve the parts of in list format.
#' @param index By default NULL, this is a numeric vector or list or indices referencing the partitions of the 
#' Dobject to get the parts to. If NULL, then this list contains pointers to all partitions.
#'
#' parts() is mainly designed to be used in conjunction with dmapply when functions are written
#' to be applied over DObjects in a partition-by-partition fashion. In other words, by returning
#' a list of the individual partitions of a DObject, that list can then be used in dmapply statements
#' where the apply function goes over every element of that list, (i.e., every partition of the dobject).
#' @return A list of DObjects, each referring to one partition of the input DObject.
#' @examples
#' \dontrun{
#' a <- darray(psize=c(3,3),dim=c(9,9),data=3) # A darray of 9 partitions, each 3x3
#' b <- dmapply(function(x) sum(x), parts(a)) # dmapply to each 3x3 partition of 'a'
#' c <- parts(a,3) # A list containing one DObject, which is the 3rd partition of 'a'
#' }
#' @export
parts <- function(dobj, index=NULL) {
  if(!is(dobj,"DObject")){
    if(is.list(dobj) && is(dobj[[1]],"DObject")) {
      return(dobj)
    } else {
      stop("input is not a valid DObject")
    }
  }

  if(is.null(index)) index = seq(totalParts(dobj))
  index <- unlist(index)
  stopifnot(is.numeric(index))
  index <- as.integer(index)

  if(max(index) > totalParts(dobj))
    stop("Partition index must be smaller than total number of partitions.")
 
  if(min(index) < 1)
    stop("Partition index must be a positive value")

  partitions <- get_parts(dobj, index)

  psize <- lapply(seq(nrow(dobj@psize)),function(i) dobj@psize[i,])[index]

  partitions <- mapply(FUN=function(obj,psize) {
    obj@nparts <- c(1L, 1L)
    obj@backend <- dds.env$driver@backendName
    obj@type <- dobj@type 
    obj@psize <- matrix(psize,nrow=1,ncol=length(psize))
    obj@dim <- as.integer(psize)
    obj
}, partitions, psize, SIMPLIFY=FALSE)

  stopifnot(length(partitions) == length(index))
  partitions
}

#' Return the 1d (in the case of DLists) or 2d-matrix (DArrays and DFrames)
#' containing the sizes of each partition of the DObject.
#' The ith row of the matrix refers to the ith partition of the DObject.
#' The 1st column refers to the number of rows, and the 2nd is the number of columns.
#' @param dobj The DObject to get the partition sizes of
#' @param index (Default: NULL), a numeric vector or list containing the indices of the
#' @seealso \code{\link{nparts}}, \code{\link{parts}}
#' partition ids to get the sizes for.
#' @return A matrix, which is 1d in the case of DLists, and 2d in the case of DArrays and
#' DFrames, containing the partition sizes of the specified DObjects (if index is NULL), or
#' the partition sizes of the provided ids of partitions (if index is not NULL).
#' @examples 
#' \dontrun{
#' a <- darray(psize=c(3,3),dim=c(9,9)) # 9 partitions of 3x3
#' b <- psize(a) # A 9x2 matrix, with each row containing c(3,3) 
#' }
#' @export
psize <- function(dobj,index=NULL) {
  if(is.null(index)) index = seq(totalParts(dobj))
  index <- unlist(index)
  stopifnot(is.numeric(index))
  index <- as.integer(index)

  if(min(index) < 1 || max(index) > totalParts(dobj))
    stop("Indices must be greater than 1 and smaller than the total number of 
      partitions in the dobject")

  dobj@psize[index,]
}

#' Returns the total number of partitions of the DObject.
#' Note that that this is the same result as prod(nparts(dobj))
#' @param dobj The DObject of which to get the number of partitions.
#' @seealso \code{\link{nparts}}
#' @return The number of partitions the DObject is divided into.
#' @examples
#' \dontrun{
#' a <- darray(psize=c(3,3),dim=c(9,9)) # 9 partitions of 3x3
#' b <- totalParts(a) # Returns 9
#' }
#' @export
totalParts <- function(dobj) {
  prod(dobj@nparts)
}

#' Returns a 2d-vector denoting the number of partitions existing along
#' each dimension of the DObject, where the vector==c(no_of_partitions_per_column,
#' no_of_partitions_per_row). Note that for DLists, the value is always
#' equivalent to c(totalParts(dobj),1).
#' @param dobj The DObject whose 2d partitioning vector to retrieve.
#' @seealso \code{\link{totalParts}}
#' @return A 2d-vectoring containing the number of partitions along each dimension.
#' @examples
#' \dontrun{
#' a <- darray(psize=c(3,3),dim=c(9,9)) # 9 partitions of 3x3
#' b <- nparts(a) # returns c(3,3)
#' }
#' @export
nparts <- function(dobj) {
  dobj@nparts
}

#' @export
# TODO: finish definitions, slots
setClass("DObject",
  representation(nparts = "numeric", psize = "matrix",
          dim = "numeric", dim.names = "list", backend = "character", type = "character"),
  prototype = prototype(nparts = c(1L, 1L),psize = matrix(1,1),
              dim = c(1L), dim.names = list()))

#' Creates a dlist with the specified partitioning and data.
#' @param ... Values to initialize the DList with (optional).
#' @param nparts (Default is 1L) The number of partitions this DList should have.
#' @return A DList containing the data in ..., or an empty DList, partitioned accordingly based on nparts.
#' @examples
#' \dontrun{
#' a <- dlist(1,2,3,4,nparts=2) # A DList containing 2 partitions, with data 1 to 4.
#' }
#' @export
dlist <- function(...,nparts = 1L) {
  nparts <- as.integer(nparts)
  if(length(nparts) == 1)
    nparts = c(nparts, 1L) #The second dimension is always 1 for dlists
  psize = matrix(0L,nparts[1])
  initialize <- list(...)
  if(length(initialize) == 0) {
    new(dds.env$driver@DListClass,backend=dds.env$driver@backendName,type = "DListClass", nparts = nparts, psize = psize, dim = 0L)
  } else{
    dmapply(function(x){ x }, initialize,nparts=nparts)
  }
}

#' @export
DList <- dlist

#' Creates a dlist from the input.
#' @param items The object to convert to a DList
#' @param nparts The number of partitions for the resulting DList to have.
#' @seealso \code{\link{dlist}}
#' @return A DList converted from the input.
#' Note that a list of partitions (resulting from the use of parts()) may
#' be used with as.dlist. This will recombine those partitions into a single 
#' DObject.
#' @examples
#' \dontrun{
#' a <- as.dlist(list(1,2,3,4)) # A DList with elements 1 to 4.
#' b <- as.dlist(parts(a,c(3,4))) # A new DList with only 2 partitions, which
#' were partitions 3 and 4 of 'a'.
#' } 
#' @export 
as.dlist <- function(items,nparts=NULL) {
  if(!is.list(items))
  items <- as.list(items)

  if(length(nparts) == 1) nparts <- c(nparts,1L)

  # Currently, if this is used on a list of dobjects (i.e., with parts()), they must all belong to the same dobject. 
  # TODO: allow reconstituting of multiple dobject partitions into a new one.

  if(is.dobject(items[[1]])) {
    newobj <- combine(dds.env$driver,items)
    newobj@nparts <- c(length(items), 1L)
    newobj@backend <- dds.env$driver@backendName
    newobj@type <- "DListClass"
    return(newobj)
  }

   dmapply(function(x) { x }, items)
}

#' Returns whether the input entity is a DList
#' @param x The input to test to see whether it is a DList.
#' @return TRUE if x is a DList, FALSE otherwise
#' @examples
#' \dontrun{
#' is.dlist(3) #FALSE
#' is.dlist(dlist(1,2,3,nparts=3)) #TRUE
#' }
#' @export
is.dlist <- function(x) {
  is(x,"DObject") && x@type == "DListClass"
}

#' @export
is.DList <- is.dlist

#' @export
as.DList <- as.dlist

#' Returns whether the input entity is a DObject
#' @param x The input to test to see whether it is a DObject.
#' @return TRUE if x is a DObject, FALSE otherwise
#' @examples
#' \dontrun{
#' is.dobject(3) # FALSE
#' is.dobject(dlist(1,2,3,nparts=3)) # TRUE
#' is.dobject(darray(psize=c(3,3),dim=c(9,9))) # TRUE
#' }
#' @export
is.dobject <- function(x) {
  is(x,"DObject")
}

#' @export 
is.DObject <- is.dobject

#' Creates a darray with the specified partitioning and data.
#' @param nparts The number of partitions this DArray should have. If this is not provided or is NULL, then psize and dim must be provided together.
#' @param dim The dimensions of the overall DArray to construct. Must be provided together with psize.
#' @param psize A 2d-vector indicating the size of each partition. In general, each 
#' dimension of this vector has a value that evenly divides into dim; if not, then the last partition will be smaller. This parameter is provided together with dim.
#' @param data (Default value: 0) The value to which each element of the DArray should be initialized to.
#' @return A DArray, with each partition equal to size psize and number of partitions in each dimension equal to nparts, with each element initialized to data.
#' @examples
#' \dontrun{
#' a <- darray(psize=c(3,3),dim=c(9,9),data=5) # A 9 partition (each partition 3x3), 9x9 DArray with each element initialized to 5.
#' b <- darray(psize=c(3,3),dim=c(9,9)) # Same as 'a', but filled with 0s.
#' c <- darray(nparts=c(2,3)) # An empty darray with 6 partitions, 2 per column and 3 per row.
#' }
#' @export
darray <- function(nparts = NULL, dim=NULL, psize = NULL, data = 0) {
 
  if(!is.null(dim) || !is.null(psize)) {
    if(is.null(psize) || is.null(dim)) stop("Need to supply both psize and dim")
    if(!is.null(nparts)) stop("Cannot supply nparts as well as psize and dimensions")

    # Test for legality of dim and psize specifications
    checkDimAndPsize(dim, psize)

    nparts <-  c(ceiling(dim[1]/psize[1]), ceiling(dim[2]/psize[2]))
    totalParts<-prod(nparts)

    # Create number of rows equal to number of parts
    sizes <- t(matrix(psize))
    sizes <- sizes[rep(seq_len(nrow(sizes)), totalParts),]
    #Last column of partitions can be smaller if dim is not properly divisible
    if((dim[2]%%psize[2])!=0){
	sizes[seq(from=nparts[2], to=totalParts, by=nparts[2]),2] <- (dim[2]%%psize[2])
    }
    #Last row of partitions can be smaller if dim is not properly divisible
    if((dim[1]%%psize[1])!=0){
	sizes[(totalParts-nparts[2]+1):totalParts,1] <- (dim[1]%%psize[1])
    }
    psize<-sizes
  }

 # If all are NULL, then initialize to some default
  if(is.null(nparts)) {
    nparts <- c(1L, 1L)
  }

  if(is.null(dim)) {
    psize <- matrix(0L,prod(nparts),2)
    dim <- c(0L,0L)
  }

  #Check that nparts should be two dimensional
  if(length(nparts)==1) nparts <- c(1L,nparts)
  if(length(nparts)!=2) stop("length(nparts) should be two: current nparts", nparts)
  nparts <- as.integer(nparts)
  
  dim <- as.integer(dim)

  if(all(dim==0)) {
    new(dds.env$driver@DArrayClass,backend=dds.env$driver@backendName,type = "DArrayClass", nparts = nparts, psize = psize, dim=dim)
  } else{
    sizes<-unlist(apply(psize,1,function(y)list(y)), recursive=FALSE)
    dmapply(function(d, v){ matrix(data=v,nrow=d[1], ncol=d[2]) }, sizes, MoreArgs=list(v=data), output.type="DArrayClass", combine="row", nparts=nparts)
  }
}

#' @export
DArray <- darray

#' Returns whether the input entity is a DArray
#' @param x The input to test to see whether it is a DArray.
#' @return TRUE if x is a DArray, FALSE otherwise
#' @examples
#' \dontrun{
#' is.darray(3) # FALSE
#' is.darray(darray(psize=c(3,3),dim=c(9,9))) # TRUE
#' }
#' @export
is.darray <- function(x) {
  is(x,"DObject") && x@type == "DArrayClass"
}

#' @export
is.DArray <- is.darray

#' Creates a DFrame with the specified partitioning and data.
#' @param nparts The number of partitions this DFrame should have. If this is not provided or is NULL, then psize and dim must be provided together.
#' @param dim The dimensions of the overall DFrame to construct. Must be provided together with psize.
#' @param psize A 2d-vector indicating the size of each partition. In general, each 
#' dimension of this vector has a value that evenly divides into dim; if not, then the last partition will be smaller. This parameter is provided together with dim.
#' @param data (Default value: 0) The value to which each element of the DFrame should be intialized to.
#' @return A DFrame, with each partition equal to size psize and number of partition in each dimension equal to nparts, with each element initialized to data.
#' @examples
#' \dontrun{
#' a <- dframe(psize=c(3,3),dim=c(9,9),data=5) # A 9 partition (each partition 3x3), 9x9 DFrame with each element initialized to 5.
#' b <- dframe(psize=c(3,3),dim=c(9,9)) # Same as 'a', but filled with 0s.
#' c <- dframe(nparts=c(2,3)) # An empty DFrame with 6 partitions, 2 per column and 3 per row.
#' }
#' @export
dframe <- function(nparts = NULL, dim=NULL, psize = NULL, data = 0) {

  if(!is.null(dim) || !is.null(psize)) {
    if(is.null(psize) || is.null(dim)) stop("Need to supply both psize and dim")
    if(!is.null(nparts)) stop("Cannot supply nparts as well as psize and dimensions")

    # Test for legality of dim and psize specifications
    checkDimAndPsize(dim, psize)

    nparts <-  c(ceiling(dim[1]/psize[1]), ceiling(dim[2]/psize[2]))
    totalParts<-prod(nparts)

    # Create number of rows equal to number of parts
    sizes <- t(matrix(psize))
    sizes <- sizes[rep(seq_len(nrow(sizes)), totalParts),]
    #Last column of partitions can be smaller if dim is not properly divisible
    if((dim[2]%%psize[2])!=0){
	sizes[seq(from=nparts[2], to=totalParts, by=nparts[2]),2] <- (dim[2]%%psize[2])
    }
    #Last row of partitions can be smaller if dim is not properly divisible
    if((dim[1]%%psize[1])!=0){
	sizes[(totalParts-nparts[2]+1):totalParts,1] <- (dim[1]%%psize[1])
    }
    psize<-sizes

  }

  # If all are NULL, then initialize to some default
  if(is.null(nparts)) {
    nparts <- c(1L, 1L)
  }

  if(is.null(dim)) {
    psize <- matrix(0L,prod(nparts),2)
    dim <- c(0L,0L)
  }

  #Check that nparts should be two dimensional
  if(length(nparts)==1) nparts <- c(1L,nparts)
  if(length(nparts)!=2) stop("length(nparts) should be two")
  nparts <- as.integer(nparts)

  dim <- as.integer(dim)

 if(all(dim==0)) {
    new(dds.env$driver@DArrayClass,backend=dds.env$driver@backendName,type = "DFrameClass", nparts = nparts, psize = psize, dim=dim)
  } else{
    sizes<-unlist(apply(psize,1,function(y)list(y)), recursive=FALSE)
    dmapply(function(d, v){ data.frame(matrix(data=v,nrow=d[1], ncol=d[2])) }, sizes, MoreArgs=list(v=data), output.type="DFrameClass", combine="row", nparts=nparts)
  }
}

#' @export
DFrame <- dframe

#' Returns whether the input entity is a DFrame
#' @param x The input to test to see whether it is a DFrame.
#' @return TRUE if x is a DFrame, FALSE otherwise
#' @examples
#' \dontrun{
#' is.dframe(3) # FALSE
#' is.dframe(dframe(psize=c(3,3),dim=c(9,9))) # TRUE
#' }
#' @export
is.dframe <- function(x) {
  is(x,"DObject") && x@type == "DFrameClass"
}

#' @export
is.DFrame <- is.dframe

#' @export
setMethod("show",signature("DObject"),function(object) {

  partsStr <- ""

  limit <- min(10,dim(object@psize)[[1]])

  for(i in seq(limit)) {
    if(i>1) partsStr <- paste0(partsStr,", ")
    dims <- paste0("",object@psize[i,],collapse=", ")
    partsStr <- paste0(partsStr,"[",dims,"]")
  }

  if(limit < dim(object@psize)[[1]]){
    partsStr <- paste0(partsStr,", ...")
  }

  printStr <- paste0("\nType: ", object@type,"\nNo. of Partitions: ", totalParts(object), "\nnparts: ", paste(object@nparts,collapse=","),"\npsize: ", partsStr, "\ndim: ", paste(object@dim,collapse=","), "\nBackend Type: ", object@backend,"\n")

  cat(printStr) 
})

#' @export
length.DObject <- function(x) {
  if(is.dlist(x)) dim(x)[[1]]
  else if(is.darray(x)) prod(dim(x))
  else dim(x)[[2]]
}

#' @export
names.DObject <- function(x) {
   nobj <- dlapply(parts(x),function(x) { as.list(names(x)) })
   unlist(collect(nobj))
}

#' @export
setReplaceMethod("names", signature(x = "DObject", value = "ANY"), definition = function(x,value) {
  stopifnot(length(value) == length(x))

  lens <- sapply(data.frame(t(x@psize)), function(x) { prod(x) })

  limits <- cumsum(lens)
  limits <- c(0,limits) + 1
  limits <- limits[seq(length(limits)-1)]

  
  namesList <- mapply(function(x,y) {
      value[x:(x+y-1)]
   },
  limits,lens,SIMPLIFY=FALSE)

  dmapply(function(x,y) { names(x) <- y; x }, parts(x), namesList,.unlistEach=TRUE, nparts=totalParts(x)) 
})

#' @export
unlist.DObject <- function(x, recursive, use.names) {
  if(is.dlist(x)) unlist(collect(x),recursive,use.names)
}

#' Repartitions a DObject.
#' This function takes two inputs, dobj, and skeleton. These inputs must both be DObjects of the same type and same dimension.
#' If dobj and skeleton have different internal partitioning, this function will return a new dobject with the same internal data as in dobj but with the partitioning scheme of skeleton.
#' @param dobj The DObject whose data is to be preserved, but repartitioned.
#' @param skeleton The DObject whose partitioning is to be emulated in the output.
#' @return A new DObject with the data of 'dobj' and the partitioning of 'skeleton'. 
#' @examples
#' \dontrun{
#' a <- dlist(1,2,3,4,nparts=2)
#' b <- dmapply(function(x) x, 10:14,nparts=4)
#' c <- repartition(a,b) # c will have 4 partitions of length 1 each, containing 1 to 4.
#' }
#' @export
repartition <- function(dobj, skeleton) {
  UseMethod("repartition")
}

#' @export
repartition.DObject <- function(dobj,skeleton) {
 
   verticalValues <- NULL
   horizontalValues <- NULL
   index <- 0
   dims <- length(dim(skeleton))

   stopifnot(dim(dobj) == dim(skeleton))

   # Don't do anything if partitioning is already the same
   if(identical(psize(dobj),psize(skeleton))) return(dobj)

  if(dims > 1) {
     while(is.null(horizontalValues) || tail(horizontalValues,n=1L) < dim(dobj)[[2]]) {
       if(is.null(horizontalValues)) prevMax <- 0
       else prevMax <- horizontalValues[index]
       index <- index + 1
       horizontalValues <- c(horizontalValues,dobj@psize[index,][[2]] + prevMax)
     }
   }

   nparts_per_row <- ifelse(dims > 1, index, 1)

   count <- 0
   if(index==0) index <- 1

   while(is.null(verticalValues) || tail(verticalValues,n=1L) < dim(dobj)[[1]]) {
     if(is.null(verticalValues)) prevMax <- 0
     else prevMax <- verticalValues[count]
     verticalValues <- c(verticalValues,dobj@psize[index,][[1]] + prevMax)
     index <- index + nparts_per_row
     count <- count + 1
   }

  cur_row <- 0
  cur_col <- 0
  index <- 0

  starts_and_ends <- matrix(0,totalParts(skeleton),4)

  if(dims > 1) {
    col_end <- dim(skeleton)[[2]]
  } else {
    col_end <- 0
  }

  while(index < totalParts(skeleton)) {

    index <- index + 1

    start_x <- cur_row + 1  
    end_x <- cur_row + skeleton@psize[index,1]

    start_y <- cur_col + 1

    if(dims > 1) {
      end_y <- cur_col + skeleton@psize[index,2]
    } else {
      end_y <- 1
    }
 
    starts_and_ends[index,] <- c(start_x,end_x,start_y,end_y)

    cur_col <- end_y

    if(cur_col >= col_end) {
      cur_col <- 0
      cur_row <- cur_row + skeleton@psize[index,1]
    }
  }

  if(dims==1) {
    partitionIdsAndOffsets <- mapply(getIdsAndOffsets,starts_and_ends[,1],starts_and_ends[,2],starts_and_ends[,3],starts_and_ends[,4],MoreArgs=list(vertical=verticalValues,psizes=dobj@psize),SIMPLIFY=FALSE)
  } else {
  partitionIdsAndOffsets <- mapply(getIdsAndOffsets,starts_and_ends[,1],starts_and_ends[,2],starts_and_ends[,3],starts_and_ends[,4],MoreArgs=list(vertical=verticalValues,horizontal=horizontalValues,psizes=dobj@psize),SIMPLIFY=FALSE)
  }

  max_parts <- 0

  for(a in seq(1,length(partitionIdsAndOffsets))) {
     numParts <- length(partitionIdsAndOffsets[[a]][["starts"]])
     if(numParts > max_parts) {
       max_parts <- numParts
     }
  }


  dmapplyArgs <- lapply(seq((max_parts*3)), function(x) {
                             ind <- ceiling(x/3)
                             if(x %% 3 == 1) field = "parts"
                             else if (x %% 3 == 2) field = "starts"
                             else field = "ends"
                             lapply(partitionIdsAndOffsets, function(y) {
                                      if(ind > length(y[[field]])) return(NA)
                                      value <- y[[field]][[ind]]
                                      if(field=="parts") return(parts(dobj,value)[[1]])
                                      else return(value)
                                   })
                           })


  repartitioner <- function(...,psize,type) {
    
    dataPartitions <- list(...)

    index <- 1
    dims <- length(psize)    

    if(dims > 2) stop("Cannot repartition object with more than 2 dimensions")

    if(type=="DListClass") {
      output <- list()    
    } else if(type=="DFrameClass") {
      output <- data.frame(matrix(0,psize[[1]],psize[[2]]))
    } else {
      output <- matrix(0,psize[[1]],psize[[2]])
    }

    currentPosition <- rep(1,dims)

    while(index <= length(dataPartitions) - 2 && !is.na(dataPartitions[[index]])) {

      oldPartition <- dataPartitions[[index]]
  
      start <- dataPartitions[[index+1]]
      end <- dataPartitions[[index+2]]
      endingPosition <- currentPosition + end - start

      if(type=="DListClass") {
        output[currentPosition:endingPosition] <- oldPartition[start:end]
      } else {
        output[currentPosition[[1]]:endingPosition[[1]],currentPosition[[2]]:endingPosition[[2]]] <-
        oldPartition[start[[1]]:end[[1]],start[[2]]:end[[2]]]
      }

      index <- index + 3

      if(dims > 1) {
        if(psize[[2]] > endingPosition[[2]]) {
          currentPosition <- c(currentPosition[[1]],endingPosition[[2]]+1)
        } else { 
          currentPosition <- c(endingPosition[[1]]+1,1)
        }
      } else {
        currentPosition <- endingPosition + 1
      }
    }
    output 
  }

  if(skeleton@type == "DListClass") type = list()
  else if(skeleton@type == "DArrayClass") type = matrix(1)
  else type = data.frame(1)

  if(skeleton@type == "DListClass") .unlistEach=TRUE
  else .unlistEach=FALSE

  dmapplyArgs <- c(FUN=repartitioner,dmapplyArgs,psize=list(as.list(data.frame(t(skeleton@psize)))),MoreArgs=list(list(type=skeleton@type)),output.type=list(skeleton@type),combine=list("row"),nparts=list(nparts(skeleton)),.unlistEach=list(.unlistEach))

  do.call(dmapply,dmapplyArgs)
}

# Given a starting x and y range, get the full list of partition ids and offsets
getIdsAndOffsets <- function(start_x,end_x,start_y,end_y,vertical,horizontal=NULL,psizes) {

  # Top left corner
  start_x_start_y <- getCorners(start_x,start_y,vertical,horizontal)

  # Bottom right corner
  end_x_end_y <- getCorners(end_x,end_y,vertical,horizontal)

  if(!is.null(horizontal)) {
    lenVertical <- floor((end_x_end_y[[1]]-start_x_start_y[[1]])/length(horizontal))
  } else {
    lenVertical <- end_x_end_y[[1]]-start_x_start_y[[1]]
  }

  if(is.null(horizontal)) {
    start_x_end_y <- start_x_start_y
    end_x_start_y <- end_x_end_y
  } else {
    start_x_end_y <- list(end_x_end_y[[1]] - lenVertical*length(horizontal),c(start_x_start_y[[2]][[1]],end_x_end_y[[2]][[2]]))
    end_x_start_y <- list(lenVertical*length(horizontal) + start_x_start_y[[1]], c(end_x_end_y[[2]][[1]],start_x_start_y[[2]][[2]])) 
  }

  lenHorizontal <- start_x_end_y[[1]]-start_x_start_y[[1]]

  if(is.null(horizontal)) partitions_range <- start_x_start_y[[1]]:end_x_end_y[[1]]
  else partitions_range <- start_x_start_y[[1]]:start_x_end_y[[1]] 
  
  partitions <- partitions_range

  if(lenVertical > 0 && !is.null(horizontal)) {
    for(i in seq(lenVertical)) {
      partitions <- c(partitions,partitions_range+length(horizontal)*i)
    }
  }

  offset_start <- list(start_x_start_y[[2]])
  
  if(is.null(horizontal)) {
    offset_start <- c(offset_start,rep(list(1),lenVertical))
  } else {
    offset_start <- c(offset_start,rep(list(c(start_x_start_y[[2]][[1]],1)),lenHorizontal))
  }

  if(lenVertical > 0 && !is.null(horizontal)) {
    for(i in seq(lenVertical)) {
       offset_start <- c(offset_start,list(c(1,start_x_start_y[[2]][[2]])))
       offset_start <- c(offset_start,rep(list(c(1,1)),lenHorizontal))
    }
  }

  offset_end <- list(end_x_end_y[[2]])

  partitionIdRow <- seq(end_x_end_y[[1]]-lenHorizontal,end_x_end_y[[1]]-1)
  partitionIdRow <- rev(partitionIdRow)

  partitionIdCol <- rev(seq(start_x_start_y[[1]],end_x_end_y[[1]]-1))

  if(lenHorizontal > 0 || is.null(horizontal)) {
    if(is.null(horizontal)) {
      if(lenVertical > 0) {
        offset_end <- c(offset_end,lapply(partitionIdCol,function(x) { psizes[x,1] } ))
      }
    } else {
      offset_end <- c(offset_end,lapply(partitionIdRow,function(x) { c(end_x_end_y[[2]][[1]],psizes[x,2]) }))
    } 
  }  

  if(lenVertical > 0 && !is.null(horizontal)) {
    for(i in seq(lenVertical)) {
      endPartition <- end_x_end_y[[1]] - i*length(horizontal)
      partitionIdRow <- seq(endPartition - lenHorizontal,endPartition-1)
      partitionIdRow <- rev(partitionIdRow)
      offset_end <- c(offset_end,list(c(psizes[endPartition,1],end_x_end_y[[2]][[2]])))
      if(lenHorizontal > 0) {
        offset_end <- c(offset_end,lapply(partitionIdRow,function(x) { psizes[x,] }))
      }
    }
  }

  offset_end <- rev(offset_end)

  list(parts=partitions,starts=offset_start,ends=offset_end)
}

# Given coordinates x and y, with horizontal and vertical cutoffs, return the partition id and offset
getCorners <- function(x,y,vertical,horizontal=NULL) {
  # Binary search through each dimension

  times <- ifelse(is.null(horizontal),1,2)
  indices <- NULL
  offsets <- NULL

  if(!is.null(horizontal))
    numPerRow <- length(horizontal)
  
  for(i in seq(times)) {
    lower <- 1
    upper <- ifelse(i==1,length(vertical),length(horizontal))

    if(i==2) {
      x <- y
      vertical <- horizontal
    }

    while(lower<=upper) {
      index <- floor((upper + lower)/2)
      if(vertical[index] >= x) {
        if(index > 1) {
          if(vertical[index-1] < x) {
            offset <- x - vertical[index-1]
            break
          }
        } else {
           offset <- x 
           break
        }
         upper <- index - 1  
      } else {
        if(index < length(vertical)) {
          if(vertical[index+1] > x) {
            index <- index+1
            offset <- x - vertical[index-1]
            break
          }
        } else {
          stop("not found")
          break
         }
         lower <- index + 1
      }
    } 
     
     indices <- c(indices,index)
     offsets <- c(offsets,offset)

  }

  
  if(length(indices) == 1) {
    partition_id <- indices[[1]]
  } else {
    partition_id <- numPerRow*(indices[[1]]-1) + indices[[2]]
  }

  list(partition_id,offsets)

}

#Helper function to check dimension and psizes when DObjects are initialized
checkDimAndPsize<-function(dim, psize){
 
 if(class(dim)!="numeric" && class(dim)!="integer") stop("dim should be numeric")
 if(class(psize)!="numeric" && class(psize)!="integer") stop("psize should be numeric")
 if(length(dim)!=2||length(psize)!=2) stop("length(dim) and length(psize) should be two")
 if(!all(dim==floor(dim)))stop("dim should be integral values")
 if(!all(psize==floor(psize)))stop("psize should be integral values")
 if(all(psize<=.Machine$integer.max) == FALSE) stop(paste("psize should be less than",.Machine$integer.max))
 if(dim[1]<=0||dim[2]<=0||psize[1]<=0||psize[2]<=0) stop("dim and psize should be larger than 0")
 if(dim[1]<psize[1]||dim[2]<psize[2]) stop("psize should be smaller than dim")
}

#' @export
as.darray <- function(input, psize=NULL) {
   convertToDobject(input, psize, type="array") 
}

#' @export
as.dframe <- function(input, psize=NULL) {
   convertToDobject(input, psize, type="data.frame") 
}


convertToDobject<-function(input, psize, type){

   mdim <- dim(input)
    if(is.null(psize)){
	#Create as many partitions as the no. of executors in the system
	psize<-mdim
        psize[1]<-ceiling(psize[1]/dds.env$nexecutors)
    }
    numparts<-c(ceiling(mdim[1]/psize[1]), ceiling(mdim[2]/psize[2]))

    if(psize[1]>mdim[1] || psize[2]>mdim[2]){
        ## check if input block dimension is larger than input matrix
        stop("input darray and matrix dimensions do not conform")
     }
    
    # return subset of matrix or data.frame given the block size and the index
    # the block is counted from left to right. then top to down
    # For example, (1,1)(1,2)(2,1)(2,2) is index of 1,2,3,4, respectively
    get_sub_object <- function(in_mat, b_size, index, type){
      nrow = dim(in_mat)[1] #number of rows
      ncol = dim(in_mat)[2]
      nb_per_r = ceiling(ncol/b_size[2]) #number of blocks in a row
      if(index>nb_per_r*(ceiling(nrow/b_size[1]))){
	stop("index out of range")
      }
      b_row_idx = 1+(b_size[1])*(floor((index-1)/nb_per_r)) #begin row index of submatrix
      b_col_idx = 1+(b_size[2])*((index-1)%%nb_per_r)  #begin column index of submatrix
      e_row_idx = ifelse(b_row_idx+b_size[1]-1<nrow, b_row_idx+b_size[1]-1, nrow)
      e_col_idx = ifelse(b_col_idx+b_size[2]-1<ncol, b_col_idx+b_size[2]-1, ncol)
      if(type == "array")
          return (matrix(in_mat[b_row_idx:e_row_idx, b_col_idx:e_col_idx], nrow=(e_row_idx-b_row_idx+1), ncol=(e_col_idx-b_col_idx+1)))
      else 
      	  return (data.frame(in_mat[b_row_idx:e_row_idx, b_col_idx:e_col_idx]))
      }

    matrixList<-lapply(1:prod(numparts), FUN=function(x){get_sub_object(input, psize,x, type)})
    answer<-NULL
    if(type == "array")
       answer<-dmapply(FUN=function(x){x}, matrixList, output.type="DArrayClass", combine="row", nparts=numparts) 
    else
       answer<-dmapply(FUN=function(x){x}, matrixList, output.type="DFrameClass", combine="row", nparts=numparts) 
    
    if(is.null(dimnames(input)) == FALSE)
        dimnames(answer) <- dimnames(input)
    return (answer)
}