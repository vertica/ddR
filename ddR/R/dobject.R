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

#' The baseline distributed object class to be extended by each backend driver.
#' Backends may elect to extend once for all distributed object types ('dlist',
#' 'darray', 'dframe,', etc.) for one per type, depending on needs.
#'
#' @slot nparts Stores the 2d-partitioning scheme of the distributed object.
#' @slot psize Stores, as a 2d-matrix (1d-for dlists) of the size of each
#' partition.
#' @slot dim The dimensions of the distributed object.
#' @slot backend A character vector of the name of the backend that created
#' the object.
#' @slot type The distributed object type for this object (e.g,. 'dlist').
#' @export
setClass("DObject",
  representation(nparts = "numeric", psize = "matrix",
          dim = "numeric", backend = "character", type = "character"),
  prototype = prototype(nparts = c(1L, 1L),psize = matrix(1,1),
              dim = c(1L)))

#' Fetch partition(s) of 'darray', 'dframe' or 'dlist' from remote workers.
#' @param dobj input distributed array, distributed data frame or distributed list.
#' @param index a vector indicating partitions to fetch. If multiple indices are provided, the result is assembled in the same order as the indices provided, though be aware that for dframes and darrays the result may lose its structure.
#' @return An R list, array, or data.frame containing data stored in the partitions of the input.
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
#' a <- darray(dim=c(9,9),psize=c(3,3),data=5)
#' b <- collect(a) # 9x9 matrix filled with 5s
#' c <- collect(a,1) # First partition of a, which contains a 3x3 matrix of 5s
#' }
#' @export
collect <- function(dobj, index=NULL) { 
  if(is.null(index)) {
    index <- seq(totalParts(dobj))
  }

  if(!is.numeric(index)) stop("Invalid argument for index")
  if(!is.dobject(dobj)) stop("Invalid argument for dobj")

  index <- as.integer(unlist(index))

  if(min(index) < 1 || max(index) > totalParts(dobj))
    stop("Indices must be greater than 0 and smaller than the total number of 
      partitions in the dobject")

  # Try to get data from backend all at once
  # If the backend does not support this, we'll have to stitch it together by ourselves
  tryCatch({
    partitions <- do_collect(dobj, index)
    partitions
    },error = function(e){
      unlist(lapply(index,do_collect,x=dobj),recursive=FALSE)
  }) 
}

#' Retrieves, as a list of independent objects, pointers to each individual 
#' partition of the input.
#'
#' @param dobj input object.
#' @param index numeric vector or list of indices referencing the partitions of the 
#' distributed object. If NULL, the returned list contains pointers to all partitions.
#' @details
#' parts() is primarily used in conjunction with dmapply when functions are written
#' to be applied over partitions of distributed objects. 
#' @return a list of distributed objects, each referring to one partition of the input.
#' @details
#' In ddR, each element of parts() is itself considered a distributed object of one partition.
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
#' a <- darray(psize=c(3,3),dim=c(9,9),data=3) # A darray of 9 partitions, each 3x3
#' b <- dmapply(function(x) sum(x), parts(a)) # dmapply to each 3x3 partition of 'a'
#' c <- parts(a,3) # A list containing one DObject, which is the 3rd partition of 'a'
#' }
#' @export
parts <- function(dobj, index=NULL) {
  # If the input is already in parts format, just return the input
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

  psize <- lapply(index,function(x) psize(dobj,x))

  stopifnot(length(partitions) == length(index))

  for(i in seq(1,length(partitions))){
    partitions[[i]]@nparts <- c(1L, 1L)
    partitions[[i]]@backend <- ddR.env$driver@backendName
    partitions[[i]]@type <- dobj@type 
    partitions[[i]]@psize <- matrix(psize[[i]],nrow=1,ncol=length(psize[[i]]))
    partitions[[i]]@dim <- as.integer(psize[[i]])
  }
  
  partitions
}

#' Return sizes of each partition of the input distributed object.
#' @param dobj input distributed object
#' @param index a numeric vector or list containing the indices of the partitions. Default is NULL.
#' @return A matrix that denotes the number of rows and columns in the
#' partition. Row i of the matrix corresponds or size of i'th partition. For a dlist, the returned matrix has only 1 column.
#' @seealso \code{\link{nparts}}, \code{\link{parts}}
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
    stop("Indices must be greater than 0 and smaller than the total number of 
      partitions in the dobject")

  ans <- dobj@psize[index,]
  if(!is.matrix(ans)) {
    ans <- as.matrix(ans)
    if(!is.dlist(dobj)) ans <- t(ans)
  }
  ans
}

#' Returns a 2d-vector denoting the number of partitions existing along
#' each dimension of the distributed object, where the vector==c(partitions_per_column,
#' partitions_per_row). For a dlist, the value is 
#' equivalent to c(totalParts(dobj),1).
#' @param dobj input distributed array, data.frame or list.
#' @seealso \code{\link{totalParts}}
#' @return A 2d-vector containing the number of partitions along each dimension.
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
#' a <- darray(psize=c(3,3),dim=c(9,9)) # 9 partitions of 3x3
#' b <- nparts(a) # returns c(3,3)
#' }
#' @export
nparts <- function(dobj) {
  dobj@nparts
}

#' Returns the total number of partitions of the distributed object.
#' The result is same as prod(nparts(dobj))
#' @param dobj input distributed array, data.frame, or list.
#' @seealso \code{\link{nparts}}
#' @return The total number of partitions in the distributed object.
#' @examples
#' \dontrun{
#' a <- darray(psize=c(3,3),dim=c(9,9)) # 9 partitions of 3x3
#' b <- totalParts(a) # Returns 9
#' }
#' @export
totalParts <- function(dobj) {
  prod(nparts(dobj))
}

#' Creates a distributed list with the specified partitioning and data.
#' @param ... values to initialize the dlist (optional).
#' @param nparts number of partitions in the dlist. If NULL, nparts will equal the length of ...
#' @return A dlist containing the data in ..., or an empty dlist, partitioned according to \code{\link{nparts}}.
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
#' ## A dlist containing 2 partitions, with data 1 to 4.
#' a <- dlist(1,2,3,4,nparts=2) 
#' collect(a)
#' }
#' @export
dlist <- function(...,nparts = NULL) {
  if(is.null(nparts)) nparts <- length(list(...))
  nparts <- as.integer(nparts)
  if(length(nparts) == 1)
    nparts = c(nparts, 1L) #The second dimension is always 1 for dlists
  psize = matrix(0L,nparts[1])
  initialize <- list(...)
  if(length(initialize) == 0) {
    new(ddR.env$driver@DListClass,backend=ddR.env$driver@backendName,type = "dlist", nparts = nparts, psize = psize, dim = 0L)
  } else{
    dmapply(function(x){ x }, initialize,nparts=nparts)
  }
}

#' @rdname dlist
#' @export
DList <- dlist

#' Creates a distributed list from the input.
#' @param items The object to convert to a dlist.
#' @param nparts The number of partitions in the resulting dlist.
#' @seealso \code{\link{dlist}}
#' @return A dlist converted from the input.
#' Note that a list of partitions (resulting from the use of parts()) may
#' be used with as.dlist. This will recombine those partitions into a single 
#' distributed object.
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
#' a <- as.dlist(list(1,2,3,4)) # A dlist with elements 1 to 4.
#' ## A new dlist with only 2 partitions, which were partitions 3 and 4 of 'a'.
#' b <- as.dlist(parts(a,c(3,4))) 
#' } 
#' @export 
as.dlist <- function(items,nparts=NULL) {
  if(!is.list(items))
  items <- as.list(items)

  if(length(nparts) == 1) nparts <- c(nparts,1L)

  # Currently, if this is used on a list of dobjects (i.e., with parts()), they must all belong to the same dobject. 
  # TODO: allow reconstituting of multiple dobject partitions into a new one.

  if(is.dobject(items[[1]])) {
    newobj <- combine(ddR.env$driver,items)
    newobj@nparts <- c(length(items), 1L)
    newobj@backend <- ddR.env$driver@backendName
    newobj@type <- "dlist"
    return(newobj)
  }

   dmapply(function(x) { x }, items)
}

#' Returns whether the input is a dlist
#' @param x Input object.
#' @return TRUE if x is a dlist, FALSE otherwise
#' @examples
#' \dontrun{
#' is.dlist(3) #FALSE
#' is.dlist(dlist(1,2,3,nparts=3)) #TRUE
#' }
#' @export
is.dlist <- function(x) {
  is(x,"DObject") && x@type == "dlist"
}

#' @rdname is.dlist
#' @export
is.DList <- is.dlist

#' @rdname as.dlist
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

#' @rdname is.dobject
#' @export 
is.DObject <- is.dobject

#' Creates a distributed array with the specified partitioning and contents.
#' @param nparts vector specifying number of partitions. If missing, 'psize' and 'dim' must be provided.
#' @param dim the dim attribute for the array to be created. A vector specifying number of rows and columns. 
#' @param psize size of each partition as a vector specifying number of rows and columns. 
#' This parameter is provided together with dim.
#' @param data initial value of all elements in array. Default is 0.
#' @param sparse If TRUE, the output darray will be of type sparse_darray. The default value is FALSE.
#' @return Returns a distributed array with the specified dimensions. Data may reside as partitions in remote nodes.
#' @seealso \code{\link{collect}} \code{\link{psize}} \code{\link{dmapply}}
#' @details 
#'  Array partitions are internally stored as dense
#'  matrices. Last set of partitions may have fewer 
#'  rows or columns if the array size is not an integer
#'  multiple of partition size. For example, the distributed array
#'  'darray(dim=c(5,5), psize=c(2,5))' has three partitions. The
#'  first two partitions have two rows each but the last partition has
#'  only one row. All three partitions have five columns.
#'
#'  Distributed arrays can also be defined by specifying just the
#'  number of partitions, but not their sizes. This flexibility is
#'  useful when the size of an array is not known apriori. For
#'  example, 'darray(nparts=c(5,1))' is a dense array with five
#'  partitions.  Each partition can contain any number of rows, though
#'  the number of columns should be same to conform to a well formed array.
#'
#' Distributed arrays can be fetched at the master using
#' \code{\link{collect}}. Number of partitions can be obtained by
#' \code{nparts}. Partitions are numbered from left to right, and
#' then top to bottom, i.e., row major order. Dimension of each
#' partition can be obtained using \code{psize}.
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
#' ## A 9 partition (each partition 3x3), 9x9 DArray with each element initialized to 5.
#' a <- darray(psize=c(3,3),dim=c(9,9),data=5) 
#' collect(a)
#' b <- darray(psize=c(3,3),dim=c(9,9)) # Same as 'a', but filled with 0s.
#' ## An empty darray with 6 partitions, 2 per column and 3 per row.
#' c <- darray(nparts=c(2,3)) 
#' }
#' @export
darray <- function(nparts = NULL, dim=NULL, psize = NULL, data = 0, sparse=FALSE) {
  if(sparse && data!=0) stop("Cannot initialize data in a sparse matrix. Initialization makes a matrix dense. Set sparse=FALSE or data=0.") 

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

  if(sparse) type = "sparse_darray"
  else type = "darray"

  if(all(dim==0)) {
    new(ddR.env$driver@DArrayClass,backend=ddR.env$driver@backendName,type = type, nparts = nparts, psize = psize, dim=dim)
  } else{

    if(class(psize) == "numeric") psize<-matrix(psize, nrow=1)
    sizes<-unlist(apply(psize,1,function(y)list(y)), recursive=FALSE)

    if(!sparse) {
      dmapply(function(d, v){ matrix(data=v,nrow=d[1], ncol=d[2]) }, sizes, MoreArgs=list(v=data), output.type="darray", combine="rbind", nparts=nparts)
    }
    else {
      dmapply(function(d, v) { requireNamespace("Matrix"); 
      			       new("dgCMatrix", i=as.integer({}), 
                               x=as.numeric({}),
                               p=as.integer(rep(v, d[2] + 1)),
                               Dim=as.integer(d))
                             }, sizes, MoreArgs=list(v=data),output.type="sparse_darray",combine="rbind", nparts=nparts)
    }
  }
}

#' @rdname darray
#' @export
DArray <- darray

#' Returns whether the input is a darray
#' @param x input object.
#' @return TRUE if x is a darray, FALSE otherwise.
#' @examples
#' \dontrun{
#' is.darray(3) # FALSE
#' is.darray(darray(psize=c(3,3),dim=c(9,9))) # TRUE
#' }
#' @export
is.darray <- function(x) {
  is(x,"DObject") && (x@type == "darray" || x@type == "sparse_darray")
}

#' @rdname is.darray
#' @export
is.DArray <- is.darray

#' Returns whether the input is a sparse_darray
#' @param x input object.
#' @return TRUE if x is a sparse_darray, FALSE otherwise.
#' @examples
#' \dontrun{
#' is.sparse_darray(3) # FALSE
#' is.sparse_darray(darray(psize=c(3,3),dim=c(9,9))) # FALSE
#' is.sparse_darray(darray(npartitions=3,sparse=TRUE)) # TRUE
#' }
#' @export
is.sparse_darray <- function(x) {
  is(x,"DObject") && x@type == "sparse_darray"
}

#' Creates a distributed data.frame with the specified partitioning and data.
#' @param nparts vector specifying number of partitions. If missing, 'psize' and 'dim' must be provided.
#' @param dim the dim attribute for the data.frame to be created. A vector specifying number of rows and columns. 
#' @param psize size of each partition as a vector specifying number of rows and columns. 
#' This parameter is provided together with dim.
#' @param data initial value of all elements in array. Default is 0.
#' @return Returns a distributed data.frame with the specified dimensions. Data may reside as partitions in remote nodes.
#' @seealso \code{\link{collect}} \code{\link{psize}} \code{\link{dmapply}}
#' @details 
#'  Data frame partitions are internally stored as data.frame
#'  objects. Last set of partitions may have fewer 
#'  rows or columns if the dframe dimension is not an integer
#'  multiple of partition size. For example, the distributed data.frame
#'  'dframe(dim=c(5,5), psize=c(2,5))' has three partitions. The
#'  first two partitions have two rows each but the last partition has
#'  only one row. All three partitions have five columns.
#'
#'  Distributed data.frames can also be defined by specifying just the
#'  number of partitions, but not their sizes. This flexibility is
#'  useful when the size of an dframe is not known apriori. For
#'  example, 'dframe(nparts=c(5,1))' is a dense array with five
#'  partitions.  Each partition can contain any number of rows, though
#'  the number of columns should be same to conform to a well formed array.
#'
#' Distributed data.frames can be fetched at the master using
#' \code{\link{collect}}. Number of partitions can be obtained by
#' \code{nparts}. Partitions are numbered from left to right, and
#' then top to bottom, i.e., row major order. Dimension of each
#' partition can be obtained using \code{psize}.
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
#' ## A 9 partition (each partition 3x3), 9x9 dframe with each element initialized to 5.
#' a <- dframe(psize=c(3,3),dim=c(9,9),data=5) 
#' collect(a)
#' b <- dframe(psize=c(3,3),dim=c(9,9)) # Same as 'a', but filled with 0s.
#' ## An empty dframe with 6 partitions, 2 per column and 3 per row.
#' c <- dframe(nparts=c(2,3)) 
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
    new(ddR.env$driver@DArrayClass,backend=ddR.env$driver@backendName,type = "dframe", nparts = nparts, psize = psize, dim=dim)
  } else{
    if(class(psize) == "numeric") psize<-matrix(psize, nrow=1)
    sizes<-unlist(apply(psize,1,function(y)list(y)), recursive=FALSE)
    dmapply(function(d, v){ data.frame(matrix(data=v,nrow=d[1], ncol=d[2])) }, sizes, MoreArgs=list(v=data), output.type="dframe", combine="rbind", nparts=nparts)
  }
}

#' @rdname dframe
#' @export
DFrame <- dframe

#' Returns whether the input is a dframe
#' @param x input object.
#' @return TRUE if x is a dframe, FALSE otherwise.
#' @examples
#' \dontrun{
#' is.dframe(3) # FALSE
#' is.dframe(dframe(psize=c(3,3),dim=c(9,9))) # TRUE
#' }
#' @export
is.dframe <- function(x) {
  is(x,"DObject") && x@type == "dframe"
}

#' @rdname is.dframe
#' @export
is.DFrame <- is.dframe

setMethod("show",signature("DObject"),function(object) {

  partsStr <- ""

  limit <- min(5,totalParts(object))

  for(i in seq(limit)) {
    if(i>1) partsStr <- paste0(partsStr,", ")
    dims <- paste0("",psize(object,i),collapse=", ")
    partsStr <- paste0(partsStr,"[",dims,"]")
  }

  if(limit < totalParts(object)){
    partsStr <- paste0(partsStr,", ...")
  }

  if(is.dlist(object)) dimStr <- "Length: "
  else dimStr <- "Dim: "

  printStr <- paste0("\nddR Distributed Object","\nType: ", object@type,"\n# of partitions: ", totalParts(object), "\nPartitions per dimension: ", paste(object@nparts,collapse="x"),"\nPartition sizes: ", partsStr, "\n", dimStr, paste(dim(object),collapse=","), "\nBackend: ", object@backend,"\n")

  cat(printStr) 
})

#' Repartitions a distributed object.
#' This function takes two inputs, a distributed object and a skeleton. These inputs must both be distributed objects of the same type and same dimension.
#' If 'dobj' and 'skeleton' have different internal partitioning, this function will return a new distributed object with the same internal data as in 'dobj' but with the partitioning scheme of 'skeleton'.
#' @param dobj distributed object whose data is to be preserved, but repartitioned.
#' @param skeleton distributed Object whose partitioning is to be emulated in the output.
#' @return A new distributed object with the data of 'dobj' and the partitioning of 'skeleton'. 
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
#' a <- dlist(1,2,3,4,nparts=2)
#' b <- dmapply(function(x) x, 11:14,nparts=4)
#' c <- repartition(a,b) # c will have 4 partitions of length 1 each, containing 1 to 4.
#' }
#' @export
repartition <- function(dobj, skeleton) {
  UseMethod("repartition")
}

#' @describeIn repartition The default implementation of repartition.
#' @export
repartition.DObject <- function(dobj,skeleton) {
 
  dims <- length(dim(skeleton))
  stopifnot(dim(dobj) == dim(skeleton))

  # Don't do anything if partitioning is already the same
  if(identical(psize(dobj),psize(skeleton))) return(dobj)
  
  cur_row <- 0
  cur_col <- 0
  index <- 0

  partitionIndices <- vector(mode = "list", length = totalParts(skeleton))

  if(dims > 1) {
    col_end <- dim(skeleton)[[2]]
  } else {
    col_end <- 0
  }

  while(index < totalParts(skeleton)) {
    index <- index + 1
    start_x <- cur_row + 1  
    end_x <- cur_row + psize(skeleton,index)[[1]]
    start_y <- cur_col + 1

    if(dims > 1) {
      end_y <- cur_col + psize(skeleton,index)[[2]]
      partitionIndices[[index]] <- list(start_x:end_x,start_y:end_y)
    } else {
      end_y <- 1
      partitionIndices[[index]] <- list(start_x:end_x)
    }
 
    cur_col <- end_y

    if(cur_col >= col_end) {
      cur_col <- 0
      cur_row <- cur_row + psize(skeleton,index)[[1]]
    }
  }

  partitionIdsAndOffsets <- mapply(getPartitionIdsAndOffsets,partitionIndices,
       MoreArgs=list(psizes=psize(dobj),nparts=nparts(dobj)),SIMPLIFY=FALSE)

  partitions <- lapply(partitionIdsAndOffsets,
                  function(x) parts(dobj,as.list(x$partitions)))

  rows <- lapply(partitionIdsAndOffsets,
                  function(x) as.list(x$row_offsets))
                   
  if(dims > 1) {
    cols <- lapply(partitionIdsAndOffsets,
                  function(x) as.list(x$col_offsets))
  }  

  repartitioner <- function(partitions,rows,cols,psize,type) {
    
    index <- 1
    dims <- length(psize)    

    if(dims > 2) stop("Cannot repartition object with more than 2 dimensions")

    if(type=="dlist") {
      output <- list()    
    } else if(type=="dframe") {
      output <- data.frame(matrix(0,psize[[1]],psize[[2]]))
    } else {
      output <- matrix(0,psize[[1]],psize[[2]])
    }

    currentPosition <- rep(1,dims)

    while(index <= length(partitions)) {
      if(dims > 1)
        endingPosition <- currentPosition + c((length(rows[[index]])-1),(length(cols[[index]])-1))
      else
        endingPosition <- currentPosition + length(rows[[index]]) - 1

      if(dims < 2) {
        output[currentPosition:endingPosition] <- partitions[[index]][rows[[index]]]
      } else {
        output[currentPosition[[1]]:endingPosition[[1]],currentPosition[[2]]:endingPosition[[2]]] <-
        partitions[[index]][rows[[index]],cols[[index]]]
      }

      index <- index + 1

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
  
  if(skeleton@type == "dlist") combine="c"
  else combine="rbind"

  if(dims > 1) {
    dmapply(FUN=repartitioner,partitions=partitions,rows=rows,cols=cols,psize=as.list(data.frame(t(psize(skeleton)))),
            MoreArgs=list(type=skeleton@type), output.type=skeleton@type, combine=combine,
            nparts=nparts(skeleton))
  } else {
    dmapply(FUN=repartitioner,partitions=partitions,rows=rows,psize=as.list(data.frame(t(psize(skeleton)))),
            MoreArgs=list(cols=NULL,type=skeleton@type), output.type=skeleton@type, combine=combine,
            nparts=nparts(skeleton))
  }
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

#' Convert input matrix into a distributed array.
#' @param input input matrix that will be converted to darray.
#' @param psize size of each partition as a vector specifying number of rows and columns.
#' @seealso \code{\link{darray}} \code{\link{psize}}
#' @return Returns a distributed array with dimensions equal to that of the
#' input matrix and partitioned according to argument 'psize'.  Data
#' may reside as partitions on remote nodes.
#' @details 
#' If partition size (psize) is missing then the input
#' matrix is row partitioned and striped across the
#' cluster, i.e., the returned distributed array has approximately as
#' many partitions as the number of R instances in the session.
#'
#' The last set of partitions may have fewer rows or columns if input
#' matrix size is not an integer multiple of partition size. If 'A'
#' is a 5x5 matrix, then 'as.darray(A, psize=c(2,5))' is a
#' distributed array with three partitions. The first two partitions
#' have two rows each but the last partition has only one row. All
#' three partitions have five columns.
#'
#' To create a distributed darray with just one partition, pass the
#' dimension of the input frame, i.e. 'as.darray(A, psize=dim(A))'
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
#' ##Create 4x4 matrix
#' mtx<-matrix(sample(0:1, 16, replace=T), nrow=4)
#' ##Create distributed array spread across the cluster
#' da<-as.darray(mtx)
#' psize(da)
#' ##Create distributed array with single partition
#' db<-as.darray(mtx, psize=dim(mtx))
#' psize(db)
#' ##Create distributed array with two partitions
#' dc<- as.darray(mtx, psize=c(2,4))
#' psize(dc)
#' ##Fetch first partition
#' collect(dc,1)
#' } 
#' @export
as.darray <- function(input, psize=NULL) {
   convertToDobject(input, psize, type="array") 
}

#' Convert input matrix or data.frame into a distributed data.frame.
#' @param input input matrix or data.frame that will be converted to dframe.
#' @param psize size of each partition as a vector specifying number of rows and columns.
#' @seealso \code{\link{dframe}} \code{\link{psize}}
#' @return Returns a distributed data.frame with dimensions equal to that of the
#' input matrix and partitioned according to argument 'psize'.  Data
#' may reside as partitions on remote nodes.
#' @details 
#' If partition size (psize) is missing then the input
#' matrix/data.frame is row partitioned and striped across the
#' cluster, i.e., the returned distributed frame has approximately as
#' many partitions as the number of R instances in the session.
#'
#' The last set of partitions may have fewer rows or columns if input
#' matrix size is not an integer multiple of partition size. If 'A'
#' is a 5x5 matrix, then 'as.dframe(A, psize=c(2,5))' is a
#' distributed frame with three partitions. The first two partitions
#' have two rows each but the last partition has only one row. All
#' three partitions have five columns.
#'
#' To create a distributed frame with just one partition, pass the
#' dimension of the input frame, i.e. 'as.dframe(A, psize=dim(A))'
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
#'     ##Create 4x4 matrix
#'     mtx<-matrix(sample(0:1, 16, replace=T), nrow=4)
#'     ##Create distributed frame spread across the cluster
#'     df<-as.dframe(mtx)
#'     psize(df)
#'     ##Create distributed frame with single partition
#'     db<-as.dframe(mtx, psize=dim(mtx))
#'     psize(db)
#'     ##Create distributed frame with two partitions
#'     dc<- as.dframe(mtx, psize=c(2,4))
#'     psize(dc)
#'     ##Fetch first partition
#'     collect(dc,1)
#'     #creating of dframe with data.frame
#'     dfa <- c(2,3,4)
#'     dfb <- c("aa","bb","cc")
#'     dfc <- c(TRUE,FALSE,TRUE)
#'     df <- data.frame(dfa,dfb,dfc)
#'     #creating dframe from data.frame with default block size
#'     ddf <- as.dframe(df)
#'     collect(ddf)
#'     #creating dframe from data.frame with 1x1 block size
#'     ddf <- as.dframe(df,psize=c(1,1))
#'     collect(ddf)
#' } 
#' @export
as.dframe <- function(input, psize=NULL) {
   convertToDobject(input, psize, type="data.frame") 
}

convertToDobject<-function(input, psize, type){

   mdim <- dim(input)
    if(is.null(psize)){
	#Create as many partitions as the no. of executors in the system
	psize<-mdim
        psize[1]<-ceiling(psize[1]/ddR.env$nexecutors)
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
       answer<-dmapply(FUN=function(x){x}, matrixList, output.type="darray", combine="rbind", nparts=numparts) 
    else
       answer<-dmapply(FUN=function(x){x}, matrixList, output.type="dframe", combine="rbind", nparts=numparts) 
    
    dnames<-dimnames(input)
    if(length(dnames) ==2 && length(dnames[[1]]) == dim(answer)[1] && length(dnames[[2]]) == dim(answer)[2])
        dimnames(answer) <- dimnames(input)
    return (answer)
}
