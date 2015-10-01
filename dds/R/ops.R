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

  lens <- sapply(data.frame(t(psize(x))), function(x) { prod(x) })

  limits <- cumsum(lens)
  limits <- c(0,limits) + 1
  limits <- limits[seq(length(limits)-1)]
   
   
  namesList <- mapply(function(x,y) {
      value[x:(x+y-1)]
   },
  limits,lens,SIMPLIFY=FALSE)

  dmapply(function(x,y) { names(x) <- y; x }, parts(x), namesList,
          combine="flatten", nparts=totalParts(x))
})

#' @export
unlist.DObject <- function(x, recursive, use.names) {
  if(is.dlist(x)) unlist(collect(x),recursive,use.names)
}

#' @export
unique.DObject <- function(x, ...) {
  unique.per.partition <- dlapply(parts(x),function(x) { unique(x) })
  result <- collect(unique.per.partition)
  unique(unlist(result,recursive=FALSE))
}

#' @export
setMethod("[", c("DObject", "numeric", "missing","ANY"), 
  function(x, i, j, ..., drop=TRUE) {
  if(is.dlist(x)){
    stopifnot(max(i) <= length(x) && min(i) > 0)
    indicesAndOffsets <- mapply(findPartitionByIndex,i,MoreArgs=list(cumRowIndex=cumsum(psize(x)[,1])))

    # Use run-length encoding algorithm to determine repeated partition sequences
    sequences <- rle(indicesAndOffsets[1,])

    temp <- c(0,cumsum(sequences$lengths)) + 1
    temp <- temp[seq((length(temp)-1))]

    partitionIndices <- as.list(sequences$values)
    valueOffsets <- mapply(function(x,y) { 
      indicesAndOffsets[2,x:(x+y-1)]
    },
   temp,sequences$lengths,SIMPLIFY=FALSE)

   values <- dmapply(function(x,y) { x[y] }, parts(x, partitionIndices), valueOffsets)
}
   unlist(collect(values),recursive=FALSE)
})

#' @export
setMethod("$", c("DObject") ,
  function(x, name) {
    matching <- dlapply(x, function(x,y) { 
      match <- x[y]
      if(is.null(match[[1]])) {
        match <- list()
      }else{
        names(match) <- y
      }
      match

}, y = name)

collect(matching)[[name]]

})

#' @export
setMethod("[[", c("DObject", "numeric", "ANY"),
  function(x, i, j, ...) {
  if(is.dlist(x)) {
  stopifnot(length(i) < 2)

  unlist(unname(x[i]),recursive=FALSE)
 }
})

# Internal helper function
findPartitionByIndex <- function(index,cumRowIndex) {
  partition <- findInterval(index,cumRowIndex)
  if(partition !=0 && index == cumRowIndex[partition]) {
    partition = partition-1
  }
  starting <- ifelse(partition == 0, 0, cumRowIndex[partition])
  c(partition+1,index-starting)
}

#' @export
rev.DObject <- function(x) {
  rev(collect(x))
}

#' @export
setMethod("colSums", signature(x="DObject"),
  function(x, na.rm = FALSE, dims = 1L) {
    if(is.dlist(x)) stop("colSums is only supported for DArrays and DFrames")  

    columnSumsPerPartition <- 
      collect(dmapply(function(part,na.rm) colSums(part,na.rm=na.rm), parts(x),
              MoreArgs=list(na.rm=na.rm),nparts=totalParts(x)))

    colPartitionResults <- lapply(1:nparts(x)[[2]], function(col) {
                             partitionIds <- seq(col,col+(nparts(x)[[1]]-1)*nparts(x)[[2]],
                                           by=nparts(x)[[2]])
                             Reduce("+",
                                  columnSumsPerPartition[partitionIds])
                           })

  unlist(colPartitionResults)
})

#' @export
setMethod("colMeans", signature(x="DObject"),
  function(x, na.rm = FALSE, dims = 1L) {
    if(is.dlist(x)) stop("colMeans is only supported for DArrays and DFrames")  

    columnSums <- colSums(x,na.rm=na.rm)
    columnSums / nrow(x)
})

#' @export
setMethod("rowSums", signature(x="DObject"),
  function(x, na.rm = FALSE, dims = 1L) {
    if(is.dlist(x)) stop("rowSums is only supported for DArrays and DFrames")  

    rowSumsPerPartition <- 
      collect(dmapply(function(part,na.rm) rowSums(part,na.rm=na.rm), parts(x),
              MoreArgs=list(na.rm=na.rm),nparts=totalParts(x)))

    rowPartitionResults <- lapply(seq(1,1+(nparts(x)[[1]]-1)*nparts(x)[[2]],
                                      by=nparts(x)[[2]]), function(rowStart) {
                             partitionIds <- rowStart:(rowStart+nparts(x)[[2]]-1)
                             Reduce("+",
                                  rowSumsPerPartition[partitionIds])
                           })

  unlist(rowPartitionResults)
})

#' @export
setMethod("rowMeans", signature(x="DObject"),
  function(x, na.rm = FALSE, dims = 1L,...) {
    if(is.dlist(x)) stop("rowMeans is only supported for DArrays and DFrames")  

    rowsSums <- rowSums(x,na.rm=na.rm)
    rowsSums / ncol(x)
})

#' @export
setMethod("max", "DObject",
  function(x,...,na.rm=FALSE) {
    types <- vapply(list(x,...),function(y) is.darray(y) || is.dframe(y),
               FUN.VALUE=logical(1))

    if(any(!types)) stop("max is only supported for DArrays and DFrames")    

    # Get maxima for each DObject in the list(x,...)
    maxima <- vapply(list(x,...), function(y) {
       # Get local maxima of every partition
       localMax <- dmapply(function(part,na.rm) max(part,na.rm=na.rm), parts(y),
            MoreArgs=list(na.rm=na.rm))
    
       # Return maximum of all partitions together
       max(unlist(collect(localMax)))

    }, FUN.VALUE=numeric(1))    

    # Return maximum of maxima
    max(maxima)
})

#' @export
setMethod("min", "DObject",
  function(x,...,na.rm=FALSE) {
    types <- vapply(list(x,...),function(y) is.darray(y) || is.dframe(y),
               FUN.VALUE=logical(1))

    if(any(!types)) stop("min is only supported for DArrays and DFrames")    

    # Get minima for each DObject in the list(x,...)
    minima <- vapply(list(x,...), function(y) {
       # Get local minima of every partition
       localMin <- dmapply(function(part,na.rm) min(part,na.rm=na.rm), parts(y),
            MoreArgs=list(na.rm=na.rm))
    
       # Return minimum of all partitions together
       min(unlist(collect(localMin)))

    }, FUN.VALUE=numeric(1))    

    # Return minimum of minima
    min(minima)
})

#' @export
setReplaceMethod("dimnames", signature(x = "DObject", value = "list"), definition = function(x,value) {
  stopifnot(length(value) == length(dim(x)))
    if(is.dlist(x)) stop("Cannot use dimnames on a DList. Use names() instead.")

  if(length(value[[1]]) != dim(x)[[1]] || length(value[[2]]) != dim(x)[[2]]) stop("Length of an assigned name vector did not match the dimension of the DObject")

  rowBoundaries <- cumsum(psize(x,seq(1,totalParts(x),by=nparts(x)[[2]]))[,1])
  rowBoundaries <- c(0,rowBoundaries) + 1
  rowBoundaries <- rowBoundaries[seq(length(rowBoundaries) -1)]

  colBoundaries <- cumsum(psize(x,seq(1,nparts(x)[[2]]))[,2])
  colBoundaries <- c(0,colBoundaries) + 1
  colBoundaries <- colBoundaries[seq(length(colBoundaries) -1)]

  # List of vectors for the rownames per sub-matrix (partition)
  rowNamesPartition <- lapply(1:totalParts(x), function(part) {
                         partitionRow <- floor((part-1)/(nparts(x)[[2]])) + 1
                        
                         start <- rowBoundaries[partitionRow]
                         end <- start + psize(x,part)[1] - 1

                         value[[1]][start:end]
                      })

  # List of vectors for the colnames per sub-matrix (partition)
  colNamesPartition <- lapply(1:totalParts(x), function(part) {
                         partitionCol <- part %% (nparts(x)[[2]])
                         if(partitionCol == 0) partitionCol <- nparts(x)[[2]]
                        
                         start <- colBoundaries[partitionCol]
                         end <- start + psize(x,part)[2] - 1

                         value[[2]][start:end]
                      })

  dmapply(function(x,y,z) { dimnames(x) <- list(y,z); x }, parts(x), rowNamesPartition, colNamesPartition, output.type=x@type, combine="row",nparts=nparts(x))
})

#' @export
setMethod("colnames", "DObject",
  function(x) {
    if(is.dlist(x)) stop("Cannot use colnames on a DList. Use names() instead.")
    getColNames <- parts(x,seq(1,nparts(x)[[2]]))

    colNames <- dmapply(function(x) colnames(x), getColNames)
    
    unlist(collect(colNames))
})

#' @export
setMethod("rownames", "DObject",
  function(x) {
    if(is.dlist(x)) stop("Cannot use rownames on a DList. Use names() instead.")
    getRowNames <- parts(x,seq(1,totalParts(x),by=nparts(x)[[2]]))

    rowNames <- dmapply(function(x) rownames(x), getRowNames)
   
    unlist(collect(rowNames))
})

#' @export
setMethod("dimnames", "DObject",
  function(x) {
    if(is.dlist(x)) stop("Cannot use dimnames on a DList. Use names() instead.")
    list(rownames(x),colnames(x))
})

#' @export
setMethod("sum", "DObject",
  function(x,...,na.rm=FALSE) {
    types <- vapply(list(x,...),function(y) is.darray(y) || is.dframe(y),
               FUN.VALUE=logical(1))

    if(any(!types)) stop("sum is only supported for DArrays and DFrames")

    # Get sums of every dobject in the list(x,...) 
    sums <- vapply(list(x,...), function(y) {
                       sum(rowSums(y,na.rm=na.rm))
                   }, FUN.VALUE=numeric(1))

    # Return sum of sums
    sum(sums)
})

#' @export
setMethod("mean", "DObject",
  function(x,trim=0,na.rm=FALSE,...) {

    if(!is.darray(x) && !is.dframe(x)) stop("mean is only supported for DArrays and DFrames")
    if(trim !=0) stop("non-zero trim is currently not supported")

    mean(rowMeans(x,na.rm=na.rm))
})

#' @export
setGeneric("rbind",
    function(..., deparse.level=1) standardGeneric("rbind"),
    signature = "...")

#' @export
setGeneric("cbind",
    function(..., deparse.level=1) standardGeneric("cbind"),
    signature = "...")

#' @export
setMethod("rbind", "DObject",
  function(...,deparse.level = 1) {

   types <- sapply(list(...),function(y) y@type)

   if(any((types!="darray") & (types != "dframe"))) stop("rbind is only supported for DArrays and DFrames")

   ncols <- vapply(list(...), function(obj) ncol(obj), FUN.VALUE=numeric(1))
   if(min(ncols) != max(ncols)) stop("All inputs should have the same number of columns")

   x <- list(...)[[1]]

   # The output is a DArray if only DArrays are present. Otherwise,
   # it is a DFrame.
   if(any(types=="dframe")) out.type = "dframe"
   else out.type = "darray" 

   # Ensure that colwise partitions have the same dimensions
   # If they don't, repartition the incompatible object to be the 
   # same colwise partitioning as the first object (x)

   colWidths <- psize(x,seq(nparts(x)[[2]]))[,2]

   dobjs <- lapply(list(...), function(obj) {
              if(identical(psize(obj,seq(nparts(obj)[[2]]))[,2],colWidths))
                return(obj)
              else {
                if(obj@type == "dframe") 
                  skeleton <- dframe(nparts=c(nparts(obj)[[1]],nparts(x)[[2]]))
                else 
                  skeleton <- darray(nparts=c(nparts(obj)[[1]],nparts(x)[[2]]))

                  row_psize <- psize(obj,seq(1,totalParts(obj),by=nparts(obj)[[2]]))[,1]
                  col_psize <- psize(x,seq(nparts(x)[[2]]))[,2]
                  
                  skeleton@psize <- t(vapply(seq(totalParts(skeleton)),
                    function(i) { 
                      curRow <- floor((i-1)/nparts(x)[[2]]) + 1
                      curCol <- ((i-1) %% nparts(x)[[2]]) + 1 
                      c(row_psize[[curRow]],col_psize[[curCol]])                          
                    }, FUN.VALUE=numeric(2)))

                  skeleton@dim <- dim(obj)
                  repartition(obj,skeleton)
              }
            })

   totRowParts <- sum(vapply(dobjs,function(obj) nparts(obj)[[1]],FUN.VALUE=numeric(1)))
   nPartsResult <- c(totRowParts,nparts(x)[[2]])

   totPartsPerObj <- vapply(dobjs,function(obj) totalParts(obj), FUN.VALUE=numeric(1))
   totParts <- sum(totPartsPerObj)
   totPartsPerObj <- c(0,cumsum(totPartsPerObj))
   
   # Splitting into multiple separate arguments since currently DistR does not support parts() lists of mixed DObjects...fill blanks with NAs
   dmapplyArgs <- lapply(seq(length(dobjs)),function(ind) {
                     start <- totPartsPerObj[[ind]] + 1
                     end <- totPartsPerObj[[ind+1]]
                     out <- as.list(rep(NA,start-1))
                     out <- c(out,parts(dobjs[[ind]]))
                     c(out,as.list(rep(NA,totParts-end)))
                  })

   fillFunction <- function(...) {
     partitions <- list(...)
     for(i in seq(length(partitions))) {
       if(!is.na(partitions[[i]])) return(partitions[[i]])
     }
   }

   dmapplyArgs <- c(FUN=fillFunction,dmapplyArgs,output.type=list(out.type),combine=list("row"),nparts=list(nPartsResult))

   do.call(dmapply,dmapplyArgs)
})

#' @export
setMethod("cbind", "DObject",
  function(...,deparse.level = 1) {

   types <- sapply(list(...),function(y) y@type)

   if(any((types!="darray") & (types != "dframe"))) stop("cbind is only supported for DArrays and DFrames")
   nrows <- vapply(list(...), function(obj) nrow(obj), FUN.VALUE=numeric(1))
   if(min(nrows) != max(nrows)) stop("All inputs should have the same number of rows")

   x <- list(...)[[1]]

   # The output is a DArray if only DArrays are present. Otherwise,
   # it is a DFrame.
   if(any(types=="dframe")) out.type = "dframe"
   else out.type = "darray" 

   # Ensure that colwise partitions have the same dimensions
   # If they don't, repartition the incompatible object to be the 
   # same colwise partitioning as the first object (x)

   rowWidths <- psize(x,seq(1,totalParts(x),by=nparts(x)[[2]]))[,1]

   dobjs <- lapply(list(...), function(obj) {
              if(identical(psize(obj,seq(1,totalParts(obj),by=nparts(obj)[[2]]))[,1],rowWidths))
                return(obj)
              else {
                if(obj@type == "dframe") 
                  skeleton <- dframe(nparts=c(nparts(x)[[1]],nparts(obj)[[2]]))
                else 
                  skeleton <- darray(nparts=c(nparts(x)[[1]],nparts(obj)[[2]]))

                  col_psize <- psize(obj,seq(nparts(obj)[[2]]))[,2]
                  row_psize <- psize(x,seq(1,totalParts(x),by=nparts(x)[[2]]))[,1]

                  skeleton@psize <- t(vapply(seq(totalParts(skeleton)),
                    function(i) { 
                      curRow <- floor((i-1)/nparts(obj)[[2]]) + 1 
                      curCol <- ((i-1) %% nparts(obj)[[2]]) + 1
                      c(row_psize[[curRow]],col_psize[[curCol]])                          
                    }, FUN.VALUE=numeric(2)))

                  skeleton@dim <- dim(obj)
                  repartition(obj,skeleton)
              }
            })

   colParts <- vapply(dobjs,function(obj) nparts(obj)[[2]],FUN.VALUE=numeric(1))
   totColParts <- sum(colParts)
   nPartsResult <- c(nparts(x)[[1]],totColParts)

   colParts <- c(0,cumsum(colParts))
   
   # Splitting into multiple separate arguments since currently DistR does not support parts() lists of mixed DObjects...fill blanks with NAs
   dmapplyArgs <- lapply(seq(length(dobjs)),function(ind) {
                    out <- list()
                    for(a in seq(nparts(x)[[1]])) {
                       start <- colParts[[ind]] + 1
                       end <- colParts[[ind+1]]
                       out <- c(out,as.list(rep(NA,start-1)))
                       
                       start_part <- (a-1)*nparts(dobjs[[ind]])[[2]]+1
                       end_part <- start_part + nparts(dobjs[[ind]])[[2]] - 1

                       out <- c(out,parts(dobjs[[ind]],start_part:end_part))
                       out <- c(out,as.list(rep(NA,totColParts-end)))
                    }
                    out
                  })

   fillFunction <- function(...) {
     partitions <- list(...)
     for(i in seq(length(partitions))) {
       if(!is.na(partitions[[i]])) return(partitions[[i]])
     }
   }

   dmapplyArgs <- c(FUN=fillFunction,dmapplyArgs,output.type=list(out.type),combine=list("row"),nparts=list(nPartsResult))

   do.call(dmapply,dmapplyArgs)
})
