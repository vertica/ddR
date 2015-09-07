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

#Returns the total number of partitions (i.e., product of all dimensions of obj@nparts)
#' @export
totalParts <- function(dobj) {
  prod(dobj@nparts)
}

#Returns the vector with partitions in each dimension (currently 2D)
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

#' @export
dlist <- function(...,nparts = 1L, psize=matrix(1,1)) {
  nparts = c(as.integer(nparts), 1L) #The second dimension is always 1 for dlists
  psize = matrix(0L,nparts[1])
  initialize <- list(...)
  if(length(initialize) == 0) {
    new(dds.env$driver@DListClass,backend=dds.env$driver@backendName,type = "DListClass", nparts = nparts, psize = psize, dim = 0L)
  } else{
    dmapply(function(x){ x }, initialize)
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
    newobj@nparts <- c(length(items), 1L)
    newobj@backend <- dds.env$driver@backendName
    newobj@type <- "DListClass"
    return(newobj)
  }

   dmapply(function(x) { x }, items)
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
darray <- function(...,nparts = NULL, psize = NULL, dim = NULL) {
 
  if(!is.null(dim) || !is.null(psize)) {
    if(is.null(psize) || is.null(dim)) stop("Need to supply both psize and dim")
    if(!is.null(nparts)) stop("Cannot supply nparts as well as psize and dimensions")

    # Test for legality of dim and psize specifications
    stopifnot(length(psize) > 0)
    stopifnot(length(psize) == length(dim))

    #TODO (iR):Add more sanity checks on dim and psize
    nparts <-  c(ceiling(dim[1]/psize[1]), ceiling(dim[2]/psize[2]))

    # Create number of rows equal to number of parts
    psize <- t(matrix(psize))
    psize <- psize[rep(seq_len(nrow(psize)), prod(nparts)),] 

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

  initialize <- list(...)

  if(length(initialize)==0) {
    new(dds.env$driver@DArrayClass,backend=dds.env$driver@backendName,type = "DArrayClass", nparts = nparts, psize = psize, dim=dim)
  } else{
    dmapply(function(x){ matrix(x) }, initialize, output.type="DArrayClass",nparts=nparts)
  }
}

#' @export
DArray <- darray

#' @export
is.darray <- function(x) {
  is(x,"DObject") && x@type == "DArrayClass"
}

#' @export
is.DArray <- is.darray


#' @export
dframe <- function(...,nparts = NULL, psize = NULL, dim = NULL) {

  if(!is.null(dim) || !is.null(psize)) {
    if(is.null(psize) || is.null(dim)) stop("Need to supply both psize and dim")
    if(!is.null(nparts)) stop("Cannot supply nparts as well as psize and dimensions")

    # Test for legality of dim and psize specifications
    stopifnot(length(psize) > 0)
    stopifnot(length(psize) == length(dim))

    #TODO (iR):Add more sanity checks on dim and psize
    nparts <-  c(ceiling(dim[1]/psize[1]), ceiling(dim[2]/psize[2]))

    # Create number of rows equal to number of parts
    psize <- t(matrix(psize))
    psize <- psize[rep(seq_len(nrow(psize)), prod(nparts)),] 

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

  initialize <- list(...)

  if(length(initialize)==0) {
    new(dds.env$driver@DFrameClass,backend=dds.env$driver@backendName,type = "DFrameClass", nparts = nparts, psize = psize, dim=dim)
  } else{
    dmapply(function(x){ data.frame(x) }, initialize, output.type="DFrameClass",nparts=nparts,combine="col")
  }
}

#' @export
DFrame <- dframe

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


  dmapply(function(x,y) { names(x) <- y; x }, parts(x), namesList) 
})

#' @export
unlist.DObject <- function(x, recursive, use.names) {
  if(is.dlist(x)) unlist(collect(x),recursive,use.names)
}

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

  dmapplyArgs <- c(FUN=repartitioner,dmapplyArgs,psize=list(as.list(data.frame(t(skeleton@psize)))),MoreArgs=list(list(type=skeleton@type)),output.type=list(skeleton@type),combine=list("row"),.unlistEach=list(TRUE))

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

