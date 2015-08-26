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
unique.DObject <- function(x, ...) {
  unique.per.partition <- dlapply(parts(x),function(x) { unique(x) })
  unique(collect(unique.per.partition))
}

#' @export
setMethod("[", c("DObject", "numeric", "missing","ANY"), 
  function(x, i, j, ..., drop=TRUE) {
  if(is.dlist(x)){
    stopifnot(max(i) <= length(x) && min(i) > 0)
    indicesAndOffsets <- mapply(findPartitionByIndex,i,MoreArgs=list(cumRowIndex=cumsum(x@psize[,1])))

    # Use run-length encoding algorithm to determine repeated partition sequences
    sequences <- rle(indicesAndOffsets[1,])

    temp <- c(0,cumsum(sequences$lengths)) + 1
    temp <- temp[1:(length(temp)-1)]

    partitionIndices <- as.list(sequences$values)
    valueOffsets <- mapply(function(x,y) { 
      indicesAndOffsets[2,x:(x+y-1)]
    },
   temp,sequences$lengths,SIMPLIFY=FALSE)

   values <- dmapply(function(x,y) { x[y] }, parts(x, partitionIndices), valueOffsets)
}
   collect(values)
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
