#' @export
setMethod("[", c("dobject", "integer,", "missing","ANY"), 
  function(x, i, j, ..., "missing", drop=TRUE)
{
   stopifnot(max(i) <= nparts(x) && min(i) > 0)
   indicesAndOffsets <- mapply(findPartitionByIndex,i,cumsum(x@psize[,1]))
   indicesAndOffsets <- matrix(unlist(indicesAndOffsets, ncol = 2, byrow = TRUE))

   # Use run-length encoding algorithm to determine repeated partition sequences
   sequences <- rle(indicesAndOffsets[,1])

   temp <- c(0,cumsum(sequences$lengths)) + 1
   temp <- temp[1:(length(temp)-1)]

   partitionIndices <- as.list(sequences$values)
   valueOffsets <- mapply(function(x,y) { 
      indicesAndOffsets[x:(x+y-1),2]
   },
  temp,sequences$lengths)

  values <- dmapply(function(x,y) { x[y] }, parts(x, partitionIndices), valueOffsets)

  collect(values)
}

# Internal helper function
findPartitionByIndex <- function(index,cumRowIndex) {
  partition <- findInterval(index,cumRowIndex)
  starting <- ifelse(partition == 0, 0, cumRowIndex[partition])
  c(partition+1,index-starting)
}
