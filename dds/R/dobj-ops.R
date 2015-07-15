#' @export
setMethod("[", c("DList", "numeric", "missing","ANY"), 
  function(x, i, j, ..., drop=TRUE) {
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
setMethod("[[", c("DList", "numeric", "ANY"),
  function(x, i, j, ...) {
  stopifnot(length(i) < 2)

  unlist(x[i],recursive=FALSE)
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
