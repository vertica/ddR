setClass("SparkRObj", contains="DObject",slots=list(RDD = "RDD", partitions = "numeric"),prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))

#' @export
setMethod("get_parts",signature("SparkRObj","missing"),
  function(x, ...){
  index = 1:length(x@splits)
 lapply(index,function(b) {
      new("SparkRObj",RDD = x@RDD, partitions = x@partitions[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("get_parts",signature("SparkRObj","missing"),
  function(x, index, ...){
 lapply(index,function(b) {
      new("SparkRObj",RDD = x@RDD, partitions = x@partitions[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("do_collect",signature("SparkRObj","integer"),
  function(x, parts) {
    if(identical(x@partitions, parts) && length(parts) > 1) {
       collect(x@RDD)
    } else if(length(parts) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      collectPartition(x@RDD,x@partitions[[parts]])
   }
})
