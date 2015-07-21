# Create Distributed R Class Union
setClassUnion("DistRObj", c("dlist","darray","dframe"))

setClass("DistributedRObj",contains="DObject",
    slots=list(DRObj = "DistRObj", splits = "numeric"),
    prototype = prototype(nparts = 1L, psize = matrix(1,1), 
      dim = c(1L,1L)
))

#' @export
setMethod("get_parts",signature("DistributedRObj","missing"),
  function(x, ...){
  index = 1:length(x@splits)
 lapply(index,function(b) {
      new("DistributedRObj",DRObj = x@DRObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("get_parts",signature("DistributedRObj","integer"),
  function(x, index, ...){
   lapply(index,function(b) {
      new("DistributedRObj",DRObj = x@DRObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    } )
  }
)

#' @export
setMethod("do_collect",signature("DistributedRObj","integer"),
  function(x, parts) {
    if(identical(x@splits, parts) && length(parts) > 1) {
       getpartition(x@DRObj)
    } else if(length(parts) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      getpartition(x@DRObj,x@splits[[parts]])
   }
})
