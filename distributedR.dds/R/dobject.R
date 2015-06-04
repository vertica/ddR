# Create Distributed R Class Union
setClassUnion("distributedRObj", c("dlist","darray","dframe"))

setClass("distributedRBackend",contains="Backend",
    slots=list(DRObj = "distributedRObj", splits = "numeric"),
    prototype = prototype(nparts = 1L, psize = matrix(1,1), 
      dim = c(1L,1L)
))

#' @export
setMethod("get_parts",signature("distributedRBackend","missing"),
  function(x, ...){
   lapply(x@splits,function(b) {
      new("distributedRBackend",DRObj = x@DRObj, splits = b)
    } )
  }
)

#' @export
setMethod("get_parts",signature("distributedRBackend","integer"),
  function(x, index, ...){
   lapply(index,function(b) {
      new("distributedRBackend",DRObj = x@DRObj, splits = x@splits[b])
    } )
  }
)

#' @export
setMethod("do_collect",signature("distributedRBackend","integer"),
  function(x, parts) {
    if(are_equal(1:npartitions(x@DRObj),parts)) {
       getpartition(x@DRObj)
    } else if(length(parts) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      getpartition(x@DRObj,parts)
   }
})
