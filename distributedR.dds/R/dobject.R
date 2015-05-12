# Create Distributed R Class Union
setClassUnion("distributedRObj", c("dlist","darray","dframe"))

setClass("distributedRBackend",contains="Backend",
  slots=list(DRObj = "distributedRObj", splits = "numeric")
)

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
  function(x, index, ...) {
    if(are_equal(1:npartitions(x@DRObj),index)) {
       getpartition(x)
    } else if(length(index) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      getpartition(x,index)
   }
})
