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
