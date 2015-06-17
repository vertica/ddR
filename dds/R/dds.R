# Current driving backend package. Package global variable
dds.env <- new.env(emptyenv())

# Set Driver 
#' @export
useBackend <- function(driver, init=TRUE) {
  # TODO: add package checks, backend driver MUST be of type
  # "DDSDriver"
  dds.env$driver <- driver
  if(init) init(driver)
}

#' @export
setClass("DDSDriver")

#' @export
setClass("Backend",
  representation(nparts = "integer", psize = "matrix", dim = "integer"))

#' @export
setGeneric("init", function(x,...) {
  standardGeneric("init")
}) 

#' @export 
# dispatches on DDSDriver
setGeneric("create.dobj", function(x,type,nparts=NULL,psize=NULL) {
  standardGeneric("create.dobj")
})

#' @export
# dispatches on DDSDriver
setGeneric("do_dmapply", function(driver,func,...,MoreArgs=list()) {
  standardGeneric("do_dmapply")
})

#' @export
# dispatches on DDSDriver
setGeneric("combine", function(driver,items) {
  standardGeneric("combine")
})

#' @export
# dispatches on backend
setGeneric("get_parts", function(x,index,...) {
  standardGeneric("get_parts")
})

#' @export
dmapply <- function(FUN,...,MoreArgs=list(),simplify=FALSE) {
  stopifnot(is.function(FUN))
  stopifnot(length(args) > 0)

  dargs <- list(...)
  # Ensure that ... arguments are of equal length
  lens <- tryCatch({
    lens <- lapply(dargs,function(x){
     length(x)
    })
    stopifnot(max(unlist(lens)) == min(unlist(lens)))
     lens}, error = 
    function(e){
      stop("Arguments to dmapply function must be of equal length (have the 
        same number of elements)")
    })

  if(simplify){
    #TODO: logic to determine the appropriate output class
    type = "DList"
  }else{
    type = "DList"
  }

  newobj <- new(type, backend = create.dobj(dds.env$driver, type, nparts=lens[[1]],psize=matrix(1L,lens[[1]])), 
        nparts = lens[[1]])

  newobj@backend <- do_dmapply(dds.env$driver, func=FUN, ..., MoreArgs=MoreArgs)
  
  # TODO: Validate dimensions
  newobj@dim <- newobj@backend@dim
  newobj@psize <- newobj@backend@psize  

  newobj
}
