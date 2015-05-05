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

setClass("DDSDriver")

setClass("Backend")

#' @export
setGeneric("init", function(x) {
  standardGeneric("init")
}) 

#' @export 
# dispatches on DDSDriver
setGeneric("create.dobj", function(x,type) {
  standardGeneric("create.dobj")
})

#' @export
# dispatches on DDSDriver
setGeneric("do_mapply", function(driver) {
  standardGeneric("do_mapply")
})

#' @export
mapply <- function(FUN,...,MoreArgs,simplify=FALSE) {
  assert_that(is.function(FUN))
  assert_that(length(args) > 0)

  dargs <- list(...)
  # Ensure that ... arguments are of equal length
  tryCatch({
    lens <- lapply(dargs,function(x) length(x))
    assert_that(max(lens) == min(lens))}, error = 
    function(e){
      stop("Arguments to mapply function must be of equal length (have the 
        same number of elements)")
    })

  if(simplify){
    #TODO: logic to determine the appropriate output class
    type = "DList"
  }else{
    type = "DList"
  }

  newobj <- new(type, backend = create.dobj(dds.env$driver, type), 
        nparts = as.integer(nparts), psize = as.integer(psize))

  newobj@backend <- do_mapply(dds.env$driver, func=FUN, ..., MoreArgs=MoreArgs)
  newobj
}
