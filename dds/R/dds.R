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
setGeneric("create.dobj", function(x,type){
  standardGeneric("create.dobj")
})
