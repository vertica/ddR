# Current driving backend package. Package global variable
dds.env <- new.env(emptyenv())

# Set Driver 
#' @export
useBackend <- function(driver, init=TRUE) {

  if(!extends(class(driver)[[1]],"DDSDriver")) stop("Invalid driver object specified")

  if(!extends(driver@DListClass,"DObject")) stop("The driver DList class does not extend DDS::Dobject")
  if(!extends(driver@DFrameClass,"DObject")) stop("The driver DFrame class does not extend DDS::Dobject")
  if(!extends(driver@DArrayClass,"DObject")) stop("The driver DArray class does not extend DDS::Dobject")

  dds.env$driver <- driver
  if(init) init(driver)
}

#' @export
setClass("DDSDriver", representation(DListClass = "character", DFrameClass = "character", DArrayClass = "character", backendName = "character"))

#' @export
setGeneric("init", function(x,...) {
  standardGeneric("init")
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
dlapply <- function(dobj,FUN,...) {
   dmapply(FUN,dobj,MoreArgs=list(...))
}

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
    type = "DListClass"
  }else{
    type = "DListClass"
  }

  # newobj <- new(type, backend = create.dobj(dds.env$driver, type, nparts=lens[[1]],psize=matrix(1L,lens[[1]])), 
   #     nparts = lens[[1]])

  margs <- list(...)
  elementWise <- FALSE
  for(i in 1:length(margs)) {
    if(is(margs[[1]],"DObject")) { elementWise <- TRUE
      newNparts <- nparts(margs[[1]])
    }
  }

  newobj <- do_dmapply(dds.env$driver, func=FUN, ..., MoreArgs=MoreArgs)
  
  newobj@backend <- dds.env$driver@backendName
  newobj@type <- type
  newobj@nparts <- ifelse(elementWise,newNparts,lens[[1]])

  # Verify that the output object is of the correct type
  stopifnot(is(newobj,slot(dds.env$driver,type)))

  # TODO: Validate dimensions
#  newobj@dim <- newobj@backend@dim
#  newobj@psize <- newobj@backend@psize  

  newobj
}
