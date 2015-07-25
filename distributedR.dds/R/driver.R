# Create distributedR DDSDriver

setClass("DistributedRDDS", contains="DDSDriver")

#' @export 
# Exported Driver
distributedR <- new("DistributedRDDS",DListClass = "DistributedRObj",DFrameClass = "DistributedRObj",DArrayClass = "DistributedRObj",backendName = "Distributed R")

setMethod("initialize", "DistributedRObj", function(.Object, ...) {
   .Object <- callNextMethod(.Object, ...)

  if(is.null(.Object@DRObj@dobject_ptr)) {
   if(.Object@type == "DListClass")
       .Object@DRObj <- distributedR::dlist(npartitions=.Object@nparts)
    else if(.Object@type == "DArrayClass")
       .Object@DRObj <- distributedR::darray(npartitions=.Object@nparts)
    else
       .Object@DRObj  <- distributedR::dframe(npartitions=.Object@nparts)
  
   .Object@splits <- 1:npartitions(.Object@DRObj)

   }

   .Object
})

#' @export
setMethod("init","DistributedRDDS",
  function(x,...)
    distributedR_start(...)
)

setMethod("combine",signature(driver="DistributedRDDS",items="list"),
  function(driver,items){
    split_indices <- lapply(items,function(x) {
      x@splits
    })
    dims <- lapply(items,function(x) {
      x@dim
    })

    psizes <- lapply(items,function(x) {
      x@psize
    })

    dims <- Reduce("+",dims) 
    psizes <- Reduce("rbind",psizes)
    rownames(psizes) <- NULL

    new("DistributedRObj",DRObj=items[[1]]@DRObj,splits = unlist(split_indices), dim = dims, psize = psizes)
  }
)

#' @export
setMethod("do_dmapply",signature(driver="DistributedRDDS",func="function",MoreArgs="list"), 
  function(driver,func,...,MoreArgs=list()){
    margs <- list(...)
    ids <- list()
    elementWise <- list()
    isElementWise <- FALSE

    dobjects <- list()
    for(num in 1:length(margs)) {

      if(!is.list(margs[[num]]) && is(margs[[num]],"DObject")) {
        elementWise[[num]] <- TRUE
        isElementWise <- TRUE
   
        lens <- mapply(function(x) { prod(x) }, data.frame(t(margs[[num]]@psize)),SIMPLIFY=FALSE)
        limits <- cumsum(unlist(lens))
        limits <- c(0,limits) + 1
        limits <- limits[1:(length(limits)-1)]

        margs[[num]] <- parts(margs[[num]])
      } else {
        elementWise[[num]] <- FALSE
      }
      dobjects[[num]] <- is(margs[[num]][[1]],"DObject")
     }

    np <- ifelse(isElementWise,length(lens),length(margs[[1]]))   

    # to store the output of the foreach
    .outObj <- distributedR::dlist(npartitions=np)
    # to store the dimensions (or length if dlist) of each partition
    .dimsObj <- distributedR::dlist(npartitions=np)

    nDobjs = 0

    # Create wrapper executor function
    exec_func <- function(){}

    formals(exec_func) <- formals(func)

    # get the splits ids for each dobject in the list
    for(num in 1:length(margs)){
      ids[[num]] <- lapply(margs[[num]],function(argument){
        if(is(argument,"DObject"))
         return(argument@splits)
        else return(argument)
      })

      if(!elementWise[[num]] && isElementWise) {
        ids[[num]] <- mapply(function(x,y) {ids[[num]][y:(y+x)-1] }, lens, limits, SIMPLIFY=FALSE)
      } 

      if(dobjects[[num]]) {
        nDobjs <- nDobjs + 1
        tempName <- paste0(".tempVar",nDobjs)
        assign(tempName,margs[[num]][[1]]@DRObj)
        tempStr <- paste0("substitute(splits(",tempName,",ids[[num]][[index]]),env=parent.frame())")
   }
      else{
        tempStr <- "substitute(ids[[num]][[index]],env=parent.frame())" 
      }
        tempStr <- gsub("num",as.character(num),tempStr)
        formals(exec_func)[[num]] <- eval(parse(text=tempStr))
    }

    formals(exec_func)[[".funct"]] <- func

    argsStr <- ""

    for(z in 1:length(margs)){
      if(z > 1) argsStr <- paste0(argsStr,", ")
      argsStr <- paste0(argsStr,names(formals(func))[[z]])
    }

  if(!isElementWise) {
     # Take care of MoreArgs
    for(other in names(MoreArgs)) {
      formals(exec_func)[[other]] <- MoreArgs[[other]]
      argsStr <- paste0(argsStr,", ",other,"=",other)
    }
  
    execLine <- paste0(".newDObj <- .funct(",argsStr,")")
  
  } else {
    for(other in names(MoreArgs)) {
      formals(exec_func)[[other]] <- NULL
    }
  
    formals(exec_func)[["MoreArgs"]] <- MoreArgs
    execLine <- paste0(".newDObj <- mapply(.funct,",argsStr,",MoreArgs=MoreArgs,SIMPLIFY=FALSE)")
  }

      body(exec_func)[[2]] <- eval(parse(text=paste0("substitute(",execLine,")")),envir=new.env())

      nLines <- length(body(exec_func))
      formals(exec_func)[[".newDObj"]] <- substitute(splits(.outObj,index),env=parent.frame())
      formals(exec_func)[[".dimObj"]] <- substitute(splits(.dimsObj,index),env=parent.frame())
      body(exec_func)[[nLines+1]] <- substitute(update(.newDObj))
     
      modLine <- ".dimObj <- list(ifelse(is.list(.newDObj),
        length(.newDObj), dim(.newDObj)))"
      body(exec_func)[[nLines+2]] <- eval(parse(
        text=paste0("substitute(",modLine,")")),envir=new.env())
      body(exec_func)[[nLines+3]] <- substitute(update(.dimObj))

    print(exec_func)

    foreach(index,1:length(margs[[1]]),exec_func,progress=FALSE) 

    dimensions <- getpartition(.dimsObj)

    psizes <- matrix(unlist(dimensions), ncol=length(dimensions[[1]]),byrow=TRUE)
    dims <- ifelse(distributedR::is.dlist(.outObj),Reduce("+",psizes),dim(.outObj))

    new("DistributedRObj",DRObj = .outObj, splits = 1:npartitions(.outObj),
         psize = psizes, dim = dims)
}
)
