# Create distributedR DDSDriver

setClass("distributedRDDS", contains="DDSDriver")

#' @export 
# Exported Driver
distributedR <- new("distributedRDDS")

#' @export
setMethod("create.dobj",c(x = "distributedRDDS"),
  function(x, type, nparts, psize){
    if(type == "DList")
       dobj <- distributedR::dlist(npartitions=nparts)
    else if(type == "Darray")
       dobj <- distributedR::darray(npartitions=nparts)
    else
       dobj <- distributedR::dframe(npartitions=nparts)
  
    new("distributedRBackend",DRObj = dobj, splits = 1:npartitions(dobj),psize=psize)
  }
)

#' @export
setMethod("init","distributedRDDS",
  function(x,...)
    distributedR_start(...)
)

setMethod("combine",signature(driver="distributedRDDS",items="list"),
  function(driver,items){
    split_indices <- lapply(items,function(x) {
      x@backend@splits
    })
    dims <- lapply(items,function(x) {
      x@backend@dim
    })

    psizes <- lapply(items,function(x) {
      x@backend@psize
    })

    dims <- Reduce("+",dims) 
    psizes <- Reduce("rbind",psizes)
    rownames(psizes) <- NULL

    new("distributedRBackend",DRObj=items[[1]]@backend@DRObj,splits = unlist(split_indices), dim = dims, psize = psizes)
  }
)

#' @export
setMethod("do_dmapply",signature(driver="distributedRDDS",func="function",MoreArgs="list"), 
  function(driver,func,...,MoreArgs=list()){
    margs <- list(...)
    # to store the output of the foreach
    .outObj <- distributedR::dlist(npartitions=length(margs[[1]]))
    # to store the dimensions (or length if dlist) of each partition
    .dimsObj <- distributedR::dlist(npartitions=length(margs[[1]]))
    ids <- list()

    dobjects <- lapply(margs,function(x) class(x[[1]])[[1]] == "DList")

    nDobjs = 0

    # Create wrapper executor function
    exec_func <- function(){}

    formals(exec_func) <- formals(func)

    # get the splits ids for each dobject in the list
    for(num in 1:length(margs)){
      ids[[num]] <- lapply(margs[[num]],function(argument){
        if(class(argument)[[1]] == "DList")
         return(argument@backend@splits)
        else return(argument)
      })

      if(dobjects[[num]]) {
        nDobjs <- nDobjs + 1
        tempName <- paste0(".tempVar",nDobjs)
        assign(tempName,margs[[num]][[1]]@backend@DRObj)
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

    # Take care of MoreArgs
    for(other in names(MoreArgs)){
      formals(exec_func)[[other]] <- MoreArgs[[other]]
      argsStr <- paste0(argsStr,", ",other,"=",other)
    }

      execLine <- paste0(".newDObj <- .funct(",argsStr,")")

      body(exec_func)[[2]] <- eval(parse(text=paste0("substitute(",execLine,")")))

      nLines <- length(body(exec_func))
      formals(exec_func)[[".newDObj"]] <- substitute(splits(.outObj,index),env=parent.frame())
      formals(exec_func)[[".dimObj"]] <- substitute(splits(.dimsObj,index),env=parent.frame())
      body(exec_func)[[nLines+1]] <- substitute(update(.newDObj))
     
      modLine <- ".dimObj <- list(ifelse(is.list(.newDObj),
        length(.newDObj), dim(.newDObj)))"
      body(exec_func)[[nLines+2]] <- eval(parse(
        text=paste0("substitute(",modLine,")")))
      body(exec_func)[[nLines+3]] <- substitute(update(.dimObj))

    foreach(index,1:length(margs[[1]]),exec_func,progress=FALSE) 

    dimensions <- getpartition(.dimsObj)

    psizes <- matrix(unlist(dimensions), ncol=length(dimensions[[1]]),byrow=TRUE)
    dims <- ifelse(distributedR::is.dlist(.outObj),Reduce("+",psizes),dim(.outObj))

    new("distributedRBackend",DRObj = .outObj, splits = 1:npartitions(.outObj),
         psize = psizes, dim = dims)
}
)
