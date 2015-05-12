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
  
    new("distributedRBackend",DRObj = dobj, splits = 1:npartitions(dobj))
  }
)

#' @export
setMethod("init","distributedRDDS",
  function(x,...)
    distributedR_start(...)
)

#' @export
setMethod("do_mapply",signature(driver="distributedRDDS",func="function",MoreArgs="list"), 
  function(driver,func,...,MoreArgs=list()){
    margs <- list(...)
    .outObj <- distributedR::dlist(npartitions=length(margs[[1]]))
    ids <- list()

    dobjects <- lapply(margs,function(x) class(x[[1]])[[1]] == "DList")

    nDobjs = 0
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
        formals(func)[[num]] <- eval(parse(text=tempStr))
    }

      nLines <- length(body(func))
      formals(func)[[".newDObj"]] <- substitute(splits(.outObj,index),env=parent.frame())
      modLine <- deparse(body(func)[[nLines]])
      modLine <- paste0(".newDObj <- ", modLine)
      body(func)[[nLines]] <- eval(parse(
        text=paste0("substitute(",modLine,")")))
      body(func)[[nLines+1]] <- substitute(update(.newDObj))

    # Take care of MoreArgs
    for(other in names(MoreArgs)){
      formals(func)[[other]] <- MoreArgs[[other]]
    }

    foreach(index,1:length(margs[[1]]),func) 

    new("distributedRBackend",DRObj = .outObj, splits = 1:npartitions(.outObj))
}
)
