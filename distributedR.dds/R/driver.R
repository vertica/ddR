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
  function(driver,func,...,MoreArgs){
    # TODO: implement
    func
  }
)
