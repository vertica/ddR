###################################################################
# Copyright 2015 Hewlett-Packard Development Company, L.P.
# This program is free software; you can redistribute it 
# and/or modify it under the terms of the GNU General Public 
# License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU 
# General Public License for more details.

# You should have received a copy of the GNU General Public License 
# along with this program; if not, write to the Free Software 
# Foundation, Inc., 59 Temple Place, Suite 330, 
# Boston, MA 02111-1307 USA.
###################################################################

# Create distributedR DDSDriver

setClass("DistributedRDDS", contains="DDSDriver")

#' @export 
# Exported Driver
distributedR <- new("DistributedRDDS",DListClass = "DistributedRObj",DFrameClass = "DistributedRObj",DArrayClass = "DistributedRObj",backendName = "Distributed R")

#' @export
setMethod("init","DistributedRDDS",
  function(x,...)
    distributedR_start(...)
)

#' @export
setMethod("shutdown","DistributedRDDS",
  function(x)
    distributedR_shutdown()
)

#' @export
setMethod("do_dmapply",signature(driver="DistributedRDDS",func="function",MoreArgs="list",FUN.VALUE="ANY"), 
  function(driver,func,...,MoreArgs=list(),FUN.VALUE=NULL){
    margs <- list(...)
    ids <- list()
    dr_obj <- list()

    elementWise <- list()
    isElementWise <- FALSE

    dobjects <- list()

    # model by which all other dobjects need to be repartitioned to be compatible with
    modelObj <- NULL

    for(num in 1:length(margs)) {
      if(!is.list(margs[[num]]) && is(margs[[num]],"DObject")) {
        elementWise[[num]] <- TRUE
        isElementWise <- TRUE

        if(is.null(modelObj)) {
        # If the dobject is a DArray or a DList, the number of apply iterations equals the total number of elements (product of dimensions)
        if(margs[[num]]@type == "DListClass" || margs[[num]]@type=="DArrayClass") {
          lens <- vapply(data.frame(t(margs[[num]]@psize)), function(x) { prod(x) },FUN.VALUE=numeric(1))
        } # otherwise, it's the number of columns 
        else {
          lens <- vapply(data.frame(t(margs[[num]]@psize)), function(x) { x[[2]] },FUN.VALUE=numeric(1))
        }

        limits <- cumsum(lens)
        limits <- c(0,limits) + 1
        limits <- limits[1:(length(limits)-1)]

        # Currently, we set the first dobject to be the model object
        modelObj <- margs[[num]]

       } else {

          # TODO: this only works if dobjects are of the same type as the model
          # need to support repartitioning objects against model objects of different type
          if(!identical(margs[[num]]@psize,modelObj@psize) && 
                margs[[num]]@type == modelObj@type) {
            warning("repartitioning")
            margs[[num]] <- repartition(margs[[num]],modelObj)
          }
       }
      
        margs[[num]] <- parts(margs[[num]])

      } else {
        elementWise[[num]] <- FALSE
      }
        dobject_list <- vapply(margs[[num]], function(x) { is(x,"DObject")},FUN.VALUE=logical(1))
        dobjects[[num]] <- any(dobject_list)
        if(dobjects[[num]]) dr_obj[[length(dr_obj) + 1]] <- margs[[num]][dobject_list][[1]]@DRObj
     }

    np <- ifelse(isElementWise,length(lens),length(margs[[1]]))   

    # to store the output of the foreach
    # also perform necessary repartitioning here if elementWise
    if(is.data.frame(FUN.VALUE)) {
      .outObj <- distributedR::dframe(npartitions=np)
    } else if (is.matrix(FUN.VALUE)) {
      .outObj <- distributedR::darray(npartition=np)
    } else {
      .outObj <- distributedR::dlist(npartitions=np)
    }

    # to store the dimensions (or length if dlist) of each partition
    .dimsObj <- distributedR::dlist(npartitions=np)

    nDobjs = 0

    # Create wrapper executor function
    exec_func <- function(){}

    nms <- names(margs)

    # get the splits ids for each dobject in the list
    for(num in 1:length(margs)){
      if(nchar(nms[[num]]) == 0 || is.null(nms)){
        nm <- paste0(".tmpVarName",num)
      } else {
        nm <- nms[[num]]
      }

      # DR currently cannot handle NAs for second argument to splits(). Therefore, we use 
      # empty list as a workaround ... repartition function has this "hack" as well
      ids[[num]] <- lapply(margs[[num]],function(argument){
        if(is(argument,"DObject"))
         return(argument@splits)
        else if(dobjects[[num]]) {
          return(list())
        } else {
          return(argument)
        }
      })


      if(!elementWise[[num]] && isElementWise) {
        ids[[num]] <- mapply(function(x,y) { ids[[num]][y:(y+x)-1] }, lens, limits, SIMPLIFY=FALSE)
      } 

      if(dobjects[[num]]) {
        nDobjs <- nDobjs + 1
        tempName <- paste0(".tempVar",nDobjs)
        assign(tempName,dr_obj[[nDobjs]])
        tempStr <- paste0("substitute(splits(",tempName,",ids[[num]][[index]]),env=parent.frame())")
      }
      else{
        tempStr <- "substitute(ids[[num]][[index]],env=parent.frame())" 
      }
        tempStr <- gsub("num",as.character(num),tempStr)
        formals(exec_func)[[nm]] <- eval(parse(text=tempStr))
    }

    formals(exec_func)[[".funct"]] <- func

    argsStr <- ""

    for(z in 1:length(margs)){
      if(z > 1) argsStr <- paste0(argsStr,", ")
      argsStr <- paste0(argsStr,names(formals(exec_func))[[z]])
     if(nchar(nms[[z]]) != 0 && !is.null(nms)){
       argsStr <- paste0(argsStr,"=",names(formals(exec_func))[[z]])      
     }
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
    if(distributedR::is.dframe(.outObj)) {
        convert <- ".newDObj <- as.data.frame(.newDObj)"
        body(exec_func)[[3]] <- eval(parse(text=paste0("substitute(",convert,")")),envir=new.env())
    } else if(distributedR::is.darray(.outObj)) {
        convert <- ".newDObj <- as.matrix"
        body(exec_func)[[3]] <- eval(parse(text=paste0("substitute(",convert,")")),envir=new.env())
    }
  }

      body(exec_func)[[2]] <- eval(parse(text=paste0("substitute(",execLine,")")),envir=new.env())

      nLines <- length(body(exec_func))
      formals(exec_func)[[".newDObj"]] <- substitute(splits(.outObj,index),env=parent.frame())
      formals(exec_func)[[".dimObj"]] <- substitute(splits(.dimsObj,index),env=parent.frame())
      body(exec_func)[[nLines+1]] <- substitute(update(.newDObj))

    if(distributedR::is.dlist(.outObj)) {
       modLine <- ".dimObj <- list(length(.newDObj))"
    }
    else {
       modLine <- ".dimObj <- list(dim(.newDObj))" 
    }

      body(exec_func)[[nLines+2]] <- eval(parse(text=paste0("substitute(",modLine,")")),envir=new.env())
      body(exec_func)[[nLines+3]] <- substitute(update(.dimObj))

    foreach(index,1:np,exec_func,progress=FALSE) 

    dimensions <- getpartition(.dimsObj)

    psizes <- matrix(unlist(dimensions,recursive=FALSE), ncol=length(dimensions[[1]]),byrow=TRUE)

    if (distributedR::is.dlist(.outObj)) {
      dims <- Reduce("+",psizes)
    } else {
      dims <- as.integer(dim(.outObj))
    }

    new("DistributedRObj",DRObj = .outObj, splits = 1:npartitions(.outObj),
         psize = psizes, dim = dims)
}
)
