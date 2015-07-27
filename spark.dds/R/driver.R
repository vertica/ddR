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

sparkr.dds.env <- new.env(emptyenv())

setClass("SparkRDDS", contains="DDSDriver")

#' @export
SparkR <- new("SparkRDDS",DListClass = "SparkRObj",DFrameClass = "SparkRObj",DArrayClass="SparkRObj", backendName = "SparkR")

setMethod("initialize", "SparkRObj", function(.Object, ...) {
  .Object <- callNextMethod(.Object, ...)

  if(class(getJRDD(.Object@RDD)) != "jobj") {
    if(.Object@type == "DListClass")
       .Object@RDD <- parallelize(sparkr.dds.env$sc,list(),.Object@nparts)
    else if(.Object@type == "DArrayClass")
       .Object@RDD <- parallelize(sparkr.dds.env$sc,matrix(0,0,0),.Object@nparts)
    else
       .Object@RDD <- parallelize(sparkr.dds.env$sc,data.frame(matrix(0,0,0)),.Object@nparts)

   .Object@partitions <- 1:(.Object@nparts)

   }

   .Object
}

#' @export
setMethod("init","SparkRDDS",
  function(x,...){
    sparkr.dds.env$sc <- sparkR.init()
  }
)

setMethod("combine",signature(driver="SparkRDDS",items="list"),
  function(driver,items) {
    split_indices <- lapply(items,function(x) {
      x@partitions
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

    new("SparkRObj",RDD=items[[1]]@RDD,partitions = unlist(split_indices), dim = dims, psize = psizes)
  }
)

#' @export
setMethod("do_dmapply",signature(driver="SparkRDDS",func="function",MoreArgs="list"),
  function(driver,func,...,MoreArgs=list()){
    margs <- list(...)
    ids <- list()

    dobjects <- lapply(margs,function(x) is(x[[1]],"DObject"))

    nDobjs = 0

    # Create wrapper executor function
    exec_func <- function(){}

    formals(exec_func) <- formals(func)

    # get the splits ids for each dobject in the list
    for(num in 1:length(margs)){
ids[[num]] <- lapply(margs[[num]],function(argument){
        if(is(argument,"DObject"))
         return(argument@partitions)
        else return(argument)
      })

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
# Take care of MoreArgs
    for(other in names(MoreArgs)){
      formals(exec_func)[[other]] <- MoreArgs[[other]]
      argsStr <- paste0(argsStr,", ",other,"=",other)
    }

      execLine <- paste0(".newDObj <- .funct(",argsStr,")")

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

    foreach(index,1:length(margs[[1]]),exec_func,progress=FALSE)

    dimensions <- getpartition(.dimsObj)
                                                    
  psizes <- matrix(unlist(dimensions), ncol=length(dimensions[[1]]),byrow=TRUE)
    dims <- ifelse(distributedR::is.dlist(.outObj),Reduce("+",psizes),dim(.outObj))

    new("DistributedRObj",DRObj = .outObj, splits = 1:npartitions(.outObj),
         psize = psizes, dim = dims)
}
)
            
