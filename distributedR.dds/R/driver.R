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

# Internal helper function to determine number of "apply"-able elements per partition
# Should be used with mapply on data.frame(t(dobj@psize)) and dobj@type 
getApplyIterations <- function(psize,type) {
  # If it's a DList partition, then the number of apply-able
  # iterations is equal to the psize
  # For DArrays, it's the prod of the dimensions
  # Both of these can be expressed using prod(psize)
  if(type != "DFrameClass") prod(psize)
  # If it's a DFrame, then we get number of columns
  # in the partition, represented by psize[[2]]
  else psize[[2]]
}

#' @export
setMethod("do_dmapply",signature(driver="DistributedRDDS",func="function",MoreArgs="list",FUN.VALUE="ANY",.model="DObject"), 
  function(driver,func,...,MoreArgs=list(),FUN.VALUE=NULL,.model=NULL) {
    # margs stores the dmapply args
    margs <- list(...)
    # ids stores the arguments to splits() and the values of the raw arguments in foreach
    ids <- list()

    # Determine if this dmapply involves any elementwise apply
    elementWiseApply <- any(vapply(margs, function(x) {
                              !is.list(x) && is(x,"DObject")
                            },FUN.VALUE=logical(1)))

    # Used to calculate breaks in lists
    if(elementWiseApply) {
      # compute apply iterations per partition
      modelApplyIterations <- mapply(getApplyIterations,
                   data.frame(t(.model@psize)),.model@type)

      limits <- cumsum(modelApplyIterations)
      limits <- c(0,limits) + 1
      limits <- limits[1:(length(limits)-1)]
    }

    np <- totalParts(.model)

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

    # Create a wrapper executor function
    exec_func <- function() {}

    # String for arguments to exec_func
    argsStr <- ""

    nms <- names(margs)

    for(num in seq(length(margs))) {
      nm <- nms[[num]]
      arg <- margs[[num]]

     
      if(!is.list(arg) && is(arg,"DObject")) {
        elementWise <- TRUE

        # Now we're checking to see whether this dobject has a compatible partitioning with the model
        applyIterations <- mapply(getApplyIterations,data.frame(t(arg@psize)),arg@type)     

        # Repartitioning needs to happen when applyIterations don't match, OR
        # when partitioning is happening rowwise
        if(!identical(applyIterations,modelApplyIterations)
            || (arg@psize[1,][[1]] != arg@dim[[1]] && arg@type != "DListClass")) {
          warning(paste0("A repartitioning of input variable '", deparse(substitute(arg)), "' has been triggered.
                    For better performance, please try to partition your inputs compatibly."))

          # Now using applyIterations, we need to build the skeleton object whose partitioning
          # the repartitioned object should match
          # TODO: DArrays need to be "shifted" into alignment??? Throw an error if not possible

      
          if(arg@type == .model@type) skeleton <- .model
          
          else {
            if(arg@type == "DFrameClass") {
              new_psize <- vapply(modelApplyIterations, 
                                  function(x) c(arg@dim[[1]],x),
                          FUN.VALUE=numeric(2))

              skeleton <- dframe(nparts=length(modelApplyIterations))
            }

           if(arg@type == "DArrayClass") {
              new_psize <- vapply(modelApplyIterations,
                                  function(x) {
                                    # if applyIterations is not a multiple of column
                                    # length, stop with error as we cannot guarantee that 
                                    # repartition is doable
                                    numCol = x/arg@dim[[1]]
                                    if(floor(numCol) != numCol)
                                      stop("Repartitioning matrix not possible.")
                                    
                                    c(arg@dim[[1]],numCol)
                                  }, FUN.VALUE=numeric(2))

             skeleton <- darray(nparts=length(modelApplyIterations))
           }

           new_psize <- t(as.matrix(new_psize))

           skeleton@dim <- arg@dim
           skeleton@psize <- new_psize
          }

          arg <- repartition(arg,skeleton)
        
        }

        arg <- parts(arg)

      } else elementWise <- FALSE

      dobject_list <- vapply(arg, function(x) { is(x,"DObject")},FUN.VALUE=logical(1))
      containsDobject <- any(dobject_list)
         
      # If unnamed, give it a name. Otherwise, leave it be.
      if(nchar(nm) == 0 || is.null(nms)) 
        nm <- paste0(".tmpVarName",num)

      # DR currently cannot handle NAs for second argument to splits(). Therefore, we use 
      # empty list as a workaround ... repartition function has this "hack" as well
      if(containsDobject) {
        ids[[num]] <- lapply(arg,function(argument) {
          if(is.na(argument)) list()
          else if(is(argument,"DObject")) argument@splits
          else stop(paste0("Argument ", deparse(substitute(arg)), " contains
                 a mix of dobjects and non-dobjects. Currently this is unsupported for 
                 dmapply in Distributed R"))
          })

          nDobjs <- nDobjs + 1
          tempName <- paste0(".tempVar",nDobjs)
          assign(tempName,arg[dobject_list][[1]]@DRObj)
          tempStr <- paste0("substitute(splits(",tempName,",ids[[num]][[index]]),env=parent.frame())")

      } else {
          ids[[num]] <- arg
          tempStr <- "substitute(ids[[num]][[index]],env=parent.frame())" 
      }

      if(!elementWise && elementWiseApply) 
        ids[[num]] <- mapply(function(x,y) { ids[[num]][y:(y+x)-1] }, lens, limits, SIMPLIFY=FALSE)
      

      tempStr <- gsub("num",as.character(num),tempStr)
      formals(exec_func)[[nm]] <- eval(parse(text=tempStr))

      if(num > 1) argsStr <- paste0(argsStr,", ")
        argsStr <- paste0(argsStr,names(formals(exec_func))[[num]])
      if(nchar(nm) != 0 && !is.null(nms)) 
        argsStr <- paste0(argsStr,"=",names(formals(exec_func))[[num]])      

    }

  formals(exec_func)[[".funct"]] <- match.fun(func)

  if(!elementWiseApply) {
     # Take care of MoreArgs
    for(other in names(MoreArgs)) {
      formals(exec_func)[[other]] <- MoreArgs[[other]]
      argsStr <- paste0(argsStr,", ",other,"=",other)
    }
  
    execLine <- paste0(".newDObj <- .funct(",argsStr,")")
  
  } else {
      for(other in names(MoreArgs))
        formals(exec_func)[[other]] <- NULL
  
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

  if(distributedR::is.dlist(.outObj)) {
    dims <- Reduce("+",psizes)
  } else {
    dims <- as.integer(dim(.outObj))
  }

  new("DistributedRObj",DRObj = .outObj, splits = 1:npartitions(.outObj),
       psize = psizes, dim = dims, nparts=c(nrow(psizes),1L))
}
)
