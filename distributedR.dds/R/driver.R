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
  function(x,...) {
    message("Backend switched to Distributed R. Starting it up...")
    distributedR_start(...)
  }
)

#' @export
setMethod("shutdown","DistributedRDDS",
  function(x) {
    message("Switching out of using Distributed R. Shutting it down...")
    distributedR_shutdown()
  }
)

#' @export
setMethod("do_dmapply",signature(driver="DistributedRDDS",func="function",MoreArgs="list",output.type="character",nparts="numeric",combine="character"), 
  function(driver,func,...,MoreArgs=list(),output.type="DListClass",nparts=NULL,combine="flatten") {
    # margs stores the dmapply args
    margs <- list(...)
    # ids stores the arguments to splits() and the values of the raw arguments in foreach
    ids <- list()

    pieceSize <- floor(length(margs[[1]])/prod(nparts)) + 1
    remainder <- length(margs[[1]]) %% prod(nparts)

    modelApplyIterations <- c(rep(pieceSize,remainder),rep(pieceSize-1,prod(nparts)-remainder))

    limits <- cumsum(modelApplyIterations)
    limits <- c(0,limits) + 1
    limits <- limits[seq(length(limits)-1)]

    # to store the output of the foreach
    if(output.type=="DFrameClass") {
      .outObj <- distributedR::dframe(npartitions=nparts)
    } else if (output.type=="DArrayClass") {
      .outObj <- distributedR::darray(npartitions=nparts)
    } else {
      .outObj <- distributedR::dlist(npartitions=nparts[[1]])
    }

    # We can't flatten (in a nice way) if it's a dframe, so throw an
    # error here
    if(distributedR::is.dframe(.outObj) && combine=="flatten") 
      stop("Cannot flatten a data frame")

    # to store the dimensions (or length if dlist) of each partition
    .dimsObj <- distributedR::dlist(npartitions=prod(nparts))

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
        # Boolean to store whether the current argument being processed is a pure
        # DObject (i.e., not used with parts() or a vanilla-R argument
        isDObj <- TRUE

        # Now we're checking to see whether this dobject has a compatible partitioning with the model
        applyIterations <- vapply(data.frame(t(arg@psize)),
          function(psize) {
            # If it's a DList partition, then the number of apply-able
            # iterations is equal to the psize
            # For DArrays, it's the prod of the dimensions
            # Both of these can be expressed using prod(psize)
            if(arg@type != "DFrameClass") prod(psize)
            # If it's a DFrame, then we get number of columns
            # in the partition, represented by psize[[2]]
            else psize[[2]]
          }, FUN.VALUE=numeric(1))     

        # Repartitioning needs to happen when applyIterations don't match, OR
        # when partitioning is happening rowwise
        if(!identical(applyIterations,modelApplyIterations)
            || (arg@psize[1,][[1]] != arg@dim[[1]] && arg@type != "DListClass")) {
          warning(paste0("A repartitioning of input variable '", deparse(substitute(arg)), "' has been triggered.
                    For better performance, please try to partition your inputs compatibly."))

          # Now using applyIterations, we need to build the skeleton object whose partitioning
          # the repartitioned object should match
          # TODO: DArrays need to be "shifted" into alignment??? Throw an error if not possible
      
          if(arg@type == "DListClass") { 
            new_psize <- matrix(modelApplyIterations, ncol = 1, byrow = TRUE)
            skeleton <- dlist(nparts=length(modelApplyIterations))
          }

          else if(arg@type == "DFrameClass") {
            new_psize <- vapply(modelApplyIterations, 
                                function(x) c(arg@dim[[1]],x),
                        FUN.VALUE=numeric(2))

            skeleton <- dframe(nparts=length(modelApplyIterations))
          }

          else if(arg@type == "DArrayClass") {
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
         

        arg <- repartition(arg,skeleton)
        }

        arg <- parts(arg)

      } else isDObj <- FALSE

      dobject_list <- vapply(arg, function(x) { is(x,"DObject")},FUN.VALUE=logical(1))
      containsDobject <- any(dobject_list)
         
      # If unnamed, give it a name. Otherwise, leave it be.
      if(nchar(nm) == 0 || is.null(nms)) 
        nm <- paste0(".tmpVarName",num)

      # DR currently cannot handle NAs for second argument to splits(). Therefore, we use 
      # empty list as a workaround ... repartition function has this "hack" as well
      if(containsDobject) {
        ids[[num]] <- lapply(arg,function(argument) {
          if(is(argument,"DObject")) argument@splits
          else NA          
        })
          nDobjs <- nDobjs + 1
          tempName <- paste0(".tempVar",nDobjs)
          assign(tempName,arg[dobject_list][[1]]@DRObj)
          tempStr <- paste0("substitute(splits(",tempName,",ids[[num]][[index]]),env=parent.frame())")

      } else {
          ids[[num]] <- arg
          tempStr <- "substitute(ids[[num]][[index]],env=parent.frame())" 
      }

      if(!isDObj) 
        ids[[num]] <- mapply(function(x,y) { ids[[num]][y:(y+x-1)] }, modelApplyIterations, limits, SIMPLIFY=FALSE)
      

      tempStr <- gsub("num",as.character(num),tempStr)
      formals(exec_func)[[nm]] <- eval(parse(text=tempStr))

      if(num > 1) argsStr <- paste0(argsStr,", ")
        argsStr <- paste0(argsStr,names(formals(exec_func))[[num]])
      if(nchar(nm) != 0 && !is.null(nms)) 
        argsStr <- paste0(argsStr,"=",names(formals(exec_func))[[num]])      

    }

    formals(exec_func)[[".funct"]] <- match.fun(func)

    for(other in names(MoreArgs))
      formals(exec_func)[[other]] <- NULL
  
    formals(exec_func)[["MoreArgs"]] <- MoreArgs
    formals(exec_func)[[".newDObj"]] <- substitute(splits(.outObj,index),env=parent.frame())
    formals(exec_func)[[".dimObj"]] <- substitute(splits(.dimsObj,index),env=parent.frame())

    execLine <- paste0(".newDObj <- mapply(.funct,",argsStr,",MoreArgs=MoreArgs,SIMPLIFY=FALSE)")

    body(exec_func)[[2]] <- eval(parse(text=paste0("substitute(",execLine,")")),envir=new.env())

    # If the output is not a dlist, then we have to worry about how to 
    # stitch together the internal components of a partition --
    # either rowwise, columnwise, or the default, which is to 
    # cbind the results after flattening them into 1d-vectors
    if(!distributedR::is.dlist(.outObj)) {
      if(combine=="row")
        stitchResults <- ".newDObj <- do.call(rbind,.newDObj)"
      else if(combine=="col")
        stitchResults <- ".newDObj <- do.call(cbind,.newDObj)"
      else 
        stitchResults <- ".newDObj <- simplify2array(.newDObj,higher=FALSE)"

      body(exec_func)[[3]] <- eval(parse(text=paste0("substitute(",stitchResults,")")),envir=new.env())

      if(distributedR::is.dframe(.outObj)) {
        convert <- ".newDObj <- as.data.frame(.newDObj)"
        body(exec_func)[[4]] <- eval(parse(text=paste0("substitute(",convert,")")),envir=new.env())
      } else if(distributedR::is.darray(.outObj)) {
        convert <- ".newDObj <- as.matrix(.newDObj)"
        body(exec_func)[[4]] <- eval(parse(text=paste0("substitute(",convert,")")),envir=new.env())
      }
    }

    nLines <- length(body(exec_func))
    body(exec_func)[[nLines+1]] <- substitute(update(.newDObj))
    if(distributedR::is.dlist(.outObj)) {
      modLine <- ".dimObj <- list(length(.newDObj))"
    }
    else {
      modLine <- ".dimObj <- list(dim(.newDObj))" 
    }

    body(exec_func)[[nLines+2]] <- eval(parse(text=paste0("substitute(",modLine,")")),envir=new.env())
    body(exec_func)[[nLines+3]] <- substitute(update(.dimObj))

    foreach(index,seq(prod(nparts)),exec_func,progress=FALSE) 

    dimensions <- getpartition(.dimsObj)

    psizes <- matrix(unlist(dimensions,recursive=FALSE), ncol=length(dimensions[[1]]),byrow=TRUE)

    if(distributedR::is.dlist(.outObj)) {
      dims <- Reduce("+",psizes)
    } else {
      dims <- as.integer(dim(.outObj))
    }

    new("DistributedRObj",DRObj = .outObj, splits = seq(npartitions(.outObj)),
         psize = psizes, dim = dims, nparts=nparts)
  }
)
