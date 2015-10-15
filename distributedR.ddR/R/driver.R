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

#' @import methods ddR Rcpp RInside XML Executor
#' @importFrom distributedR foreach getpartition distributedR_start 
#' distributedR_shutdown distributedR_status partitionsize splits
#' npartitions dimnames.dobject dimnames<-.dobject
#' @importClassesFrom distributedR dobject 

dR.env <- new.env()

# Create distributedR ddRDriver
setClass("DistributedRddR", contains="ddRDriver")

#' @export 
# Exported Driver
distributedR <- new("DistributedRddR",DListClass = "DistributedRObj",DFrameClass = "DistributedRObj",DArrayClass = "DistributedRObj",backendName = "Distributed R")

#' @export
setMethod("init","DistributedRddR",
  function(x,warn=TRUE,...) {
    if(!is.logical(warn)) stop("`warn` must be either TRUE or FALSE.")
    message("Backend switched to Distributed R. Starting it up...")
    distributedR_start(...)
    dR.env$DRWarn <- warn
    return (sum(distributedR_status()$Inst))
  }
)

#' @export
setMethod("shutdown","DistributedRddR",
  function(x) {
    message("Switching out of using Distributed R. Shutting it down...")
    distributedR_shutdown()
  }
)

#' @export
setMethod("do_dmapply",
           signature(driver="DistributedRddR",func="function"), 
           function(driver,func,...,MoreArgs=list(),
                    output.type = 
                      c("dlist","dframe","darray","sparse_darray"),
                    nparts=NULL,
                    combine=c("default","c","rbind","cbind")) {

    # margs stores the dmapply args
    margs <- list(...)
    # ids stores the arguments to splits() and the values of the raw arguments in foreach
    ids <- list()
 
    # stores arguments that are expressed in a nested parts list
    nested_parts <- list()

    warned <- !dR.env$DRWarn
  
    output.type <- match.arg(output.type)
    combine <- match.arg(combine)

    pieceSize <- floor(length(margs[[1]])/prod(nparts)) + 1
    remainder <- length(margs[[1]]) %% prod(nparts)

    modelApplyIterations <- c(rep(pieceSize,remainder),rep(pieceSize-1,prod(nparts)-remainder))

    limits <- cumsum(modelApplyIterations)
    limits <- c(0,limits) + 1
    limits <- limits[seq(length(limits)-1)]

    # to store the output of the foreach
    if(output.type=="dframe") {
      .outObj <- "distributedR::dframe(npartitions=nparts)"
    } else if (output.type=="darray") {
      .outObj <- "distributedR::darray(npartitions=nparts)"
    } else if (output.type=="sparse_darray") {
      .outObj <- "distributedR::darray(npartitions=nparts,sparse=TRUE)"
    } else {
      .outObj <- "distributedR::dlist(npartitions=nparts[[1]])"
    }

    if(!dR.env$DRWarn) .outObj <- paste0("suppressWarnings(",.outObj,")")
    .outObj <- eval(parse(text=.outObj))

    nDobjs = 0

    # Create a wrapper executor function
    exec_func <- function() {}

    # String for arguments to exec_func
    argsStr <- ""

    nms <- names(margs)

    for(num in seq(length(margs))) {
      nm <- nms[[num]]
      arg <- margs[[num]]
      containsDobject <- FALSE     

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
            if(arg@type != "dframe") prod(psize)
            # If it's a DFrame, then we get number of columns
            # in the partition, represented by psize[[2]]
            else psize[[2]]
          }, FUN.VALUE=numeric(1))     

        # Repartitioning needs to happen when applyIterations don't match, OR
        # when partitioning is happening rowwise
        if(!identical(unname(applyIterations), modelApplyIterations)
            || (arg@psize[1,][[1]] != arg@dim[[1]] && arg@type != "dlist")) {
          if(!warned) {
            warning(paste("At least one repartitioning of an input variable has been triggered.", 
              "For better performance, partition your outputs and inputs in a compatible fashion."))
            warned <- TRUE
          }

          # Now using applyIterations, we need to build the skeleton object whose partitioning
          # the repartitioned object should match
          # TODO: DArrays need to be "shifted" into alignment??? Throw an error if not possible
      
          if(arg@type == "dlist") { 
            new_psize <- matrix(modelApplyIterations, ncol = 1, byrow = TRUE)
            skeleton <- dlist(nparts=length(modelApplyIterations))
          }

          else if(arg@type == "dframe") {
            new_psize <- vapply(modelApplyIterations, 
                                function(x) c(arg@dim[[1]],x),
                        FUN.VALUE=numeric(2))

            skeleton <- dframe(nparts=length(modelApplyIterations))
            new_psize <- t(as.matrix(new_psize))
          }

          else if(arg@type == "darray") {
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
           new_psize <- t(as.matrix(new_psize))
         }

         skeleton@dim <- arg@dim
         skeleton@psize <- new_psize
         
        arg <- repartition(arg,skeleton)
        }

        arg <- parts(arg)
        containsDobject <- TRUE
      } else {
        isDObj <- FALSE

        if(is.list(arg)) {
          is_dobject <- rapply(arg, function(x) is(x,"DistributedRObj"),how="unlist")
          containsDobject <- any(is_dobject)        

          if(containsDobject) {
            if(!all(is_dobject)) stop("Currently, the driver does not support input lists that mix DObject partitions and other object types")
            nested_list <- any(vapply(arg, function(x) { is.list(x) },FUN.VALUE=logical(1)))
            dobject_list <- rapply(arg, function(x) x@DRObj, how="unlist")
            if(length(unique(dobject_list)) > 1) stop("The driver does not currently permit partitions of different DObjects be mixed in a list")

         }
       }
     }

      # If unnamed, give it a name. Otherwise, leave it be.
      orig_nm <- nm
      if(nchar(nm) == 0 || is.null(nms)) 
        nm <- paste0(".tmpVarName",num)

      if(containsDobject) {
        if(isDObj) {
          ids[[num]] <- lapply(arg,function(argument) {
                          argument@splits
                        })
        }
          nDobjs <- nDobjs + 1
          tempName <- paste0(".tempVar",nDobjs)

          if(isDObj) 
            assign(tempName,arg[[1]]@DRObj)
          else 
            assign(tempName,dobject_list[[1]])

          tempStr <- paste0("substitute(splits(",tempName,",ids[[num]][[index]]),env=parent.frame())")

      } else {
          tempStr <- "substitute(ids[[num]][[index]],env=parent.frame())" 
      }

      if(!isDObj) {
        temp <-  mapply(function(x,y) { arg[y:(y+x-1)] }, modelApplyIterations, limits, SIMPLIFY=FALSE)
        if(!containsDobject) ids[[num]] <- temp
        else {
          all.splits <- lapply(temp, function(x) {
            as.list(rapply(x,function(x) x@splits, how="unlist"))
          })
          ids[[num]] <- all.splits   

          # replace variable with list structure provided by the user
          if(nested_list) {        
            switch_statement <- eval(parse(text=paste0("substitute(",nm,"<-switch(.execId))")),envir=new.env())
            for(x in seq(1,length(temp))) {
              # Remove contents, but maintain structure of list
              skel <- rapply(temp[[x]],function(x) NULL, how="replace")

              rhs <- eval(parse(text=paste0("substitute(",paste0(deparse(skel),collapse=""),")")),envir=new.env())
              indices <- 2
              # number the nested list items in a depth-first fashion 
              count <- 1

              while(indices[[1]] <= length(rhs)) {
                len <- length(indices)
                if(is.list(eval(rhs[[indices]]))) {
                  if(length(rhs[[indices]]) > 1)
                    indices <- c(indices,2)
                  else
                    indices[len] <- indices[len] + 1
                }
                else { 
                  rhs[[indices]] <- eval(parse(text=paste0("substitute(",nm,"[[",count,"]])")),envir=new.env())
                  count <- count + 1
                   indices[len] <- indices[len] + 1
                }
                if(len > 1 && indices[len] > length(rhs[[indices[1:(len-1)]]])) {
                  indices <- indices[1:(len-1)]
                  indices[len-1] <- indices[len-1] + 1
                }
              }

              switch_statement[[3]][[2+x]] <- rhs
            }
            nested_parts[[length(nested_parts)+1]] <- switch_statement
          }
          
        }
        
      }

      tempStr <- gsub("num",as.character(num),tempStr)
      formals(exec_func)[[nm]] <- eval(parse(text=tempStr))

      if(num > 1) argsStr <- paste0(argsStr,", ")

      argsStr <- paste0(argsStr,names(formals(exec_func))[[num]])
      if(nchar(orig_nm) != 0 && !is.null(nms)) 
        argsStr <- paste0(argsStr,"=",names(formals(exec_func))[[num]])      

    }

    defineFunction <- ".funct <- NULL"

    # Sweep through MoreArgs and check to see if there are any DistributedRObj. If so, remove them for now,
    # convert into a composite argument, and create statement to reinsert on the executor
    
    is.compositeArg <- vapply(MoreArgs,function(x) 
                      is(x,"DistributedRObj"),FUN.VALUE=logical(1))

    # Remove from MoreArgs
    compositeArgs <- MoreArgs[is.compositeArg]
    MoreArgsStr <- "MoreArgs"

    # Convert to composite dobject
    if(length(compositeArgs) > 0) {
      argInd <- length(margs)
      MoreArgsStr <- paste0("c(MoreArgs,list(")
      for(ind in seq(1,length(compositeArgs))) {
        if(!identical(compositeArgs[[ind]]@splits,1:(distributedR::npartitions(compositeArgs[[ind]]@DRObj))))
          stop("Your MoreArgs DObject argument contains only a partial and/or out-of-order split set of the underlying
                  backend Distributed R object. It cannot be converted into a composite arg") 

        nDobjs <- nDobjs + 1
        tempName <- paste0(".tempVar",nDobjs)
        assign(tempName,compositeArgs[[ind]]@DRObj)
        
        nm <- names(compositeArgs[ind])
        orig_nm <- nm
        if(nchar(nm) == 0 || is.null(nm))
          nm <- paste0(".tmpVarName",argInd+ind)

        tempStr <- paste0("substitute(splits(",tempName,"),env=parent.frame())")
        formals(exec_func)[[nm]] <- eval(parse(text=tempStr))
  
        if(ind > 1) MoreArgsStr <- paste0(MoreArgsStr,",")

        MoreArgsStr <- paste0(MoreArgsStr,nm)
        if(nchar(orig_nm) != 0 && !is.null(orig_nm))
          MoreArgsStr <- paste0(MoreArgsStr,"=",nm)
      }
     MoreArgsStr <- paste0(MoreArgsStr,"))")
    }

    MoreArgs <- MoreArgs[!is.compositeArg]   
 
    formals(exec_func)[["MoreArgs"]] <- MoreArgs
    formals(exec_func)[[".newDObj"]] <- substitute(splits(.outObj,index),env=parent.frame())

    execLine <- paste0(".newDObj <- mapply(.funct,",argsStr,",MoreArgs=",MoreArgsStr,",SIMPLIFY=FALSE)")
    
    if(length(nested_parts) > 0) {
      formals(exec_func)[[".execId"]] <- substitute(index) 
      for(l in seq(1,length(nested_parts))) {
        body(exec_func)[[1+l]] <- nested_parts[[l]]
      }
    }
 
    nLines <- length(body(exec_func))

    body(exec_func)[[nLines+1]] <- eval(parse(text=paste0("substitute(",defineFunction,")")),envir=new.env())
    body(exec_func)[[nLines+1]][[3]] <- match.fun(func)
    body(exec_func)[[nLines+2]] <- eval(parse(text=paste0("substitute(",execLine,")")),envir=new.env())
    
    if(combine=="c" && output.type=="dlist") {
      unlistedResults <- ".newDObj <- unlist(.newDObj,recursive=FALSE)"
      body(exec_func)[[nLines+3]] <- eval(parse(text=paste0("substitute(",unlistedResults,")")),envir=new.env())
    }

    nLines <- length(body(exec_func))

    # If the output is not a dlist, then we have to worry about how to 
    # stitch together the internal components of a partition --
    # either rowwise, columnwise, or the default, which is to 
    # cbind the results after flattening them into 1d-vectors
    if(output.type != "dlist") {
      if(combine=="rbind")
        if(output.type=="sparse_darray") 
          stitchResults <- ".newDObj <- do.call(rBind,.newDObj)"
        else
          stitchResults <- ".newDObj <- do.call(rbind,.newDObj)"
      else if(combine=="cbind")
        if(output.type=="sparse_darray") 
          stitchResults <- ".newDObj <- do.call(cBind,.newDObj)"
        else
          stitchResults <- ".newDObj <- do.call(cbind,.newDObj)"
      else 
        stitchResults <- ".newDObj <- simplify2array(.newDObj,higher=FALSE)"

      body(exec_func)[[nLines+1]] <- eval(parse(text=paste0("substitute(",stitchResults,")")),envir=new.env())

      if(output.type=="dframe") {
        convert <- ".newDObj <- as.data.frame(.newDObj)"
        body(exec_func)[[nLines+2]] <- eval(parse(text=paste0("substitute(",convert,")")),envir=new.env())
      } else if(output.type=="darray") {
        convert <- ".newDObj <- as.matrix(.newDObj)"
        body(exec_func)[[nLines+2]] <- eval(parse(text=paste0("substitute(",convert,")")),envir=new.env())
      }
    }

    nLines <- length(body(exec_func))

    body(exec_func)[[nLines+1]] <- substitute(update(.newDObj))

    if(dR.env$DRWarn)
      foreach(index,seq(prod(nparts)),exec_func,progress=FALSE) 
    else
      suppressWarnings(foreach(index,seq(prod(nparts)),exec_func,progress=FALSE))  

    psizes <- partitionsize(.outObj)

    if(distributedR::is.dlist(.outObj)) {
      dims <- Reduce("+",psizes)
    } else {
      dims <- as.integer(dim(.outObj))
    }

    new("DistributedRObj",DRObj = .outObj, splits = seq(npartitions(.outObj)),
         psize = psizes, dim = dims, nparts=nparts)
  }
)

