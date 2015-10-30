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

#' @import methods ddR 
#' @importFrom SparkR sparkR.init sparkR.stop 

Spark.ddR.env <- new.env()

# Create distributedR ddRDriver
setClass("SparkRddR", contains="ddRDriver")

#' @export 
# Exported Driver
SparkR <- new("SparkRddR",DListClass = "ddR_RDD",DFrameClass = "ddR_RDD",DArrayClass = "ddR_RDD",backendName = "SparkR")

#' @export
setMethod("init","SparkRddR",
  function(x,...) {
    message("Backend switched to SparkR. Initializing the Spark context...")
    Spark.ddR.env$context <- sparkR.init()
    ## TODO(etduwx): return the actual number of executors
    return (1)
  }
)

#' @export
setMethod("shutdown","SparkRddR",
  function(x) {
    message("Switching out of using SparkR. Shutting it down...")
    sparkR.stop()
  }
)

#' @export
setMethod("do_dmapply",
           signature(driver="SparkRddR",func="function"),
           function(driver,func,...,MoreArgs=list(),
                    output.type =
                      c("dlist","dframe","darray","sparse_darray"),
                    nparts=NULL,
                    combine=c("default","c","rbind","cbind")) {

  all.args <- list(...)
  totParts <- prod(nparts)

  # Each argument will be an RDD
  all.RDDs <- vector(mode = "list", length = length(all.args))
  wrap <- vector(mode = "logical", length = length(all.args))

  current.index <- 0

  ## Step 1: Convert all non-distributed object inputs into RDDs

  ## Step 2: Repartition all distributed RDD inputs to have the same
  ## number of partitions as the output

  for(arg in all.args) {
    is_dobject <- FALSE
    current.index = current.index + 1
    wrap[[current.index]] <- FALSE
    # If not a dobject or parts of dobjects
    if(!is.list(arg)) is_dobject <- is(arg,"ddR_RDD")
    else {
      is_dobject <- rapply(arg, function(x) is(x,"ddR_RDD"),how="unlist")
      is_dobject <- any(is_dobject)
    }

    if(!is_dobject) new.RDD <- 
      SparkR:::parallelize(Spark.ddR.env$context,arg,totParts)
    else { # If it's a regular dobject or list of partitions
      if(!is.list(arg)) {
        # Need to coalesce with RDD down to number of output partitions
        # TODO(etduwx): Make sure resulting partitions are consistent
        # with necessary dimensions
        new.RDD <- SparkR:::coalesce(arg@RDD, totParts, shuffle=FALSE)
      } else { # TODO: take care of embedded lists, currently there is only
               # code for direct list of parts
        # TODO(etduwx): Is there any way to make this more efficient?
        # Get all parts referred to in this list
 
        wrap[[current.index]] <- TRUE       

        part.ids <- vapply(arg, function(x)
          x@partitions, FUN.VALUE=integer(1))

        # If the selection is a superset or a subset, we need to pick them out
        if(!identical(part.ids,1:SparkR:::numPartitions(arg[[1]]@RDD))) {
          ## TODO: implement
          # new.RDD <- getPartitionPrunedRDD(arg[[1]]@RDD,part.ids)
        } else {
          new.RDD <- arg[[1]]@RDD
        }
        
      }
    }                                
    
    all.RDDs[[current.index]] <- new.RDD
  }

  ## Step 3: Zip up all inputs into one RDD 
  compound.RDD <- all.RDDs[[1]]

  if(length(all.RDDs) > 1) {
    for(j in seq(2,length(all.RDDs))) {
      compound.RDD <- SparkR:::zipRDD(compound.RDD,all.RDDs[[j]])
    }
  } 

  ## Step 4: Insert wrapper functions, list-of-parts conversion code
  # Let SparkR serialize this closure, as well as MoreArgs
  .funct <- match.fun(func)

  nms <- names(all.args)

  argsStr = ""

  exec.func <- function(RDD_part) {}

  for(p in seq(1,length(all.args))) {
    if(p != 1) argsStr <- paste0(argsStr,", ")
    if(nchar(nms[[p]]) != 0 && !is.null(nms))
      argsStr <- paste0(argsStr, nms[[p]], "=")
    
    if(!wrap[[p]]) {
      argsStr <- paste0(argsStr, "lapply(RDD_part, function(x) x[[",p,"]])")
    } else {
      argsStr <- paste0(argsStr, "list(lapply(RDD_part, function(x) x[[",p,"]]))")
    }
  }

  execLine <- paste0(".newDObj <- mapply(.funct,",argsStr,",MoreArgs=MoreArgs,SIMPLIFY=FALSE)")
  
  body(exec.func)[[2]] <- eval(parse(text=paste0("substitute(",execLine,")")),envir=new.env())

  ## Step 5: Run lapplyWithPartitions
  output.RDD <- SparkR:::lapplyPartition(compound.RDD,exec.func)

  ## Step 6: Collect and compute psizes
  if(output.type == "dlist")
    getSizes <- "length(RDD_part)"
  else
    getSizes <- "dim(RDD_part[[1]])"

  body(exec.func)[[2]] <- eval(parse(text=paste0("substitute(",getSizes,")")),envir=new.env())
  sizes.RDD <- SparkR:::lapplyPartition(output.RDD,exec.func)

  psizes <- t(as.data.frame(SparkR:::collect(sizes.RDD)))
  dims <- sum(psizes)

  ## Step 7: Create new ddR_RDD object

  new("ddR_RDD", RDD = output.RDD, nparts=nparts, psize=psizes, dim=dims, partitions = 1:prod(nparts))
})
