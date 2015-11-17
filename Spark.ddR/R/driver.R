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

flatMapValues <- SparkR:::flatMapValues
broadcast <- SparkR:::broadcast
parallelize <- SparkR:::parallelize
groupByKey <- SparkR:::groupByKey
partitionBy <- SparkR:::partitionBy
filterRDD <- SparkR:::filterRDD
map <- SparkR:::lapply
mapValues <- SparkR:::mapValues
mapPartition <- SparkR:::lapplyPartition
mapPartitionsWithIndex <- SparkR:::lapplyPartitionsWithIndex
zipRDD <- SparkR:::zipRDD
numPartitions <- SparkR:::numPartitions

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

  pieceSize <- floor(length(all.args[[1]])/totParts) + 1
  remainder <- length(all.args[[1]]) %% totParts

  modelApplyIterations <- c(rep(pieceSize,remainder),rep(pieceSize-1,totParts-remainder))

  key.vec <- NULL

  for(a in seq(length(modelApplyIterations))) {
    key.vec <- c(key.vec,rep(a-1,modelApplyIterations[[a]]))
  }

  current.index <- 0

  ## Step 1: Convert all non-distributed object inputs into RDDs
  ## We use PairRDDs to keep partitioning consistent

  ## Step 2: Repartition all distributed RDD inputs to have the same
  ## number of partitions as the output

  for(arg in all.args) {
    current.index <- current.index + 1
    is_dobject <- FALSE
    # If not a dobject or parts of dobjects
    if(!is.list(arg)) is_dobject <- is(arg,"ddR_RDD")
    else {
      is_dobject <- rapply(arg, function(x) is(x,"ddR_RDD"),how="unlist")
      is_dobject <- any(is_dobject)
    }

    # Standard R Object
    if(!is_dobject) {
      # Create key/value by partition id
      keyed.arg <- mapply(function(x,y) 
        list(x,y), key.vec, arg, SIMPLIFY=FALSE)
      new.RDD <- 
        parallelize(Spark.ddR.env$context,keyed.arg,totParts)
      new.RDD <- partitionBy(new.RDD, length(modelApplyIterations), 
        function(x) x[[1]])
    }
    else { # If it's a regular dobject or list of partitions
      if(!is.list(arg)) {

        # Extract this here to avoid serializing and sending the entire object
        all.psizes <- psize(arg)
        obj.nparts <- nparts(arg)
        nrows <- nrow(arg)

        # Use lapplyPartition...all dobjects should already be partitioned
        # correctly, but if not, uncomment the lines below

        # arg@RDD <- partitionBy(arg@RDD, totalParts(arg), 
        #  function(x) {
        #    x[[1]]
        #  })

        warning("Repartitioning")

        shuffle.partitions <- function(part_ind,x) {
            global.row <- part_ind / obj.nparts[[2]]
            global.col <- part_ind %% obj.nparts[[2]]
            if(global.row == 0) row.offset <- 0
            else {   
              row.parts <- seq(1,global.row*obj.nparts[[2]],by=obj.nparts[[2]])
              row.offset <- sum(all.psizes[row.parts,][[1]])
            }
            if(global.col == 0) col.offset <- 0
            else {
              col.parts <- seq(1,global.col)
              col.offset <- sum(all.psizes[col.parts,][[2]])
            }

          global.offset <- row.offset + col.offset * nrows
    
          local.colsize <- all.psizes[part_ind + 1,][[1]]
          x <- mapply(function(x,y) {
            row <- y %% local.colsize
            col <- y / local.colsize + 1
            global.pos <- global.offset + y + (nrows*col) + row

            list(key.vec[[global.pos]], x[[2]])
            },x,1:length(x),SIMPLIFY=FALSE)
        }

        new.RDD <- partitionBy(arg@RDD,totParts,
                    function(x) x[[1]])

        # perform repartitioning here if needed, print warning
      } else { # TODO: take care of embedded lists, currently there is only
               # code for direct list of parts
        # TODO(etduwx): Is there any way to make this more efficient?
        # Get all parts referred to in this list
        
        all.objs <- unlist(arg)
        all.parts <- vapply(all.objs, function(x) {
          if(!is.dobject(x)) stop("Parts-list cannot contain objects other than 
            dobjects")
          x@partitions
          },
          FUN.VALUE=numeric(1))

        dims.map <- list()
        
        for(a in seq(length(all.objs))) {
          dims.map[[as.character(all.parts[[a]]-1)]] <- all.objs[[a]]@dim
        }

        # Currently, we don't have support of using the same partition more than once
        if(any(duplicated(all.objs))) stop("Cannot reuse the same partition index of the same RDD")

        # Filter this RDD down to the constituent parts
        new.RDD <- filterRDD(all.objs[[1]]@RDD, function(x)
          x[[1]] %in% (all.parts-1))

        new.RDD <- groupByKey(new.RDD,length(all.parts))

        # Now wrap each partition into its distributed object type
        if(is.dlist(all.objs[[1]])) {
          wrap.FUN <- function(x) {
            x <- list(x[[1]],x[[2]]) 
          }
        } else if(is.darray(all.objs[[1]]))
          {
          wrap.FUN <- function(x) {
            dimensions <- 
              dims.map[[as.character(x[[1]])]]
            x <- list(x[[1]],
             matrix(unlist(x[[2]]),
               dimensions[[1]],dimensions[[2]]))
          }        
        } else {
            wrap.FUN <- function(x) {
            x <- list(x[[1]],
               do.call(data.frame(x[[2]]))
               )
          }
        }

        new.RDD <- map(new.RDD,wrap.FUN)

        # Create switch statement to order elements
        switch.statement <- substitute(x[[1]] <- switch(x[[1]] + 1))

        for(f in seq(length(all.parts))) {
          switch.statement[[3]][[all.parts[[f]] + 2]] <- key.vec[[f]]
        }

        repartition.fun <- function(x){}
        body(repartition.fun)[[2]] <- switch.statement
        body(repartition.fun)[[3]] <- substitute(x)

        # Now partition by new ID
        new.RDD <- map(new.RDD,repartition.fun)
        new.RDD <- partitionBy(new.RDD,totParts,function(x) x[[1]])
      }                              
    }
    
    # Store all of the RDDs
    all.RDDs[[current.index]] <- new.RDD
  }

  ## Step 3: Zip up all inputs into one RDD 
  compound.RDD <- all.RDDs[[1]]

  if(length(all.RDDs) > 1) {
    for(j in seq(2,length(all.RDDs))) {
      compound.RDD <- zipRDD(compound.RDD,all.RDDs[[j]])
    }
   # Can't use groupByKey here because we still want to keep 
  # elements separate
  compound.RDD <- map(compound.RDD, function(x) 
    list(x[[1]][[1]],lapply(x,function(y) y[[2]])))
  } 

#  return(compound.RDD)
  ## Step 4: Insert wrapper functions, list-of-parts conversion code
  # Let SparkR serialize this closure, as well as MoreArgs
  .funct <- match.fun(func)

  nms <- names(all.args)

  argsStr = "c("

  exec.func <- function(data) {}

  for(p in seq(1,length(all.args))) {
    if(p != 1) argsStr <- paste0(argsStr,", ")
    argsStr <- paste0(argsStr,'"')
    if(nchar(nms[[p]]) != 0 && !is.null(nms))
      argsStr <- paste0(argsStr,nms[[p]])
    argsStr <- paste0(argsStr,'"')
  }

  argsStr <- paste0(argsStr,")")

  listWrap <- "data <- list(data)"
  setNames <- paste0("names(data) <- ", argsStr)

  # broadcast moreargs
  if(length(MoreArgs) > 0) {
    # strange behavior, need to check for NAs
    execLine <- "out <- do.call(.funct,c(data,MoreArgs))"
  } else {
    execLine <- "out <- do.call(.funct,data)"
  }

  if(length(all.RDDs) == 1) 
    body(exec.func)[[2]] <- eval(parse(text=paste0("substitute(",listWrap,")")),envir=new.env())

  body(exec.func)[[length(body(exec.func)) + 1]] <- eval(parse(text=paste0("substitute(",setNames,")")),envir=new.env())
  body(exec.func)[[length(body(exec.func)) + 1]] <- eval(parse(text=paste0("substitute(",execLine,")")),envir=new.env())
  
  # Unlist each result if dlist and combine is "c"
  if(output.type == "dlist" && combine == "c") 
    body(exec.func)[[length(body(exec.func)) + 1]] <- substitute(unlist(out,recursive=FALSE))

  ## Step 5: Run lapplyWithPartitions
  output.RDD <- mapValues(compound.RDD,exec.func)

  exec.func <- function(data){}
  ## Step 6: Collect and compute psizes
  if(output.type == "dlist")
    getSizes <- "length(data)"
  else {
    if(combine == "c" || combine == "default") { 
      body(exec.func)[[length(body(exec.func))+ 1]] <- 
        substitute(temp <- unlist(data))
    }
    else if (combine == "cbind") {
       body(exec.func)[[length(body(exec.func))+ 1]] <- 
        substitute(temp <- do.call(cbind,data))
    }
    else {
       body(exec.func)[[length(body(exec.func))+ 1]] <- 
        substitute(temp <- do.call(rbind,data))
    }
    if(output.type == "dframe") {
       body(exec.func)[[length(body(exec.func))+ 1]] <- 
        substitute(data <- as.data.frame(temp)) 
    }
    else  {
      body(exec.func)[[length(body(exec.func))+ 1]] <- 
        substitute(data <- as.matrix(temp))
    }

    getSizes <- "dim(data)"
    combined.RDD <- mapValues(groupByKey(output.RDD,totParts), exec.func)
  }

  exec.func <- function(data){}
  body(exec.func)[[length(body(exec.func))+ 1]] <- eval(parse(text=paste0("substitute(",getSizes,")")),envir=new.env())

  if(output.type=="dlist")
    combined.RDD <- groupByKey(output.RDD,totParts)

  sizes.RDD <- mapValues(combined.RDD, exec.func)
  
  if(output.type != "dlist") {
   output.RDD <- partitionBy(flatMapValues(output.RDD, function(x) {
     as.list(x)
     }), totParts, function(x) x[[1]]) 
  }

  psizes <- t(as.data.frame(lapply(SparkR:::collect(sizes.RDD),function(x) x[[2]])))

  if(output.type != "dlist") {
    colParts <- seq(1,nparts[[2]])
    rowParts <- seq(1,totParts,by=nparts[[2]])
 
    dims <- c(sum(psizes[rowParts,1]),sum(psizes[colParts,2]))
  } else {
    dims <- sum(psizes)
  }

  ## Step 7: Create new ddR_RDD object

  new("ddR_RDD", RDD = output.RDD, nparts=nparts, psize = psizes, dim = dims, partitions = 1:prod(nparts))
})
