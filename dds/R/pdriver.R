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

# Driver for the parallel package

parallel.dds.env <- new.env(emptyenv())

setClass("ParallelDDS", contains="DDSDriver")

#' @export 
# Exported Driver
parallel <- new("ParallelDDS",DListClass = "ParallelObj",DFrameClass = "ParallelObj",DArrayClass = "ParallelObj",backendName = "parallel")

setMethod("initialize", "ParallelObj", function(.Object, ...) {
   .Object <- callNextMethod(.Object, ...)

   #TODO: Fix for data.frame and arrays
   if(length(.Object@pObj)==0) {
    if(.Object@type == "DListClass")  
       .Object@pObj <- vector("list", .Object@nparts)
    else if(.Object@type == "DArrayClass")
       .Object@pObj <- vector("list", .Object@nparts)
    else
       .Object@pObj  <- vector("list", .Object@nparts) 
  
   .Object@splits <- 1:(.Object@nparts)
  }

   .Object
})

#' @export
# Initialize the no. of cores in parallel
setMethod("init","ParallelDDS",
  function(x,...){
    library(parallel)
    #Use all cores for now (Detectcores can return NA)
    ncores<-detectCores(all.tests=TRUE, logical=FALSE)
    if(is.na(ncores)) x<-1
    parallel.dds.env$cores = ncores
    }
)

setMethod("combine",signature(driver="ParallelDDS",items="list"),
  function(driver,items){
    split_indices <- lapply(items,function(x) {
      x@splits
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
    
    new("ParallelObj",pObj=items[[1]]@pObj,splits = unlist(split_indices), dim = dims, psize = psizes)
  }
)

#' @export
setMethod("do_dmapply",signature(driver="ParallelDDS",func="function",MoreArgs="list"), 
  function(driver,func,...,MoreArgs=list()){
  dots <- list(...)
  for(num in 1:length(dots)){
    if(is(dots[[num]],"DObject")){
      #If this is a DObject, we just need to extract the backend object
      dots[[num]] <- dots[[num]]@pObj[dots[[num]]@splits]
    }else{
      #At the moment we don't support non-list objects to iterate on
      stopifnot(is(dots[[num]],"list"))
      #There are two cases for a list (1) parts(dobj) or (2) normal list
      dots[[num]] <- unlist (lapply(dots[[num]],function(argument){
	      if(is(argument,"DObject"))
               return(argument@pObj[argument@splits])
             else return(argument)
     }), recursive=FALSE)
    }
   }

   #Directly call the internal mapply function (check using print(mapply) on R console)
   #TODO fix to use mcmapply
   FUN <- match.fun(func)
   answer <- .Internal(mapply(FUN, dots, MoreArgs))
   USE.NAMES<-FALSE #TODO(ir): Fix
   SIMPLIFY<-FALSE
   if (USE.NAMES && length(dots)) {
        if (is.null(names1 <- names(dots[[1L]])) && is.character(dots[[1L]])) 
            names(answer) <- dots[[1L]]
        else if (!is.null(names1)) 
            names(answer) <- names1
    }
   if (!identical(SIMPLIFY, FALSE) && length(answer)) 
        answer<-simplify2array(answer, higher = (SIMPLIFY == "array"))

   #Calculate partition sizes and total dimension. For lists, numeric etc. use length. Otherwise dim()
   #TODO: Fix base DObject dimension to be numeric since length of list partition can be double?
   psizes<-vapply(answer, function(x){ if(is.null(dim(x))) {c(as.integer(length(x)),0L)} else {dim(x)}}, FUN.VALUE=integer(2))
   psizes<-t(psizes) #ith row now corresponds to ith partition
   dims<-colSums(psizes)
   #Use single dimension if we know it's a list
   if(dims[2]==0) psizes<-as.matrix(psizes[,1])
   
   new("ParallelObj",pObj = answer, splits = 1:length(answer), psize = psizes, dim = as.integer(dims))
})

