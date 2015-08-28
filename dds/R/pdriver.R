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

setClass("ParallelDDS", contains="DDSDriver")

#' @export 
# Exported Driver
parallel <- new("ParallelDDS",DListClass = "ParallelObj",DFrameClass = "ParallelObj",DArrayClass = "ParallelObj",backendName = "parallel")

# Driver for the parallel package. Parallel is also the default backend.
dds.env$driver <- parallel
#Set environment and default number of cores to total no. of cores (but one on windows)
#Note that DetectCores() can return NA. 
parallel.dds.env <- new.env(emptyenv())
parallel.dds.env$cores <- detectCores(all.tests=TRUE, logical=FALSE) 
if(.Platform$OS.type == "windows" || is.na(parallel.dds.env$cores)) parallel.dds.env$cores <- 1 

#' @export
# Initialize the no. of cores in parallel backend
setMethod("init","ParallelDDS",
  function(x, inst=NULL, ...){
    if(!is.null(inst)){
    #On windows we should only use a single core, which is already the default (limitation of 'parallel')
    if(.Platform$OS.type == "windows" && inst!=1) {stop("Argument 'inst' should be 1 on Windows\n")}
    if(!((is.numeric(inst) || is.integer(inst)) && floor(inst)==inst && inst>=0)) stop("Argument 'inst' should be a non-negative integral value")
        parallel.dds.env$cores = inst
  }
})

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
})

#This function calls mclapply internally. 
# TODO(iR): Parallel processing does not work on Windows due to limitation of parallel package
#' @export
setMethod("do_dmapply",signature(driver="ParallelDDS",func="function",MoreArgs="list", FUN.VALUE="ANY"), 
  function(driver,func,...,MoreArgs=list(), FUN.VALUE=NULL){
  dots <- list(...)
  dlen<-length(dots)
  elementWise <- vector(mode="logical", dlen)

  for(num in 1:dlen){
    if(is(dots[[num]],"DObject")){
      #If this is a DObject, we just need to extract the backend object
      if(dots[[num]]@type != "DListClass"){ 
         stop("Elementwise operation is currently supported only on distributed lists.")
      }
      tmp<-unlist(dots[[num]]@pObj[dots[[num]]@splits])
      #Check if unlist results in an empty list. This happens when the DObject has just been declared but not initialized any value
      if(length(tmp) ==0){
            dots[[num]] <- dots[[num]]@pObj[dots[[num]]@splits]
      } else {
            dots[[num]] <- tmp
      }
      elementWise[num] <- TRUE
    }else{
      #At the moment we don't support non-list objects to iterate on
      stopifnot(is(dots[[num]],"list"))
      #There are two cases for a list (1) parts(dobj) or (2) normal list (don't do anything)
      if(is(dots[[num]][[1]], "DObject")){
            dots[[num]] <- unlist (lapply(dots[[num]],function(argument){
	    	      if(is(argument,"DObject"))
            	         return(argument@pObj[argument@splits])
	              else return(argument)
         }), recursive=FALSE)
      }
    }
   }

   #Now iterate in parallel
   #We directly call the internal mcmapply function. Check code by print(mcmapply) 
   FUN <- match.fun(func)
   if (!length(dots)) 
        stop("Length of dmapply argument is zero")
   lens <- sapply(dots, length)
   n <- max(lens)
   if (n && min(lens) == 0L) 
       stop("Zero-length inputs cannot be mixed with those of non-zero length")
   answer <- if (n < 2L) 
       .mapply(FUN, dots, MoreArgs)
   else {
        X <- if (!all(lens == n)){ 
            #lapply(dots, function(x) rep(x, length.out = n))
	    stop("Unequal length of input arguments. Length of largest argument is ", n)
	}
        else dots
        do_one <- function(indices, ...) {
            dots <- lapply(X, function(x) x[indices])
            .mapply(FUN, dots, MoreArgs)
        }
        answer <- mclapply(seq_len(n), do_one, mc.preschedule = TRUE, 
            mc.set.seed = TRUE, mc.silent = FALSE, 
            mc.cores = parallel.dds.env$cores, mc.cleanup = TRUE)
        do.call(c, answer)
    }

   #Let's check if each partition corresponds to the class defined in FUN.VALUE. 
   #By default each partition should be a list
   type<-"list"
   if(is.matrix(FUN.VALUE)) type<-"matrix"
   if(is.data.frame(FUN.VALUE)) type<-"data.frame"
   if(!all(vapply(answer, function(x){return (class(x)==type)}, FUN.VALUE=array()))){
    #Check if an error occurred. Parallel returns object of type "try-error"
    lapply(answer, function(x){if(class(x)=="try-error"){stop(x)}})
    stop("Result of FUN should be of type ", type)
   }
   
   USE.NAMES<-TRUE #TODO(ir): What should the default be?
   if (USE.NAMES && length(dots)) {
        if (is.null(names1 <- names(dots[[1L]])) && is.character(dots[[1L]])) 
            names(answer) <- dots[[1L]]
        else if (!is.null(names1)) 
            names(answer) <- names1
    }

   #Calculate partition sizes and total dimension. For lists, numeric etc. use length. Otherwise dim()
   #TODO (iR): Fix base DObject dimension to be numeric since length of list partition can be double?
   psizes<-vapply(answer, function(x){ if(is.null(dim(x))) {c(as.integer(length(x)),0L)} else {dim(x)}}, FUN.VALUE=integer(2))
   psizes<-t(psizes) #ith row now corresponds to ith partition
   dims<-colSums(psizes)
   #Use single dimension if we know it's a list
   if(type=="list"){
	 psizes<-as.matrix(psizes[,1])
	 dims<-dims[1]
   }

   new("ParallelObj",pObj = answer, splits = 1:length(answer), psize = psizes, dim = as.integer(dims))
})

