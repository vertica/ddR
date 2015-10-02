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
parallel.dds.env$cores <- parallel::detectCores(all.tests=TRUE, logical=FALSE) 
parallel.dds.env$clusterType <- "FORK"
if(is.na(parallel.dds.env$cores)){ parallel.dds.env$cores <- 1 }

#' @export
# Initialize the no. of cores in parallel backend
# By default we use the FORK method of parallel which works only on UNIV environments. The "PSOCK" method requires SNOW but works on all OSes.
setMethod("init","ParallelDDS",
  function(x, executors=NULL, type= "FORK", ...){
    if(!is.null(executors)){
    if(!((is.numeric(executors) || is.integer(executors)) && floor(executors)==executors && executors>=0)) stop("Argument 'executors' should be a non-negative integral value")
        parallel.dds.env$cores <- executors
  }

  #On windows parallel can use only a single core. We need to use socket based SNOW for more number of cores.
  if((.Platform$OS.type == "windows" && executors!=1) || type =="PSOCK") {
     message("Using socket based parallel (SNOW) backend.")
     cl <- parallel::makeCluster(getOption("cl.cores", parallel.dds.env$cores))
     parallel.dds.env$snowCluster <- cl
     parallel.dds.env$clusterType <- "PSOCK"
  }
  return (parallel.dds.env$cores)
})

#' @export
setMethod("shutdown","ParallelDDS",
  function(x) {
    if(!is.null(parallel.dds.env$snowCluster) && parallel.dds.env$clusterType == "PSOCK") {
        message("Switching out of using 'parallel (SNOW)'. Shutting down SNOW cluster...")
        parallel::stopCluster(parallel.dds.env$snowCluster)
        parallel.dds.env$clusterType <- "FORK"
	parallel.dds.env$snowCluster <- NULL
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

#This function calls mclapply internally when using parallel with "FORK" or 
#snow's staticClusterApply when used with "PSOCK" option. 
#On windows only "PSOCK" provides true parallelism.

#' @export
setMethod("do_dmapply",
          signature(driver="ParallelDDS", func="function"), 
          function(driver, func, ..., MoreArgs = list(),
                   output.type =
                       c("dlist", "dframe", "darray", "sparse_darray"),
                   nparts = NULL,
                   combine = c("flatten", "row", "col"))
{
  stopifnot(is.list(MoreArgs))
  output.type <- match.arg(output.type)
  if (!is.null(nparts)) {
      stopifnot(is.numeric(nparts),
                length(nparts) == 1L || length(nparts) == 2L)
  }
  combine <- match.arg(combine)

  dots <- list(...)
  dlen<-length(dots)

  if(output.type =="dframe" && combine=="flatten") 
      stop("Cannot flatten a data frame")

  for(num in 1:dlen){
    if(is(dots[[num]],"DObject")){
      #If this is a DObject, we need to extract the backend object and reassemble it (i.e., use collect)
      #By converting DObject into a normal R object, we let mcmapply handle how iterations on differnt data types
      dots[[num]] <- collect(dots[[num]])
    }else{
      #There are two cases for a list (1) parts(dobj) or (2) normal list (don't do anything)
      if(is(dots[[num]],"list") && is(dots[[num]][[1]], "DObject")){
         dots[[num]] <- unlist (lapply(dots[[num]],function(argument){
	    	      if(is(argument,"DObject"))
            	         return(argument@pObj[argument@splits])
	              else return(argument)
         }), recursive=FALSE)
      }
    }
   }

   #Check if MoreArgs contains a distributed object. If yes, convert it into a regular object via collect
   for(index in seq_along(MoreArgs)){
         if(is(MoreArgs[[index]], "DObject"))
      	   MoreArgs[[index]] <- collect(MoreArgs[[index]])
   }


   answer <- NULL
   #Now iterate in parallel
   if(parallel.dds.env$clusterType == "PSOCK"){
    # We are using the SNOW backend, i.e., clusterMap. Check code with print(clusterMap)

    n <- lengths(dots)
    vlen <- max(n)
    if (vlen && min(n) == 0L) 
        stop("zero-length inputs cannot be mixed with those of non-zero length")
    if (!all(n == vlen)) 
         stop("Unequal length of input arguments. Length of largest argument is ", n)

    argfun <- function(i) c(lapply(dots, function(x) x[[i]]), MoreArgs)

    #TODO(iR): For now we only use static scheduling
    answer <- parallel:::staticClusterApply(parallel.dds.env$snowCluster, func, vlen, argfun)

   } else {
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
	    stop("Unequal length of input arguments. Length of largest argument is ", n)
	}
        else dots
        do_one <- function(indices, ...) {
            dots <- lapply(X, function(x) x[indices])
            .mapply(FUN, dots, MoreArgs)
        }
        answer <- parallel::mclapply(seq_len(n), do_one, mc.preschedule = TRUE, 
            mc.set.seed = TRUE, mc.silent = FALSE, 
            mc.cores = parallel.dds.env$cores, mc.cleanup = TRUE)
        do.call(c, answer)
    }
   }

   #Perform a cheap check on whether there was an error since man pages say that an error on one core will result in error messages on all. TODO: Sometimes the class of the error is "character"
   if(class(answer[[1]]) == "try-error") {stop(answer[[1]])}
   if(class(answer[[1]]) == "character" && grepl("Error", answer[[1]])) {stop(answer[[1]])}

   USE.NAMES<-TRUE #TODO(ir): What should the default be?
   if (USE.NAMES && length(dots)) {
        if (is.null(names1 <- names(dots[[1L]])) && is.character(dots[[1L]])) 
            names(answer) <- dots[[1L]]
        else if (!is.null(names1)) 
            names(answer) <- names1
    }

   #Create the output object, since we store partitions
   totalParts <- prod(nparts)
   outputObj<-vector("list", totalParts)

   #Create the output based on the number of partitions expected in the output
   #We create partitions with near equal sizes, i.e. if the #elements is not purely divisible by totalparts, we spread the remainder
   #to the 1:remainder elements
   lenAnswer<-length(answer)
   if(totalParts > lenAnswer) stop("Number of elements generated by dmapply is less than those supplied in argument 'nparts'.")

   remainder<-lenAnswer %% totalParts
   elemInEachPart<- rep(floor(lenAnswer/totalParts), totalParts)
   if(remainder>0) {elemInEachPart[1:remainder]<-elemInEachPart[1:remainder]+1}
   elemInEachPart<-c(0,cumsum(elemInEachPart))

   #Decide how we may need to combine entries from the answer into partitions
   #We handle the "flatten" case in the while loop since simplify2array has to be called with parameter "higher=FALSE"
   combineFunc <- list

   if(output.type !="dlist"){
	if(combine == "row"){
	   if(dds.env$RminorVersion > 2) #If R >3.2, use new rbind
	   	   combineFunc <- rbind
           else
	   	   combineFunc <- Matrix::rBind
	}
	else if(combine == "col"){
	   if(dds.env$RminorVersion > 2) #If R >3.2, use new cbind
	   	   combineFunc <- cbind
           else
	   	   combineFunc <- Matrix::cBind
       }
   }
   index<-1
   psizes<-array(0L,dim=c(totalParts,2)) #Stores partition sizes
   while(index <= totalParts){
   	     if((output.type == "dlist") || (combine != "flatten")){
	        if(combine == "unlist")
			     outputObj[[index]]<-do.call(combineFunc, unlist(answer[(elemInEachPart[index]+1):elemInEachPart[index+1]], recursive=FALSE))
	        else
			     outputObj[[index]]<-do.call(combineFunc, answer[(elemInEachPart[index]+1):elemInEachPart[index+1]])
             }
	     else
	        	     outputObj[[index]]<-simplify2array(answer[(elemInEachPart[index]+1):elemInEachPart[index+1]], higher=FALSE)
	     d<-dim(outputObj[[index]])
	     psizes[index,] <-(if(is.null(d)){c(length(outputObj[[index]]),0L)} else {d})
	     index<-index+1
   }

   dims<-NULL

   #Check if partions conform and can be stitched together. 
   #Check partitions in each logical row have the same number of rows/height. Similary for columns
   rowseq<-seq(1, totalParts, by=nparts[2])
   for (index in rowseq){
	     if(any(psizes[index:(index+nparts[2]-1),1]!=psizes[index,1])) stop("Adjacent partitions have different number of rows, should be ", psizes[index,1])
   }
	 
   for (index in 1:nparts[2]){
	    if(any(psizes[(rowseq+(index-1)),2]!=psizes[index,2])) stop("Adjacent partitions have different number of columns, should be ", psizes[index,2])
   }

  numcols<-sum(psizes[1:nparts[2],2]) #add cols of all partitions in the first row
  numrows<-sum(psizes[seq(1, totalParts, by=nparts[2]), 1]) #add all partitions in the first column group
  dims<-c(numrows, numcols)


  #Use single dimension if we know it's a list
  if(output.type=="dlist"){
	 psizes<-as.matrix(psizes[,1])
	 dims<-dims[1]
   }

   new("ParallelObj",pObj = outputObj, splits = 1:length(outputObj), psize = psizes, dim = as.integer(dims), nparts = nparts)
})

