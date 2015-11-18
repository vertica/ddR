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

setClass("ParallelddR", contains="ddRDriver")

#' The default parallel driver
#' @examples
#' \dontrun{
#' useBackend(parallel,executors=4)
#' }
#' @export 
# Exported Driver
parallel <- new("ParallelddR",DListClass = "ParallelObj",DFrameClass = "ParallelObj",DArrayClass = "ParallelObj",backendName = "parallel")
# Driver for the parallel package. Parallel is also the default backend.
ddR.env$driver <- parallel

#Set environment and default number of cores to total no. of cores (but one on windows)
#Note that DetectCores() can return NA. If it's windows we use SNOW by default
parallel.ddR.env <- new.env(emptyenv())
parallel.ddR.env$cores <- parallel::detectCores(all.tests=TRUE, logical=FALSE) 
parallel.ddR.env$clusterType <- "FORK"
parallel.ddR.env$snowCluster <- NULL
if(is.na(parallel.ddR.env$cores)){ parallel.ddR.env$cores <- 1 }
if((.Platform$OS.type == "windows")) {parallel.ddR.env$clusterType <- "PSOCK"}

#Function to initialize SNOW on windows
initializeSnowOnWindows<-function(){
  cl <- parallel::makeCluster(getOption("cl.cores", parallel.ddR.env$cores))
  parallel.ddR.env$snowCluster <- cl
  parallel.ddR.env$clusterType <- "PSOCK"
}

# Initialize the no. of cores in parallel backend
# By default we use the FORK method of parallel which works only on UNIx environments. The "PSOCK" method requires SNOW but works on all OSes.
#' @param executors Number of cores to run with.
#' @param type If "FORK", will use UNIX fork() method. If "PSOCK", will use SNOW method.
#' @describeIn init Initialization for parallel
setMethod("init","ParallelddR",
  function(x, executors=NULL, type= "FORK", ...){
    if(!is.null(executors)){
    if(!((is.numeric(executors) || is.integer(executors)) && floor(executors)==executors && executors>=0)) stop("Argument 'executors' should be a non-negative integral value")
        parallel.ddR.env$cores <- executors
  }

  #On windows parallel can use only a single core. We need to use socket based SNOW for more number of cores.
  if((.Platform$OS.type == "windows" &&  parallel.ddR.env$cores!=1 ) || type =="PSOCK") {
     message("Using socket based parallel (SNOW) backend.")
     initializeSnowOnWindows()
  } else{
     if(.Platform$OS.type == "windows" && parallel.ddR.env$cores>1) { stop("On windows, multi-process execution with FORK is not supported for more than one core. Use backend with type = 'PSOCK'")}
     parallel.ddR.env$clusterType <- "FORK"
  }
  return (parallel.ddR.env$cores)
})

#' @describeIn shutdown Shutdown for parallel
setMethod("shutdown","ParallelddR",
  function(x) {
    if(!is.null(parallel.ddR.env$snowCluster) && parallel.ddR.env$clusterType == "PSOCK") {
        message("Switching out of using 'parallel (SNOW)'. Shutting down SNOW cluster...")
        parallel::stopCluster(parallel.ddR.env$snowCluster)
        parallel.ddR.env$clusterType <- "FORK"
	parallel.ddR.env$snowCluster <- NULL
    }
})

#This function calls mclapply internally when using parallel with "FORK" or 
#snow's staticClusterApply when used with "PSOCK" option. 
#On windows only "PSOCK" provides true parallelism.

#' @rdname do_dmapply
setMethod("do_dmapply",
          signature(driver="ParallelddR", func="function"), 
          function(driver, func, ..., MoreArgs = list(),
                   output.type =
                       c("dlist", "dframe", "darray", "sparse_darray"),
                   nparts = NULL,
                   combine = c("default","c", "rbind", "cbind"))
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


  for(num in 1:dlen){
    if(is(dots[[num]],"DObject")){
      #If this is a DObject, we need to extract the backend object and reassemble it (i.e., use collect)
      #By converting DObject into a normal R object, we let mcmapply handle how iterations on differnt data types
      dots[[num]] <- collect(dots[[num]])
    }else{
      #There are two cases for a list (1) parts(dobj) or (2) list of parts(dobj)

      if(is(dots[[num]],"list") && any(rapply(dots[[num]], function(x) is(x,"ParallelObj"),how="unlist"))){    
        tmp <- rapply(dots[[num]],function(argument){
	    	      if(is(argument,"DObject"))
            	         return(argument@pObj[argument@splits])
	              else return(argument)}, how="replace")

       #(iR): This is bit of a hack. rapply increases the depth of the list, but at the level that the replacement occured. 
       #Simple ulist() does not work. If this was just parts(A,..), we can call unlist. If this is a list of parts, then 
       #we unwrap the second layer of the list. Unwrapping the second layer is incorrect if parts(A) was embedded deeper than that.
       if(is(dots[[num]][[1]], "DObject")){
       	  dots[[num]] <- unlist(tmp, recursive=FALSE)
       } else {
          for(index in seq_along(tmp)){
       	        if(length(tmp[[index]])>0)
			tmp[[index]] <- unlist(tmp[[index]], recursive=FALSE)
	        else
			tmp[[index]] <- tmp[[index]]
 	       }
          dots[[num]] <- tmp
      }
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
   if(parallel.ddR.env$clusterType == "PSOCK"){
    # We are using the SNOW backend, i.e., clusterMap. Check code with print(clusterMap)
    if(is.null(parallel.ddR.env$snowCluster)){initializeSnowOnWindows()}

    #Wrap the input arguments and use do.call()
    dots <- c(list(cl = parallel.ddR.env$snowCluster, fun = func, MoreArgs = MoreArgs, RECYCLE = FALSE, SIMPLIFY = FALSE), dots)
    answer <- do.call(parallel::clusterMap, dots)

   } else {
   #Wrap inputs in a list to call mcmapply via do.call()

   dots <- c(list(FUN = func, MoreArgs = MoreArgs, SIMPLIFY = FALSE, mc.cores = parallel.ddR.env$cores), dots)
   answer <- do.call(parallel::mcmapply, dots)

   }

   #Perform a cheap check on whether there was an error since man pages say that an error on one core will result in error messages on all. TODO: Sometimes the class of the error is "character"
   if(inherits(answer[[1]], "try-error")) {stop(answer[[1]])}
   if(class(answer[[1]]) == "character" && grepl("Error", answer[[1]])) {stop(answer[[1]])}

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
       #Setup the partition types that we will use later to check if partitions conform to output.type.   
       if(output.type == "darray") ptype<-"matrix"
       if(output.type == "dframe") ptype<-"data.frame"
       if(output.type == "sparse_darray") ptype<-c("dsCMatrix", "dgCMatrix")

	if(combine == "rbind"){
	   if(ddR.env$RminorVersion > 2) #If R >3.2, use new rbind
	   	   combineFunc <- rbind
           else
	   	   combineFunc <- Matrix::rBind
	}
	else if(combine == "cbind"){
	   if(ddR.env$RminorVersion > 2) #If R >3.2, use new cbind
	   	   combineFunc <- cbind
           else
	   	   combineFunc <- Matrix::cBind
       }
   }
   index<-1
   psizes<-array(0L,dim=c(totalParts,2)) #Stores partition sizes
   while(index <= totalParts){
   	     if(output.type == "dlist"){
	        if(combine == "c")
			     outputObj[[index]] <- unlist(answer[(elemInEachPart[index]+1):elemInEachPart[index+1]], recursive=FALSE)
	        else
			     outputObj[[index]] <- answer[(elemInEachPart[index]+1):elemInEachPart[index+1]]
             }else {
	        if(combine == "c" || combine =="default"){
	        	     outputObj[[index]] <- simplify2array(answer[(elemInEachPart[index]+1):elemInEachPart[index+1]], higher=FALSE)
                }else{
			     outputObj[[index]] <- do.call(combineFunc, answer[(elemInEachPart[index]+1):elemInEachPart[index+1]])
                }
	        if(!(class(outputObj[[index]]) %in% ptype)) {stop("Each partition of the result should be of type = ", ptype, ", to match with output.type =", output.type)}
	     }

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

