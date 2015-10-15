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

# Create Class Union for Parallel backend
setClassUnion("ParallelObjUnion", c("list","array","data.frame"))

setClass("ParallelObj",contains="DObject",
    slots=list(pObj = "ParallelObjUnion", splits = "numeric"),
    prototype = prototype(nparts = c(1L, 1L), psize = matrix(1,1), 
      dim = c(1L,1L)
))

setMethod("initialize", "ParallelObj", function(.Object, ...) {
   .Object <- callNextMethod(.Object, ...)

   numparts<-totalParts(.Object)
   #TODO: Fix for data.frame and arrays
   if(length(.Object@pObj)==0) {
    if(.Object@type == "dlist")  
       .Object@pObj <- vector("list", numparts)
    else if(.Object@type == "darray")
       .Object@pObj <- lapply(1:numparts, function(x) array(0, dim=c(0,0)))
    else
       .Object@pObj  <- lapply(1:numparts, function(x) data.frame())
  
   .Object@splits <- 1:numparts
  }

   .Object
})

#' @rdname combine
setMethod("combine",signature(driver="ParallelddR",items="list"),
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

#' @rdname get_parts
setMethod("get_parts",signature("ParallelObj","missing"),
  function(x, ...){
  index = 1:length(x@splits)
 lapply(index,function(b) {
      new("ParallelObj",pObj = x@pObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @rdname get_parts
setMethod("get_parts",signature("ParallelObj","integer"),
  function(x, index, ...){
   lapply(index,function(b) {
      new("ParallelObj",pObj = x@pObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    } )
  }
)

#' @rdname do_collect
setMethod("do_collect",signature("ParallelObj","integer"),
  function(x, parts) {
 
  xparts<-totalParts(x)
  #We can combine arbitraty subset of lists but not for other objects
  plen <- length(parts)
  if(plen> 1 && plen!=xparts && !is.dlist(x)) stop("Cannot collect more than one index at a time for non-list objects")

  if(plen < xparts){
    #We are extracting 1 partition (for darrays/dframes) or subset (for dlists)
    res<-x@pObj[x@splits[parts]]
    #Unlist only if the output had multiple partitions (i.e., nested list)
    if(plen > 1) return (unlist(res, recursive= FALSE))
    
    #We need to return a single partition
    return (res[[1]])

  }else{  

  #We have to return the full object 
  #Case 1: object is dlist
  if(is.dlist(x)){
	 #Check if unlist results in an empty list. 
         #This happens when the DObject has just been declared but not initialized any value
	 if(dim(x)==0) return (x@pObj)
	 else return (unlist(x@pObj, recursive=FALSE))
   } else{
         #Case 2: Object is darray or dframe
         #We need to reassemble the full array. We stitch partitions from left to right, and then top to bottom
         #Note that for empty darray/dframe colect returns data.frame() and 0x0 array()
	 if(all(dim(x)==0)){
	  if(is.darray(x)) return (array(0, dim=c(0,0)))
	  else return (data.frame())
	 }
	 res<-NULL
	 combinefunc<- cbind
	 #If R <3.2, use old cBind
         if(ddR.env$RminorVersion < 2) combinefunc <- Matrix::cBind

	 for (index in seq(1, xparts, by=nparts(x)[2])){
	     if(is.null(res))  #For sparse array rbind on null does not work
		res<-do.call(combinefunc,x@pObj[index:(index+nparts(x)[2]-1)])
	     else
	        res<-rbind2(res, do.call(combinefunc,x@pObj[index:(index+nparts(x)[2]-1)]))
         }
    	 return (res)
   } 
  }
})
