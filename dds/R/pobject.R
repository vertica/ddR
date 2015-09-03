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
    if(.Object@type == "DListClass")  
       .Object@pObj <- vector("list", numparts)
    else if(.Object@type == "DArrayClass")
       .Object@pObj <- vector("list", numparts)
    else
       .Object@pObj  <- vector("list", numparts)
  
   .Object@splits <- 1:numparts
  }

   .Object
})


#' @export
setMethod("get_parts",signature("ParallelObj","missing"),
  function(x, ...){
  index = 1:length(x@splits)
 lapply(index,function(b) {
      new("ParallelObj",pObj = x@pObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("get_parts",signature("ParallelObj","integer"),
  function(x, index, ...){
   lapply(index,function(b) {
      new("ParallelObj",pObj = x@pObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    } )
  }
)

#' @export
setMethod("do_collect",signature("ParallelObj","integer"),
  function(x, parts) {
 
  xparts<-totalParts(x)
  #We can combine arbitraty subset of lists but not for other objects
  plen <- length(parts)
  if(plen> 1 && plen!=xparts && !is.dlist(x)) stop("Cannot collect more than one index at a time for non-list objects")

  if(plen < xparts){
    #We are extracting 1 partition (for darrays/dframes) or subset (for dlists)
    res<-x@pObj[x@splits[[parts]]]
    #Unlist only if the output had multiple partitions (i.e., nested list)
    if(plen > 1) res<-unlist(res, recursive= FALSE)
    return (res[[1]])
  }else{  
  #We have to return the full object 
  #Case 1: object is dlist
  if(is.dlist(x)){
	 if(is.list(x@pObj[[1]])) return (unlist(x@pObj, recursive=FALSE))
         else return (x@pObj)
   } else{
         #Case 2: Object is darray or dframe
         #We need to reassemble the full array. We stitch partitions from left to right, and then top to bottom
	 res<-NULL
	 for (index in seq(1, xparts, by=nparts(x)[2])){
             res<-rbind2(res, do.call(cbind,x@pObj[index:(index+nparts(x)[2]-1)]))
         }
    	 return (res)
   } 
  }
})
