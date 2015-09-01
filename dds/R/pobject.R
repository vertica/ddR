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

   #TODO: Fix for data.frame and arrays
   if(length(.Object@pObj)==0) {
    if(.Object@type == "DListClass")  
       .Object@pObj <- vector("list", nparts(.Object))
    else if(.Object@type == "DArrayClass")
       .Object@pObj <- vector("list", nparts(.Object))
    else
       .Object@pObj  <- vector("list", nparts(.Object))
  
   .Object@splits <- 1:nparts(.Object)
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
 
  #We can combine arbitraty subset of lists but not for other objects
  plen <- length(parts)
  if(plen> 1 && plen!=nparts(x) && !is.dlist(x)) stop("Cannot getpartition on more than one index at a time")

  if(plen < nparts(x)){
    #We are extracting 1 partition (for darrays/dframes) or subset (for dlists)
    res<-x@pObj[x@splits[[parts]]]
    #Unlist only if the output had multiple partitions (i.e., nested list)
    if(is.list(res[[1]])) res<-unlist(res, recursive= FALSE)
    return (res)
  }else{  
  #We have to return the full object 
  #Case 1: object is dlist
  if(is.dlist(x)){
	 if(is.list(x@pObj[[1]])) return (unlist(x@pObj, recursive=FALSE))
         else return (x@pObj)
   } else{
     #Case 2: Object is darray
     if(is.darray(x)){
       #We need to reassemble the full array
       res<-NULL
       sid<-1
       eid<-sid
       while(sid <= nparts(x)){
       #Let's find partition ids for current row (i.e., stitch from left to right)
	  csize<-0
	  while(csize < x@dim[2]) {
	     csize<-csize+x@psize[eid,2]
 	     eid<-eid+1
	   }
       res<-rbind2(res, do.call(cbind,x@pObj[sid:(eid-1)]))
       sid<-eid 
       }
       return (res)
     } else {stop("Collect on all partitions is not yet supported for object of type ",class(x))}
   } 
  }
})
