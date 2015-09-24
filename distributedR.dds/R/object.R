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

# Create Distributed R Class Union
setClassUnion("DistRObj", c("dlist","darray","dframe"))

setClass("DistributedRObj",contains="DObject",
    slots=list(DRObj = "DistRObj", splits = "numeric"),
    prototype = prototype(nparts = c(1L,1L), psize = matrix(1,1), 
      dim = c(1L,1L)
))

setMethod("initialize", "DistributedRObj", function(.Object, ...) {
   .Object <- callNextMethod(.Object, ...)
    
  if(is.null(.Object@DRObj@dobject_ptr)) {
   if(.Object@type == "dlist")
       .Object@DRObj <- distributedR::dlist(npartitions=totalParts(.Object))
   else if(.Object@type == "darray")
     if(.Object@dim[1] < 1) {
       .Object@DRObj <- distributedR::darray(npartitions=totalParts(.Object))
     } else {
       .Object@DRObj <- distributedR::darray(dim=.Object@dim,blocks=.Object@psize[1,])
     }
   else
     if(.Object@dim[1] < 1) {
       .Object@DRObj <- distributedR::dframe(npartitions=totalParts(.Object))
     } else {
       .Object@DRObj <- distributedR::dframe(dim=.Object@dim,blocks=.Object@psize[1,])
     }

   .Object@splits <- seq(npartitions(.Object@DRObj))

   }

   .Object
})

#' @export
setMethod("get_parts",signature("DistributedRObj","missing"),
  function(x, ...){
  index = seq(length(x@splits))
 lapply(index,function(b) {
      new("DistributedRObj",DRObj = x@DRObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("get_parts",signature("DistributedRObj","integer"),
  function(x, index, ...){
   lapply(index,function(b) {
      new("DistributedRObj",DRObj = x@DRObj, splits = x@splits[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    } )
  }
)

#' @export
setMethod("do_collect",signature("DistributedRObj","integer"),
  function(x, parts) {
    if(identical(x@splits, parts) && length(parts) > 1) {
       getpartition(x@DRObj)
    } else if(length(parts) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      getpartition(x@DRObj,x@splits[[parts]])
   }
})

setMethod("combine",signature(driver="DistributedRDDS",items="list"),
  function(driver,items){
    split_indices <- sapply(items,function(x) {
      x@splits
    })
    dims <- sapply(items,function(x) {
      x@dim
    })

    psizes <- sapply(items,function(x) {
      x@psize
    })

    if(is.matrix(dims)) dims <- colSums(dims)
    else dims <- sum(dims) 

    psizes <- as.matrix(psizes)
    rownames(psizes) <- NULL

    new("DistributedRObj",DRObj=items[[1]]@DRObj,splits = unlist(split_indices), dim = dims, psize = psizes)
  }
)
