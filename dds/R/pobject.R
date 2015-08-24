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
    prototype = prototype(nparts = 1L, psize = matrix(1,1), 
      dim = c(1L,1L)
))

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
    if(identical(x@splits, parts) && length(parts) > 1) {
       unlist(x@pObj, recursive=FALSE)
    } else if(length(parts) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      unlist(x@pObj[x@splits[[parts]]], recursive=FALSE)
   }
})
