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

setClass("SparkRObj", contains="DObject",slots=list(RDD = "RDD", partitions = "numeric"),prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))

#' @export
setMethod("get_parts",signature("SparkRObj","missing"),
  function(x, ...){
  index = 1:length(x@splits)
 lapply(index,function(b) {
      new("SparkRObj",RDD = x@RDD, partitions = x@partitions[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("get_parts",signature("SparkRObj","missing"),
  function(x, index, ...){
 lapply(index,function(b) {
      new("SparkRObj",RDD = x@RDD, partitions = x@partitions[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("do_collect",signature("SparkRObj","integer"),
  function(x, parts) {
    if(identical(x@partitions, parts) && length(parts) > 1) {
       collect(x@RDD)
    } else if(length(parts) > 1) {
      stop("Cannot getpartition on more than one index at a time")
    }
   else {
      collectPartition(x@RDD,x@partitions[[parts]])
   }
})
