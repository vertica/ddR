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

#' @include driver.R

collectPartition <- SparkR:::collectPartition

# Create the ddR_RDD Object
setClass("ddR_RDD",contains="DObject",
    slots=list(RDD = "RDD", partitions = "numeric"),
    prototype = prototype(nparts = c(1L,1L), psize = matrix(1,1),
      dim = c(1L,1L)
))

setMethod("initialize", "ddR_RDD", function(.Object, ...) {
   .Object <- callNextMethod(.Object, ...)
})

#' @export
setMethod("get_parts",signature("ddR_RDD","missing"),
  function(x, ...){
  index = seq(length(x@partitions))
 lapply(index,function(b) {
      new("ddR_RDD",RDD = x@RDD, partitions = x@partitions[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    })
}
)

#' @export
setMethod("get_parts",signature("ddR_RDD","integer"),
  function(x, index, ...){
   lapply(index,function(b) {
      new("ddR_RDD",RDD = x@RDD, partitions = x@partitions[b],dim = as.integer(x@psize[b,]),psize=matrix(x@psize[b,],nrow=1,ncol=length(x@psize[b,])))
    } )
  }
)

#' @export
setMethod("do_collect",signature("ddR_RDD","integer"),
  function(x, parts) {
    if(identical(x@partitions, parts) && length(parts) > 1) {
       temp <- SparkR:::collect(x@RDD,flatten=FALSE)
       if(is.dlist(x)) { do.call(c,
         lapply(temp,function(x) 
            lapply(x,function(y) y[[2]])))}
       else {# Use nparts to stitch together here
         ind <- 0
         temp <- SparkR:::collect(mapPartition(x@RDD, function(p) {
               lapply(p, function(o)
                 o[[2]])}),flatten=FALSE)
 
         ans <- NULL
     
         for(i in seq(1,nparts(x)[[1]])) {
           intermediate <- NULL
           for(j in seq(1,nparts(x)[[2]])) {
             ind <- ind + 1
             if(is.dframe(x))  {
               chunk <- do.call(data.frame,temp[[ind]])
               intermediate <- c(intermediate,chunk) 
             }
             else {
               chunk <- matrix(unlist(temp[[ind]]),psize(x,ind)[[1]],
                          psize(x,ind)[[2]])
               intermediate <- cbind(intermediate, chunk)         
             }
               
           }

             # TODO: Optimize and set real names
             if(is.dframe(x)) {
               intermediate <- as.matrix(unname(as.data.frame(intermediate)))
             }
  
             ans <- rbind(ans,intermediate)
         }
        if(is.dframe(x)) ans <- as.data.frame(ans)
        ans
       }
    } else if(length(parts) > 1) {
      stop("Cannot collect on more than one index at a time")
    }
   else {
      temp <- lapply(collectPartition(x@RDD,(x@partitions[[parts]])-1L), function(y) y[[2]])
      if(is.dlist(x)) temp
      else{
        if(is.dframe(x)) {
             # TODO: Optimize and set real names
             df.names <- lapply(seq(length(temp)), function(x)
              paste0("V",as.character(x)))
             df <- do.call(data.frame,temp)
             names(df) <- df.names
             df
          }
          else {
            matrix(unlist(temp),psize(x,parts)[[1]],
                          psize(x,parts)[[2]])
          }
 
      }

   }
})

setMethod("combine",signature(driver="ddR_RDD",items="list"),
  function(driver,items){
    split_indices <- sapply(items,function(x) {
      x@partitions
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

    new("ddR_RDD",RDD=items[[1]]@RDD,partitions = unlist(split_indices), dim = dims, psize = psizes)
  }
)
