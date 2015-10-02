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

# Override dimnames, colnames, and rownames to use the Distributed R package implementations
#' @export
setReplaceMethod("dimnames", signature(x = "DistributedRObj", value = "list"), definition = function(x,value) {
  dimnames(x@DRObj) <- value
  x
})

#' @export
setMethod("dimnames", "DistributedRObj",
  function(x) {
    if(is.dlist(x)) stop("Cannot use dimnames on a DList. Use names() instead.")
    dimnames(x@DRObj)
})

#' @export
setMethod("rownames", "DistributedRObj",
  function(x) {
    if(is.dlist(x)) stop("Cannot use rownames on a DList. Use names() instead.")
    dimnames(x@DRObj)[[1]]
})

#' @export
setMethod("colnames", "DistributedRObj",
  function(x) {
    if(is.dlist(x)) stop("Cannot use colnames on a DList. Use names() instead.")
    dimnames(x@DRObj)[[2]]
})
