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

## Contains the default "Parallel" driver

# Using default "Parallel" as dummy backend for now.
setClass("ParallelDriver", contains = "DDSDriver")

setClass("ParallelDList", contains = "DObject", 
      prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))
setClass("ParallelDArray", contains = "DObject",
prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L))
)
setClass("ParallelDFrame", contains = "DObject",
prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))

setMethod("init",c(x = "ParallelDriver"),
      function(x) print("Init'ing ParallelDriver"))

# Singleton driver
#parallel <- new("ParallelDriver", DListClass = "ParallelDList", DFrameClass = "ParallelDFrame", DArrayClass = "ParallelDArray", backendName = "Parallel (Dummy)")

#dds.env$driver <- parallel
