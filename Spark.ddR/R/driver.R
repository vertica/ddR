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

#' @import methods ddR 
#' @importFrom SparkR sparkR.init sparkR.stop 

Spark.ddR.env <- new.env()

# Create distributedR ddRDriver
setClass("SparkRddR", contains="ddRDriver")

#' @export 
# Exported Driver
SparkR <- new("SparkRddR",DListClass = "ddR_RDD",DFrameClass = "ddR_RDD",DArrayClass = "ddR_RDD",backendName = "SparkR")

#' @export
setMethod("init","SparkRddR",
  function(x,...) {
    message("Backend switched to SparkR. Initializing the Spark context...")
    Spark.ddR.env$context <- sparkR.init()
    ## TODO(etduwx): return the actual number of executors
    return (1)
  }
)

#' @export
setMethod("shutdown","SparkRddR",
  function(x) {
    message("Switching out of using SparkR. Shutting it down...")
    sparkR.stop()
  }
)

#' @export
setMethod("do_dmapply",
           signature(driver="SparkRddR",func="function"),
           function(driver,func,...,MoreArgs=list(),
                    output.type =
                      c("dlist","dframe","darray","sparse_darray"),
                    nparts=NULL,
                    combine=c("default","c","rbind","cbind")) {

  ## Placeholder
  rdd <- SparkR:::parallelize(Spark.ddR.env$context, 1:10, prod(nparts))
 
  if(output.type == "dlist") num.dims <- 1
  else num.dims <- 2

  new("ddR_RDD", RDD = rdd, nparts=nparts, psize=matrix(1,prod(nparts), num.dims), partitions = 1:prod(nparts))
})
