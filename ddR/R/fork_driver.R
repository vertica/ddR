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

#' @include ddR.R
NULL


setClass("fork.ddR", contains = "ddRDriver")


init_fork <- function(executors="all", ...){

    if(executors == "all"){
        executors <- parallel::detectCores()
    }
    executors <- as.integer(executors)

    new("fork.ddR",
        DListClass = "ParallelObj",
        DFrameClass = "ParallelObj",
        DArrayClass = "ParallelObj",
        name = "fork",
        executors = executors
        )
}

# `fork` doesn't need a shutdown method


setMethod("do_dmapply", signature = c(driver = "fork.ddR"),
function(driver, func, MoreArgs, dots)
{
   allargs <- c(list(FUN = func, MoreArgs = MoreArgs, SIMPLIFY = FALSE,
             mc.cores = driver@executors), dots)
   answer <- do.call(parallel::mcmapply, allargs)
   answer
})
