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

# Current driving backend package. Package global variable
dds.env <- new.env(emptyenv())
#Track no. of executors in the backend
dds.env$nexecutors <- 1

#' Sets the active backend driver. Functions exported by the 'dds' package 
#' are dispatched to the backend driver.
#' Backend-specific initialization parameters may be passed into the ellipsis (...) part of the 
#' function arguments. 
#'
#' The default driver uses R's 'parallel' as the backend.
#'
#' @param driver driver object for the backend that will be used. This object should extend class 'DDSDriver', and the S4 methods for do_dmapply, do_collect, and get_parts should be defined in the class of the driver object. 
#' @param ... additional parameters to pass to the initialization function of the driver.
#' @details
#' After successfully registering a new backend with useBackend(), all subsequent dmapply, collect, and parts operations will dispatch on that driver object's class. Note that distributed objects created with a different backend prior to switching will be incompatible with these backend-specific functions of the new driver.
#' @references 
#' Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu, V., Hsu, M., Roy, I. 
#' Large scale predictive analytics in Vertica: Fast data transfer, distributed model creation 
#' and in-database prediction. _Sigmod 2015_, 1657-1668.
#'
#' Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and
#' Schreiber, R. (2013) Presto: Distributed Machine Learning and
#' Graph Processing with Sparse Matrices. _EuroSys 2013_, 197-210.
#'
#' Homepage: https://github.com/vertica/DistributedR
#' @examples
#' \dontrun{
#' useBackend(parallel,inst=2)
#' library(distributedR.dds); useBackend(distributedR)
#' } 
#' @export
useBackend <- function(driver, ...) {

  if(!is.null(dds.env$driver)) shutdown(dds.env$driver)

  if(!extends(class(driver)[[1]],"DDSDriver")) stop("Invalid driver object specified")

  if(!extends(driver@DListClass,"DObject")) stop("The driver DList class does not extend DDS::Dobject")
  if(!extends(driver@DFrameClass,"DObject")) stop("The driver DFrame class does not extend DDS::Dobject")
  if(!extends(driver@DArrayClass,"DObject")) stop("The driver DArray class does not extend DDS::Dobject")

  nexecutors<-init(driver, ...)
  if(!is.null(nexecutors) && is.numeric(nexecutors) && (nexecutors > 1)) {dds.env$nexecutors <- nexecutors}
      
  dds.env$driver <- driver
}

#' @export
setClass("DDSDriver", representation(DListClass = "character", DFrameClass = "character", DArrayClass = "character", backendName = "character"))

#' @export
setGeneric("init", function(x,...) {
  standardGeneric("init")
}) 

#' @export
setGeneric("shutdown", function(x) {
  standardGeneric("shutdown")
}) 

#' @export
setMethod("init","DDSDriver",
  function(x,...) {
    message(paste0("Activating the ",x@backendName," backend."))
  }
)

#' @export
setMethod("shutdown","DDSDriver",
  function(x) {
    message(paste0("Deactivating the ",x@backendName," backend."))
  }
)

#' @export
# dispatches on DDSDriver
setGeneric("do_dmapply", function(driver,func,...,MoreArgs=list(),output.type="dlist",nparts=NULL,combine="flatten") {
   standardGeneric("do_dmapply")
})

#' @export
# dispatches on DDSDriver
setGeneric("combine", function(driver,items) {
  standardGeneric("combine")
})

#' @export
# dispatches on backend
setGeneric("get_parts", function(x,index,...) {
  standardGeneric("get_parts")
})

#' @export
setGeneric("do_collect", function(x,parts) {
  standardGeneric("do_collect")
})

#' Distributed version of 'lapply'. Similar to \code{\link{dmapply}}, but permits only one iterable argument, and output.type is 
#' always 'dlist'. 
#'
#' @param X vector, matrix, list, data.frame, dlist, darray, or dframe or other iterable object to supply to the function in FUN.
#' @param FUN the function to be applied to each element of ‘X’.
#' @param ... optional arguments to 'FUN'.
#' @param nparts number of partitions in the output dlist.
#' @return a dlist with number of partitions specified in 'nparts'
#' @references 
#' Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu, V., Hsu, M., Roy, I. 
#' Large scale predictive analytics in Vertica: Fast data transfer, distributed model creation 
#' and in-database prediction. _Sigmod 2015_, 1657-1668.
#'
#' Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and
#' Schreiber, R. (2013) Presto: Distributed Machine Learning and
#' Graph Processing with Sparse Matrices. _EuroSys 2013_, 197-210.
#'
#' Homepage: https://github.com/vertica/DistributedR
#' @examples
#' \dontrun{
#' a <- dlapply(1:5,function(x) x, nparts=3) # A DList with 3 partitions, which in the aggregate contains the elements 1 through 5.
#' b <- dlapply(a,function(x) x+3) # Adds 3 to each element of dlist a.
#' } 
#' @export
dlapply <- function(X,FUN,...,nparts=NULL) {
   dmapply(FUN,X,MoreArgs=list(...),output.type="dlist",nparts=nparts)
}

#' Distributed version of mapply. Similar to R's 'mapply', it allows a multivariate function, FUN, to be applied to several inputs. Unlike standard mapply, it always returns a distributed object.
#'
#' Though dmapply is modeled after mapply, there are several important differences, as evident in the parameters described below.
#'
#' @param FUN function to apply, found via ‘match.fun’.
#' @param ...  arguments to vectorize over (vectors or lists of strictly positive length, or all of zero length). These may also be distributed objects, such as dlists, darrays, and dframes.
#' @param MoreArgs a list of other arguments to ‘FUN’.
#' @param output.type the output type of the distributed object. The default value of "dlist" means that the result of dmapply will be stored in a distributed list. "darray" will make dmapply return a darray, just as "dframe" will make it return a dframe. "sparse_darray" results in a special version of darray where the elements are sparse.
#' @param nparts a 1d or 2d numeric value to specify how the output should be partitioned. dlists only have one-dimensional partitioning, whereas darrays and dframes have two (representing the number partitions across the vertical and horizontal dimensions). 
#' @param combine for dframes and darrays, it specifies how the results of dmapply are combined within each partition (if each partition contains more than one result). If "row", the results are stitched using rbind; if "col", cbind is used. If the value is "flatten", the results are flattened into one column, as is the case with simplify2array(). For dlists, "unlist" will first attempt to unlist each element of the dmapply result and then expand these items within the partition of the dlist. The default value is "flatten".
#' @return A dlist, darray, or dframe (depending on the value of output.type), with number of partitions equal to \code{\link{nparts}}
#' @references 
#' Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu, V., Hsu, M., Roy, I. 
#' Large scale predictive analytics in Vertica: Fast data transfer, distributed model creation 
#' and in-database prediction. _Sigmod 2015_, 1657-1668.
#'
#' Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and
#' Schreiber, R. (2013) Presto: Distributed Machine Learning and
#' Graph Processing with Sparse Matrices. _EuroSys 2013_, 197-210.
#'
#' Homepage: https://github.com/vertica/DistributedR
#' @examples
#' \dontrun{
#' ## A dlist created by adding two input vectors
#' a <- dmapply(function(x,y) x+y, 1:5, 2:6, nparts=3) 
#' collect(a)
#'
#' ##Create a darray with 4 partitions. Partitions are stitched in 2x2 fashion, meaning the overall dims of the darray will be 4x4.
#' b <- dmapply(function(x) matrix(x,2,2), 1:4,output.type="darray",combine="row",nparts=c(2,2)) 
#' collect(b,1) #First partition
#' collect(b)
#' }
#' @export
dmapply <- function(FUN,...,MoreArgs=list(),output.type="dlist",nparts=NULL,combine="flatten") {
  if(!is.function(FUN)) stop("FUN needs to be a function")
  
  # Allow for multiple ways of expressing output.type
  da_types <- c("da","darray","darrayclass","daclass","a","matrix","dmatrix","array","darr")
  dl_types <- c("dl","l","dlist","dlistclass","list","dlclass")
  df_types <- c("df","f","dframe","dframeclass","data.frame","dataframe")
  sp_types <- c("sparse darray", "sparse_darray","sparsedarray","sda","sdarray","sparse","sparsearray","sarray","sparsematrix","smatrix","s","sp")

  if(tolower(output.type) %in% da_types) output.type <- "darray"
  else if (tolower(output.type) %in% dl_types) output.type <- "dlist"
  else if (tolower(output.type) %in% df_types) output.type <- "dframe"
  else if (tolower(output.type) %in% sp_types) output.type <- "sparse_darray"

  accepted_types <- c("dlist","darray","dframe","sparse_darray")

  if(!(output.type %in% accepted_types))
    stop("Unrecognized value for output.type -- try one of: {'dlist', 'darray', 'dframe', 'sparse_darray'}.")

  if(!is.null(nparts))
    if(!is.numeric(nparts) || length(nparts) < 1 || length(nparts) > 2) 
      stop("Invalid nparts vector provided. Must be a 1d or 2d vector")

  # Allow for multiple ways of expressing combine
  flatten_types <- c("flatten","flat","default","f")
  row_types <- c("row","r","rbind")
  col_types <- c("col","column","c","cbind")

  if(tolower(combine) %in% flatten_types) combine <- "flatten"
  else if (tolower(combine) %in% row_types) combine <- "row"
  else if (tolower(combine) %in% col_types) combine <- "col"

  accepted_combine <- c("flatten","row","col","unlist")

  if(!(combine %in% accepted_combine))
    stop("Unrecognized option for combine -- try one of: {'flatten', 'row', 'col'}")
  
  dargs <- list(...)

  if(length(dargs) == 0) stop("Need to supply at least one iterable item for the function.")

  # Ensure that ... arguments are of equal length. length() works correctly for data.frame,
  # arrays, and lists
  lens <- vapply(dargs,function(x) {
     if(is(x,"DObject") && x@backend != dds.env$driver@backendName)
       stop(paste0("An argument passed in was created with 
            backend '",x@backend,"'; the currently loaded backend is '",
            dds.env$driver@backendName,"'."))
        
     length(x)
   },FUN.VALUE=numeric(1))

  if(max(lens) != min(lens)) stop("The lengths of your iterable arguments need to be equal.")
    
  partitioning <- getBestOutputPartitioning(dds.env$driver,...,nparts=nparts,type=output.type)

  # simplify2array does not work well on data.frames, default to column instead
  if(output.type == "dframe" && combine == "flatten") combine = "col"
  if(output.type == "sparse_darray" && combine != "col" && combine != "row") 
    stop("sparse_darray outputs must have either 'row' or 'col' for combine")

  newobj <- do_dmapply(dds.env$driver, func=match.fun(FUN), ..., MoreArgs=MoreArgs,
                       output.type=output.type,nparts=partitioning,combine=combine)                       

  checkReturnObject(partitioning,newobj)

  newobj@backend <- dds.env$driver@backendName
  newobj@type <- output.type 

  newobj
}

# Check object returned by backend
checkReturnObject <- function(partitioning,result) {
  if(!identical(partitioning,nparts(result))) 
    stop("The backend returned a result with partitioning that does not match what is expected.")
}

# Given a list of arguments into dmapply, return a dobject
# whose partitioning scheme we want to enforce the output to have
# this should be used by do_dmapply as well
#' @export
getBestOutputPartitioning <- function(driver,...,nparts=NULL,type=NULL) {
  UseMethod("getBestOutputPartitioning")
}

# This has been recently changed to returning the "best" nparts configuration

# Currently, we naively choose the first DObject argument we find,
# or just use the length of the input arguments if none are found
# (i.e., when parts() is used for all args)
#' @export
getBestOutputPartitioning.DDSDriver <- function(driver, ...,nparts=NULL,type=NULL) {

  margs <- list(...)
  # Change this value when we want to change default
  # TODO(etduwx): determine proper number. 
  # Should be either 1 or length or arguments?
  defaultNoOfPartitions <- length(margs[[1]])

  # if nparts is NULL, we run our heuristic algorithm here
  if(is.null(nparts)) {
    # first we prioritize matching nparts of one (the first) dobject, if any are dobjects
    for(i in seq(length(margs))) {
      if(is(margs[[i]],"DObject")) { 
        nparts  <- totalParts(margs[[i]])
        break
      }
    }
  }   

  # or if none are found, then we resort to making nparts == length of first argument
  
  # default to cbinding partitions if 2D
  # # TODO(etduwx): Assert that user-provided nparts (if any) is valid and acceptable.
  # If not, throw an error. Alternatively, resort to defaultValue as below:
  # if(error(nparts) || is.null(nparts)) nparts <- defaultNoOfPartitions
  if(is.null(nparts)) nparts <- defaultNoOfPartitions 

  if(length(nparts) == 1) {
    # default to cbinding partitions if 2D
    if(type == "dlist")
      nparts <- c(nparts, 1L)
    else
      nparts <- c(1L,nparts)
  }

  nparts 
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nWelcome to 'dds' (Distributed Data-structures)!\nFor more information, visit: https://github.com/vertica/Standard-R-API")
}
