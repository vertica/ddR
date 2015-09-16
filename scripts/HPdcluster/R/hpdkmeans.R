# Copyright [2013] Hewlett-Packard Development Company, L.P.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

#########################################################
#  File hpdkmeans.R
#  Distributed version of kmeans 
#
#  This code is a distributed version of kmeans function available in R stats package.
#
#  
#########################################################

.hpkmeans.env <- new.env()
.hpkmeans.env$iterations_totalTime <- 0

#   X: is a darray of samples. Each row represents a sample.
#   centers: the matrix of centers or their number
#   iter.max: maximum number of iterations
#   nstart: number of tries
#   algorithm: choice of algorithm
#   sampling_threshold: determining between centralized and distributed sampling for choosing random centers
#   trace: boolean, to show the progress
#   na_action: the desired behaviour in the case of infinite data
#   completeModel: when it is FALSE (default), the function does not return cluster label of samples
hpdkmeans <-
function(X, centers, iter.max = 10, nstart = 1,
#         algorithm = c("Lloyd", "Forgy", "Hartigan-Wong", "MacQueen"), 
        sampling_threshold = 1e6, trace = FALSE, na_action = c("exclude","fail"), completeModel = FALSE)
{
    startTotalTime <- proc.time()
    # loading the so library on all available executors
    #ddyn.load("HPdcluster")
    # unloading the so library from all available executors at the exit time
    #on.exit(ddyn.unload("HPdcluster"))
    a <- dmapply(function(x) library("HPdcluster"), 1:totalParts(X))

    # validating the input arguments
    if(is.null(X)) stop("'X' is a required argument")
    nSample <- nrow(X)    # number of samples
    if(is.na(nSample)) stop("invalid nrow(X)")
    p <- ncol(X)    # number of predictors
    if(is.na(p)) stop("invalid ncol(X)")
    if(p != X@psize[1,2]) stop("'X' should be partitioned row-wise")
    if(missing(centers)) stop("'centers' must be a number or a matrix")
#A    nmeth <- switch(match.arg(algorithm),
#A                    "Hartigan-Wong" = 1,
#A                    "Lloyd" = 2, "Forgy" = 2,
#A                    "MacQueen" = 3)
    nmeth <- 2
    if(nmeth != 2) stop("The specified algorithm is not supported")
    
    nstart <- as.integer(nstart)
    if(nstart <= 0) stop("nstart should be a positive integer number")
    
    nparts <- totalParts(X)  # number of partitions (blocks)

    mask <- NULL
    switch(match.arg(na_action),
        "fail" = {
            naCheckOutput <- .naCheckKmeans(X, trace=trace)   
	    anyMiss <- naCheckOutput$found
            if(anyMiss > 0)
                stop(anyMiss, " sample(s) have missed values")
        },
        "exclude" = {
            mask <- .clone(X, ncol=1, sparse=FALSE)
            naCheckOutput <- .naCheckKmeans(X, trace=trace, mask)
	    anyMiss <- naCheckOutput$found
	    mask <- naCheckOutput$mask
            if(anyMiss > 0) {
                if(anyMiss == nSample)
                    stop("all the samples have missed values")
                else
                    warning(anyMiss, " sample(s) are excluded because of missed value")
            }
            else
                mask <- NULL # to avoid extra overhead
        },
#A        "pass" = {
#A            # do nothing            
#A        },
        {
            stop("only 'exclude' and 'fail' are valid values for na_action")
        }
    )
    
    initialCenters <- FALSE

    if(length(centers) == 1L) { # the number of centers is given
#O        if (centers == 1) nmeth <- 3
        k <- as.integer(centers)
        if(is.na(k)) stop("'invalid value of 'k'")
        if(nSample < k)
            stop("more cluster centers than data points")

        centers <- .pickCenters(X, k, sampling_threshold, trace)

    } else {    # initial centers are given
        if(! all(is.finite(centers))    )
            stop("only finite values for centers are acceptable")
        centers <- as.matrix(centers)
        if(any(duplicated(centers)))
            stop("initial centers are not distinct")
        initialCenters <- TRUE
        k <- nrow(centers)
        if(nSample < k)
            stop("more cluster centers than data points")
    }

    iter.max <- as.integer(iter.max)
    if(is.na(iter.max) || iter.max < 1) stop("'iter.max' must be positive")
    if(ncol(X) != ncol(centers))
        stop("must have same number of columns in 'X' and 'centers'")

    # calculating the norm of all samples for approximating distance (||a|| - ||b|| <= ||a - b||)
    if(trace) {
        cat("Calculating the norm of samples\n")
        starttime<-proc.time()
    }
    Norms <- .clone(X, ncol=1, sparse=FALSE)
    if(is.null(mask)) {

    	calculateNorm <- function(Xi, Ni)
	{
            if(class(Xi) == "matrix")
                .Call("calculate_norm", Xi, Ni, PACKAGE="HPdcluster")
            else
                .Call("calculate_norm", as.matrix(Xi), Ni, PACKAGE="HPdcluster")
	    return(Ni)
	}

    	Norms <- dmapply(calculateNorm, Xi = parts(X), Ni = parts(Norms),
	      output.type = "DArrayClass",combine = "row", nparts = c(totalParts(X),1))


    } else {

    	calculateNorm <- function(Xi, Ni)
	{
            good <- maski > 0
            if(class(Xi) == "matrix")
                Ni[good,] <- .Call("calculate_norm", Xi[good,], Ni[good,], PACKAGE="HPdcluster")
            else
                Ni[good,] <- .Call("calculate_norm", as.matrix(Xi[good,]), Ni[good,], PACKAGE="HPdcluster")
	    return(Ni)
	}

    	Norms <- dmapply(calculateNorm, Xi = parts(X), Ni = parts(Norms),
	      output.type = "DArrayClass",combine = "row", nparts = c(totalParts(X),1))

    }
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }

#O  Z <- do_one(nmeth)
    Z <- .do_oneSet(nmeth, X, Norms, k, centers, iter.max, trace, mask, completeModel=completeModel)
    if(completeModel)
        best <- sum(Z$wss)

    if(nstart >= 2 && !initialCenters && completeModel) {
	    for(i in 2:nstart) {
	        centers <- .pickCenters(X, k, sampling_threshold, trace)
	        ZZ <- .do_oneSet(nmeth, X, Norms, k, centers, iter.max, trace, mask, completeModel=completeModel)
	        if((z <- sum(ZZ$wss)) < best) {
		        Z <- ZZ
		        best <- z
	        }
	    }
    }

    centers <- Z$centers
    dimnames(centers) <- list(1L:k, colnames(X))
    cluster <- Z$cluster
    if(completeModel && !is.null(rn <- rownames(X))) {
        rownames(cluster) <- rn
    }

#O    totss <- sum(scale(x, scale = FALSE)^2)
    if(completeModel)
        totss <- .d.totss(X, trace, mask)

    if (trace) {
        endTotalTime <- proc.time()
        totalTime <- endTotalTime - startTotalTime
        cat("*****************************\n")
        cat("Total running time:",(totalTime)[3],"sec\n")
        iterationTime = .hpkmeans.env$iterations_totalTime / Z$iter
        cat("Running time of each iteration on average:",iterationTime,"sec\n")
    }

    if(completeModel) 
        structure(list(cluster = cluster, centers = centers, totss = totss,
                       withinss = Z$wss, tot.withinss = best,
                       betweenss = totss - best, 
                       size = Z$size, iter = Z$iter),
	              class = c("hpdkmeans","kmeans"))
    else
        structure(list(centers = centers, totss = NA,
                       withinss = NA, tot.withinss = NA,
                       betweenss = NA, 
                       size = Z$size, iter = Z$iter),
	              class = c("hpdkmeans","kmeans"))
}

## modelled on print methods in the cluster package
print.hpdkmeans <- function(x, ...)
{
    cat("K-means clustering with ", length(x$size), " clusters of sizes ",
        paste(x$size, collapse=", "), "\n", sep="")
    cat("\nCluster means:\n")
    print(x$centers, ...)
#O    cat("\nClustering vector:\n")
#O    print(x$cluster, ...)
    if(! is.null(x$withinss)) {
        cat("\nWithin cluster sum of squares by cluster:\n")
        print(x$withinss, ...)
        cat(sprintf(" (between_SS / total_SS = %5.1f %%)\n",
		    100 * x$betweenss/x$totss))
    }
    cat("\nAvailable components:\n")
    print(names(x))
    invisible(x)
}

fitted.hpdkmeans <- function(object, method = c("centers", "classes"), ...)
{
	method <- match.arg(method)
	if (method == "centers") object$centers[object$cl, , drop=FALSE]
	else object$cl
}

##################################################
#        Functions Added by Arash                #
##################################################

## Calculating totss
.d.totss <- function(X, trace = TRUE, mask = NULL)
{
    # validating the input arguments
    if(is.null(X)) stop("'X' is a required argument")
    if(!is.logical(trace)) stop("'trace' should be logical")

    nparts <- nparts(X)
    if(trace) {
        cat("Calculating the total sum of squares\n")
        starttime<-proc.time()
    }

    if(is.null(mask)) {
        center <- colMeans(X, na.rm=TRUE)
	ss <- dmapply(function(Xi,center) apply(Xi,1,function(xi) sum((xi - center)^2)), Xi=parts(X),MoreArgs = list(center = center),
	   output.type = "DArrayClass",combine = "row",nparts = c(totalParts(X),1))
	
    } else { # we need to ignore the entire sample when it contains NA 

        center_darray <- dmapply(function(Xi, maski) colSums(Xi[maski > 0,]) , 
		 Xi=parts(X), maski = parts(mask),
		 output.type = "DArrayClass",combine = "row",nparts = c(totalParts(X),1))
	center <- rowSums(center_darray) / sum(mask)

	ss <- dmapply(function(Xi,maski, center) apply(Xi,1,function(xi) sum((xi - center)^2)), 
	   Xi=parts(X), maski = parts(mask), MoreArgs = list(center = center),
	   output.type = "DArrayClass",combine = "row",nparts = c(totalParts(X),1))
    }
    totss <- sum(ss)
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }

    totss
}

## .naCheckKmeans checks any missed value (NA, NaN, Inf) in X
.naCheckKmeans <- function(X, trace, mask = NULL) {
    nparts <- totalParts(X)

    if (trace) {
        cat("Checking for missed values\n")
        starttime<-proc.time()
    }
    if(is.null(mask)) {

    	tempArray <- dmapply(function(Xi) sum(!is.finite(rowSums(Xi))), 
		  Xi = parts(X),output.type = "DArrayClass",combine = "row",nparts = c(totalParts(X),1))   
	found <- sum(tempArray)

    } else {

      	mask <- dmapply(function(Xi) as.numeric(is.finite(rowSums(Xi))), 
		  Xi = parts(X),output.type = "DArrayClass",combine = "row",nparts = c(totalParts(X),1))
    	found  <- nrow(mask) - sum(mask)
    }

    if (trace) {    # end of timing step
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }
    
    list(found=found,mask=mask)
} 

## Randomly finds centers
.pickCenters <- function (X, k, sampling_threshold, trace) {
    nSample <- nrow(X)    # number of samples
    p <- as.integer(ncol(X))    # number of predictors
    nparts <- totalParts(X)  # number of partitions (blocks)
    blockSizes <- X@psize[,1]
    ## we need to randomly select centers from samples distributed amon partitions of X
    ## all the samples should have the same chance to be picked as a center
    ## we need to avoid duplicates here
    maxIterCenterFinding <- 10
    nCenterAttempts <- 0
    dupplicateCenters <- TRUE
    while(dupplicateCenters && (nCenterAttempts < maxIterCenterFinding)) {
        ## k and p are not expected to be huge

        if ( blockSizes[1] > sampling_threshold || nSample > 1e9) {
            selectedBlocks <- sample.int(nparts, k, replace=TRUE)
            if(trace) {
                cat("Picking randomly selected centers (distributed sampling)\n")
                starttime<-proc.time()
            }

	    d.centerDist <- function(Xi, idx, selectedBlocks) {
	    	nselect <- sum(selectedBlocks == idx)
		d.centersi <- matrix(NA,nrow = 1, ncol = ncol(Xi))
                if(nselect > 0) {
                  index <- sample.int(nrow(Xi), nselect)
                  if(class(Xi) == "matrix")
                      d.centersi <- Xi[index,]
                  else    # When X is sparse, class(Xi) == "dgCMatrix"
                      d.centersi <- as.matrix(Xi[index,])
		  d.centersi
            	  }
	    }

	    d.centers <- dmapply(d.centerDist, Xi = parts(X), idx = 1:nparts, 
	    	      MoreArgs = list(selectedBlocks=selectedBlocks), 
		      output.type = "DArrayClass", combine = "row", nparts = c(totalParts(X),1))

	    centers <- na.omit(collect(d.centers))

            ## As we assume no NA/NaN/Inf in X the number of centers are correct
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
	} else {                                                      # centralized sampling
            indexOfCenters <- sample.int(nSample, k)  # It is not suitable for a big value of nSample
            if(trace) {
                cat("Picking randomly selected centers (centralized sampling)\n")
                starttime<-proc.time()
            }

	    d.centerCent <- function(Xi , idx, indexOfCenters, blockSizes)
	    {
		offset <- 0
                if(idx > 1)
                    offset <- sum(blockSizes[1:(idx -1)])
                index <- (indexOfCenters > offset) & (indexOfCenters <= nrow(Xi) + offset)
		d.centersi <- matrix(NA,nrow = 1, ncol = ncol(Xi))
                if(any(index)) {
                    if(class(Xi) == "matrix")
                        d.centersi <- Xi[indexOfCenters[index] - offset,]
                    else    # When X is sparse, class(Xi) == "dgCMatrix"
                        d.centersi <- as.matrix(Xi[indexOfCenters[index] - offset,])
		}
		return(d.centersi)
	    	
	    }
	    d.centers <- dmapply(d.centerCent, Xi = parts(X), idx = 1:nparts, 
	    	      MoreArgs = list(indexOfCenters = indexOfCenters, blockSizes = blockSizes), 
		      output.type = "DArrayClass", combine = "row", nparts = c(totalParts(X),1))
	    centers <- na.omit(collect(d.centers))


            ## As we assume no NA/NaN/Inf in X the number of centers are correct
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
        }

        d.centers = NULL    # lets GC remove it
        nCenterAttempts <- nCenterAttempts + 1
        # dupplicateCenters becomes TRUE when there is a duplicate or the number of picked centers is smaller than desired
        dupplicateCenters <- any(duplicated(centers)) || (nrow(centers) < k)
    }
    if(dupplicateCenters)
        stop("Unsuccefully tried  ", sQuote(maxIterCenterFinding), 
            " times to randomly pick distinct centers. Please specify appropriate centers manually.")
    centers

}

## Main clustersing function (a complete set of iterations for given centers)
.do_oneSet <- function (nmeth, X, Norms, k, centers, iter.max, trace, mask = NULL, completeModel=FALSE) {
    nSample <- nrow(X)    # number of samples
    p <- as.integer(ncol(X))    # number of features
    nparts <- totalParts(X)  # number of partitions (blocks)

    iteration_counter <- 1
    .hpkmeans.env$iterations_totalTime <- 0

    if(is.null(mask)) {
        # The main loop for the convergence (mask == NULL)
        for(iter in 1:iter.max) {
            if(trace) {
                cat("Making Clusters; iteration: ",iter,"\n")
                starttime<-proc.time()
            }

	    kmeansFunc <- function(Xi, Ni,centers, nmeth, completeModel)
	    {
		cl = integer(nrow(Xi))
                nc = integer(nrow(centers))
                if(class(Xi) == "matrix")
                    .Call("hpdkmeans_Lloyd", Xi, Ni, centers, cl, nc, PACKAGE="HPdcluster")
                else    # When X is sparse, class(Xi) == "dgCMatrix"
                    .Call("hpdkmeans_Lloyd", as.matrix(Xi), Ni, centers, cl, nc, PACKAGE="HPdcluster")
		centers[is.nan(centers)] <- 0
		if(completeModel)
			return(list(sumOfClusteri = matrix(centers * nc), numOfPointsi = matrix(nc), clusteri =  matrix(cl)))
		return(list(sumOfClusteri = matrix(centers * nc), numOfPointsi = matrix(nc)))
	    }

	    clustering_info <- dmapply(kmeansFunc, Xi = parts(X), Ni = parts(Norms),
	    		    MoreArgs = list(centers=centers, nmeth=nmeth, completeModel=completeModel),
			    nparts = totalParts(X))

	    size <- dmapply(function(x){ x$numOfPointsi }, clustering_info,
	    	 output.type="DArrayClass", combine = "col", nparts = c(1,totalParts(clustering_info)))
	    cent <- dmapply(function(x) x$sumOfClusteri, clustering_info,
	    	 output.type="DArrayClass", combine = "col", nparts = c(1,totalParts(clustering_info)))
	    size <- rowSums(size)
	    cent <- rowSums(cent)
  

            newCenters <- matrix(cent,nrow=k,ncol=p) / matrix(size,nrow=k,ncol=p)
            newCenters[size == 0,] <- centers[size == 0,] # some of the clusters might be empty
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                .hpkmeans.env$iterations_totalTime <- .hpkmeans.env$iterations_totalTime + spentTime[3]
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
            if(all(centers == newCenters))
                break   # converged
            else {
                centers = newCenters
                iteration_counter <- iteration_counter + 1
            }
        }
        # The end of the main loop (mask == NULL)
    } else {
        # The main loop for the convergence (mask != NULL)
        for(iter in 1:iter.max) {
            if(trace) {
                cat("Making Clusters\n")
                starttime<-proc.time()
            }

	    kmeansFunc <- function(Xi, maski, Ni,centers, nmeth, completeModel)
	    {
                good <- maski > 0
                m <- as.integer(sum(maski))
		cl = integer(nrow(Xi))
                nc = integer(nrow(centers))
                if(class(Xi) == "matrix")
                    .Call("hpdkmeans_Lloyd", Xi[good,], Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
                else    # When X is sparse, class(Xi) == "dgCMatrix"
                    .Call("hpdkmeans_Lloyd", as.matrix(Xi[good,]), Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
		centers[is.nan(centers)] <- 0
		if(completeModel)
			return(list(sumOfClusteri = centers * nc, numOfPointsi = nc, clusteri =  cl))
		return(list(sumOfClusteri = centers * nc, numOfPointsi = nc))
	    }
	    clustering_info <- dmapply(kmeansFunc, Xi = parts(X), maski = parts(mask), Ni = parts(Norms),
	    		    MoreArgs = list(centers=centers, nmeth=nmeth, completeModel=completeModel))

	    size <- dmapply(function(x) x$numOfPointsi, clustering_info,
	    	 output.type="DArrayClass", combine = "col", nparts = c(1,totalParts(clustering_info)))
	    cent <- dmapply(function(x) x$sumOfClusteri, clustering_info,
	    	 output.type="DArrayClass", combine = "col", nparts = c(1,totalParts(clustering_info)))
	    size <- rowSums(size)
	    cent <- rowSums(cent)


            newCenters <- matrix(cent,nrow=k,ncol=p) / matrix(size,nrow=k,ncol=p)
            newCenters[size == 0,] <- centers[size == 0,] # some of the clusters might be empty
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                .hpkmeans.env$iterations_totalTime <- .hpkmeans.env$iterations_totalTime + spentTime[3]
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
            if(all(centers == newCenters))
                break   # converged
            else {
                centers = newCenters
                iteration_counter <- iteration_counter + 1
            }
        }
        # The end of the main loop (mask != NULL)
    }

    if(iteration_counter > iter.max)
        warning(sprintf(ngettext(iter.max, "did not converge in %d iteration",
            "did not converge in %d iterations"), iter.max), call.=FALSE, domain = NA)
    if(any(size == 0))
        warning("empty cluster: try a better set of initial centers", call.=FALSE)

    if(completeModel) {
        #Create a darray for wss
        if(trace) {
            cat("Calculating wss\n")
            starttime<-proc.time()
        }


        if(is.null(mask)) {

		wssFunction<- function(Xi, cluster_infoi, centers = centers)
		{
			clusteri = cluster_infoi$clusteri
			if(class(Xi) == "matrix")
				dwssi <- .Call("calculate_wss", Xi, centers, clusteri, PACKAGE="HPdcluster")
			else
				dwssi <- .Call("calculate_wss", as.matrix(Xi), centers, clusteri, PACKAGE="HPdcluster")
	    		dwssi
		}

		dwss <- dmapply(wssFunction, parts(X), parts(cluster_info), MoreArgs = list(centers = centers),
	     	     output.type = "DArrayClass", combine = "col", nparts = c(1,totalParts(X)))
        } else {

		wssFunction<- function(Xi, maski, cluster_infoi, centers = centers)
		{
			good <- maski > 0
			clusteri = cluster_infoi$clusteri
			if(class(Xi) == "matrix")
				dwssi <- .Call("calculate_wss", Xi[good,], centers, clusteri, PACKAGE="HPdcluster")
			else
				dwssi <- .Call("calculate_wss", as.matrix(Xi[good,]), centers, clusteri, PACKAGE="HPdcluster")
	    		dwssi
		}

		dwss <- dmapply(wssFunction, parts(X), parts(mask), parts(cluster_info), MoreArgs = list(centers = centers),
	     	     output.type = "DArrayClass", combine = "col", nparts = c(1,totalParts(X)))
        }

        wss <- rowSums(dwss)
        if (trace) {    # timing end
            endtime <- proc.time()
            spentTime <- endtime-starttime
            cat("Spent time:",(spentTime)[3],"sec\n")
        }
    } else # if(completeModel)
        wss <- NA

	cluster <- NULL
	if(completeModel)
	    cluster <- dmapply(function(x) x$clusteri, clustering_info,
	    	 output.type="DArrayClass", combine = "row", nparts = c(totalParts(clustering_info),1))

    list(cluster=cluster, centers=centers, wss=wss, size=size, iter=iteration_counter)
}

## Applying the cluster centers learned from hpdkmeans to label samples of a new dataset
# newdata: it is the new dataset. It must be of type either darray or matrix.
# centers: cluster centers that will be used for labeling
# trace: when it is TRUE, displays the progress
hpdapply <- function(newdata, centers, trace=FALSE) {
  darrayInput <- TRUE
  if(!is.darray(newdata)) {
    if(is.matrix(newdata) && is.numeric(newdata)) {
      darrayInput <- FALSE
    } else {
      stop("newdata must be of type either darray or numeric matrix")
    }
  }

  if(!is.matrix(centers) || !is.numeric(centers))
    stop("centers must be a numeric matrix")
  if(ncol(newdata) != ncol(centers))
    stop("newdata and centers should have the same number of features")

  nSample <- nrow(newdata)    # number of samples
  p <- as.integer(ncol(newdata))    # number of features

  if(darrayInput) { # newdata is a darray
    nparts <- totalParts(newdata)  # number of partitions (blocks)
    if(p != newdata@psize[1,2]) stop("newdata should always be row-wise partitioned")

    # samples with missed values should not be used
    mask <- .clone(newdata, ncol=1, sparse=FALSE)
    naCheckOutput <- .naCheckKmeans(newdata, trace=trace, mask)
    anyMiss <- naCheckOutput$found
    mask <- naCheckOutput$mask
    if(anyMiss > 0) {
        if(anyMiss == nSample)
            stop("all the samples have missed values")
        else
            warning(anyMiss, " sample(s) are excluded because of missed value")
    } else
        mask <- NULL # to avoid extra overhead

    #Create an array that maps points to their cluster labels
    cluster <- .clone(newdata, ncol=1, data=NA, sparse=FALSE)

    # Create a norm darray to fulfil the requirement of "hpdkmeans_Lloyd"
    if(trace) {
        cat("Calculating the norm of samples\n")
        starttime<-proc.time()
    }
    Norms <- .clone(newdata, ncol=1, sparse=FALSE)
    if(is.null(mask)) {

        calculateNorm <- function(Xi,Ni){
            if(class(Xi) == "matrix")
                .Call("calculate_norm", Xi, Ni, PACKAGE="HPdcluster")
            else
                .Call("calculate_norm", as.matrix(Xi), Ni, PACKAGE="HPdcluster")
	    Ni
	}

    	Norms <- dmapply(calculateNorm, parts(X), parts(Norms), 
	      output.type = "DArrayClass", combine = "row", nparts = c(totalParts(X),1))
    } else {
 
        calculateNorm <- function(Xi,maski, Ni){
	    good <- maski > 0
            if(class(Xi) == "matrix")
                .Call("calculate_norm", Xi[good,], Ni[good,], PACKAGE="HPdcluster")
            else
                .Call("calculate_norm", as.matrix(Xi[good,]), Ni[good,], PACKAGE="HPdcluster")
	    Ni
	}

    	Norms <- dmapply(calculateNorm, parts(X), parts(Norms), 
	      output.type = "DArrayClass", combine = "row", nparts = c(totalParts(X),1))
    }
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }


    if(trace) {
      cat("Finding labels\n")
      starttime<-proc.time()
    }
    if(is.null(mask)) { # there is no missed value in newdata


    	kmeansFunc <- function(xi, Ni, clusteri, centers)
	{
          cl = integer(nrow(xi))
          nc = integer(nrow(centers))
          if(class(xi) == "matrix")
            .Call("hpdkmeans_Lloyd", xi, Ni, centers, cl, nc, PACKAGE="HPdcluster")
          else    # When newdata is sparse, class(xi) == "dgCMatrix"
            .Call("hpdkmeans_Lloyd", as.matrix(xi), Ni, centers, cl, nc, PACKAGE="HPdcluster")
        
          matrix(cl)
	}

	cluster <- dmapply(kmeansFunc, parts(newdata), parts(Ni), parts(cluster), MoreArgs = list(centers = centers),
		output.type = "DArrayClass", combine = "row", nparts = c(totalParts(X),1))

    } else { # some of the samples should be ignored because of missed values


    	kmeansFunc <- function(xi, maski, Ni, clusteri, centers)
	{
          good <- maski > 0
          m <- as.integer(sum(maski))
          cl <- integer(m)
          nc = integer(nrow(centers))
          if(m == 1)
            .Call("hpdkmeans_Lloyd", matrix(xi[good,],1), matrix(Ni[good,],1), centers, cl, nc, PACKAGE="HPdcluster")
          else    
            if(class(xi) == "matrix")
                .Call("hpdkmeans_Lloyd", xi[good,], Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
            else # When newdata is sparse, class(xi) == "dgCMatrix"
                .Call("hpdkmeans_Lloyd", as.matrix(xi[good,]), Ni[good,], centers, cl, nc, PACKAGE="HPdcluster")
        
          clusteri[good,] <- matrix(cl)
	  
	}

	cluster <- dmapply(kmeansFunc, parts(newdata), parts(mask), parts(Ni), parts(cluster), MoreArgs = list(centers = centers),
		output.type = "DArrayClass", combine = "row", nparts = c(totalParts(X),1))

    }
    if (trace) {    # timing end
      endtime <- proc.time()
      spentTime <- endtime-starttime
      cat("Spent time:",(spentTime)[3],"sec\n")
    }
  } else { # newdata is a matrix

    if(trace) {
      cat("Finding labels\n")
      starttime <- proc.time()
    }
    # samples with missed values should not be used
    mask <- is.finite(rowSums(newdata))
    missingSamples <- sum(!mask)
    if(missingSamples == 0)  mask <- NULL # avoid extra overhead of mask
    else {
        cluster <- matrix(NA, nrow=nSample, ncol=1)
        nSample <- nSample - missingSamples
        if(nSample == 0)   stop("all the samples in newdata contain missed values")
    }

    centersTemp <- matrix(centers, nrow=nrow(centers)) # the copy will be modified by the function
    normsTemp <- matrix(0, nrow=nrow(newdata), ncol=1) # a norm matrix for the newdata

    cl = integer(nSample)
    nc = integer(nrow(centers))
    if(missingSamples == 0) {
        .Call("calculate_norm", newdata, normsTemp, PACKAGE="HPdcluster")
        .Call("hpdkmeans_Lloyd", newdata, normsTemp, centersTemp, cl, nc, PACKAGE="HPdcluster")
        cluster <- matrix(cl,nrow=nSample, ncol=1)
    } else {
        if(nSample == 1) {
            normsTemp[mask,] <- .Call("calculate_norm", matrix(newdata[mask,],1), normsTemp[mask,], PACKAGE="HPdcluster")
            .Call("hpdkmeans_Lloyd", matrix(newdata[mask,],1), normsTemp[mask,], centersTemp, cl, nc, PACKAGE="HPdcluster")
        } else {
            normsTemp[mask,] <- .Call("calculate_norm", newdata[mask,], normsTemp[mask,], PACKAGE="HPdcluster")
            .Call("hpdkmeans_Lloyd", newdata[mask,], normsTemp[mask,], centersTemp, cl, nc, PACKAGE="HPdcluster")
        }
        cluster[mask,1] <- cl
    }
    if (trace) {    # timing end
      endtime <- proc.time()
      spentTime <- endtime-starttime
      cat("Spent time:",(spentTime)[3],"sec\n")
    }
  }

  cluster
}

## A supplementary function for deployment
# inputModel: it is the model that is going to be prepared for deployment
deploy.hpdkmeans <- function(inputModel) {
    if(is.null(inputModel$centers))
        stop("the model does not contain centers and cannot be used for prediction")
    distributed_objects <- sapply(inputModel, is.darray)
    if(any(distributed_objects))
        inputModel <- inputModel[-(which(distributed_objects, arr.ind=TRUE))]
    class(inputModel) <- c("hpdkmeans","kmeans")
    inputModel
}




.clone <- function(X, ncol = 1, data = 0,sparse = FALSE)
{
	cloned_obj <- dmapply(function(X,ncol,data) matrix(data,nrow = nrow(X), ncol = ncol), 
		   parts(X), MoreArgs = list(ncol = ncol, data = data),
		   output.type = "DArrayClass",combine = "row",nparts = c(totalParts(X),1))
}