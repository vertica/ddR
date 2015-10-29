#########################################################
#  File dkmeans.R
#  Distributed version of kmeans 
#
#  This code is a distributed version of kmeans function available in R stats package.
#
#  
#########################################################

.dkmeans.env <- new.env()
.dkmeans.env$iterations_totalTime <- 0

#   X: is a darray of samples. Each row represents a sample.
#   centers: the matrix of centers or their number
#   iter.max: maximum number of iterations
#   nstart: number of tries
#   sampling_threshold: determining between centralized and distributed sampling for choosing random centers
#   trace: boolean, to show the progress
#   na_action: the desired behaviour in the case of infinite data
#   completeModel: when it is FALSE (default), the function does not return cluster label of samples
dkmeans <-
function(X, centers, iter.max = 10, nstart = 1,
        sampling_threshold = 1e6, trace = FALSE, 
	na_action = c("exclude","fail"), completeModel = FALSE)
{
    startTotalTime <- proc.time()

    # validating the input arguments
    if(is.null(X)) stop("'X' is a required argument")
    if(!is.darray(X)) stop("'X' must be a darray")
    nSample <- nrow(X)    # number of samples
    if(is.na(nSample)) stop("invalid nrow(X)")
    p <- ncol(X)    # number of predictors
    if(is.na(p)) stop("invalid ncol(X)")
    if(p != X@psize[1,2]) stop("'X' should be partitioned row-wise")
    if(missing(centers)) stop("'centers' must be a number or a matrix")
    nmeth <- 2
    if(nmeth != 2) stop("The specified algorithm is not supported")
    
    nstart <- as.integer(nstart)
    if(nstart <= 0) stop("nstart should be a positive integer number")
    
    nparts <- totalParts(X)  # number of partitions (blocks)

    switch(match.arg(na_action),
        "fail" = {
            naCheckOutput <- .naCheckKmeans(X, trace=trace, mask = NULL)   
	    anyMiss <- naCheckOutput$found
            if(anyMiss > 0)
                stop(anyMiss, " sample(s) have missed values")
        },
        "exclude" = {
            naCheckOutput <- .naCheckKmeans(X, trace=trace, mask = .createMask(X))
	    anyMiss <- naCheckOutput$found
	    mask <- naCheckOutput$mask
            if(anyMiss > 0) {
                if(anyMiss == nSample)
                    stop("all the samples have missed values")
                else
                    warning(anyMiss, " sample(s) are excluded because of missed value")
            }
        },
        {
            stop("only 'exclude' and 'fail' are valid values for na_action")
        })
    
    initialCenters <- FALSE

    if(length(centers) == 1L) { # the number of centers is given
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

    calculateNorm <- function(Xi, Ni, maski)
	{
	    suppressMessages({
            requireNamespace("kmeans.ddR")
	    })

	    if(!is.null(maski))
		good <- maski > 0
	    else
	        good <- rep(1,nrow(Xi))
            if(class(Xi) == "matrix")
                Ni[good,] <- .Call("calculate_norm", Xi[good,], Ni[good,], PACKAGE="kmeans.ddR")
            else
                Ni[good,] <- .Call("calculate_norm", as.matrix(Xi[good,]), Ni[good,], PACKAGE="kmeans.ddR")
	    return(Ni)
	}

    Norms <- dmapply(calculateNorm, Xi = parts(X), Ni = parts(Norms), 
    	  maski = if(is.null(mask)) rep(NULL,totalParts(X)) else parts(mask),
    	  output.type = "darray",combine = "rbind", nparts = c(totalParts(X),1))

    
    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }

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

    if(completeModel)
        totss <- .d.totss(X, trace, mask)

    if (trace) {
        endTotalTime <- proc.time()
        totalTime <- endTotalTime - startTotalTime
        cat("*****************************\n")
        cat("Total running time:",(totalTime)[3],"sec\n")
        iterationTime = .dkmeans.env$iterations_totalTime / Z$iter
        cat("Running time of each iteration on average:",iterationTime,"sec\n")
    }

    model <- list()
    model$centers = centers
    
    if(completeModel) 
    {
	model$cluster = cluster
	model$totss = totss
	model$withinss = Z$wss
	model$tot.withinss = best
	model$betweenss = totss - best
    }
    else
    {
	model$totss = NA
	model$withinss = NA
	model$tot.withinss = NA
	model$betweenss = NA
    }
    class(model) <- c("dkmeans","kmeans")
    model$size = Z$size
    model$iter = Z$iter
    return(model)
}

## modelled on print methods in the cluster package
print.dkmeans <- function(x, ...)
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

fitted.dkmeans <- function(object, method = c("centers", "classes"), ...)
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

    .calculate_center <- function(Xi, maski)
    	{
		if(is.null(maski))
			maski = rep(1,nrow(Xi))
		matrix(colSums(Xi[maski > 0,]),nrow = 1)
	}
    center_darray <- dmapply(.calculate_center , 
	 Xi=parts(X), maski = parts(mask),
	 output.type = "darray",combine = "rbind",nparts = c(totalParts(X),1))

    center <- colSums(center_darray) 
    if(!is.null(mask))
	center <- center / sum(mask)
    else
	center <- center / nrow(X)


    ss <- dmapply(function(Xi,maski, center) apply(Xi,1,function(xi) sum((xi - center)^2)), 
	 maski = if(is.null(mask)) rep(NULL,totalParts(X)) else parts(mask),
	 Xi=parts(X), MoreArgs = list(center = center),
	output.type = "darray",combine = "rbind",nparts = c(totalParts(X),1))

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
		  Xi = parts(X),output.type = "darray",combine = "rbind",nparts = c(totalParts(X),1))   
	found <- sum(tempArray)

    } else {

      	mask <- dmapply(function(Xi) as.numeric(is.finite(rowSums(Xi))), 
		  Xi = parts(X),output.type = "darray",combine = "rbind",nparts = c(totalParts(X),1))
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
    blockSizes <- psize(X)[,1]
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
            	  }
		return(d.centersi)

	    }

	    d.centers <- dmapply(d.centerDist, Xi = parts(X), idx = 1:nparts, 
	    	      MoreArgs = list(selectedBlocks=selectedBlocks), 
		      output.type = "darray", combine = "rbind", nparts = c(totalParts(X),1))

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
		      output.type = "darray", combine = "rbind", nparts = c(totalParts(X),1))
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
    .dkmeans.env$iterations_totalTime <- 0

    # The main loop for the convergence (mask == NULL)
    for(iter in 1:iter.max) {
            if(trace) {
                cat("Making Clusters; iteration: ",iter,"\n")
                starttime<-proc.time()
            }
	    kmeansFunc <- function(Xi, maski, Ni,centers, nmeth, completeModel)
	    {
	        suppressMessages({
                requireNamespace("kmeans.ddR")
	        })

		if(is.null(maski))
			maski = rep(1,nrow(Xi))
                good <- maski > 0
                m <- as.integer(sum(maski))
		cl = integer(nrow(Xi))
                nc = integer(nrow(centers))
                if(class(Xi) == "matrix")
                    .Call("dkmeans_Lloyd", Xi[good,], Ni[good,], centers, cl, nc, PACKAGE="kmeans.ddR")
                else    # When X is sparse, class(Xi) == "dgCMatrix"
                    .Call("dkmeans_Lloyd", as.matrix(Xi[good,]), Ni[good,], centers, cl, nc, PACKAGE="kmeans.ddR")
		centers[is.nan(centers)] <- 0
		if(completeModel)
			return(list(sumOfClusteri = matrix(centers * nc), 
						  numOfPointsi = matrix(nc), 
						  clusteri =  matrix(cl)))
		return(list(sumOfClusteri = matrix(centers * nc), 
					          numOfPointsi = matrix(nc)))
	    }


	    cluster_info <- dmapply(kmeansFunc, Xi = parts(X), Ni = parts(Norms),
	    		    maski = if(is.null(mask)) rep(NULL,totalParts(X)) else parts(mask),
	    		    MoreArgs = list(centers=centers, nmeth=nmeth, completeModel=completeModel),
			    nparts = totalParts(X))


	    size <- dmapply(function(x){ x$numOfPointsi }, cluster_info,
	    	 output.type="darray", combine = "cbind", nparts = c(1,totalParts(cluster_info)))
	    cent <- dmapply(function(x) x$sumOfClusteri, cluster_info,
	    	 output.type="darray", combine = "cbind", nparts = c(1,totalParts(cluster_info)))

	    size <- rowSums(collect(size))
	    cent <- rowSums(collect(cent))

            newCenters <- matrix(cent,nrow=k,ncol=p) / matrix(size,nrow=k,ncol=p)
            newCenters[size == 0,] <- centers[size == 0,] # some of the clusters might be empty
            if (trace) {    # timing end
                endtime <- proc.time()
                spentTime <- endtime-starttime
                .dkmeans.env$iterations_totalTime <- .dkmeans.env$iterations_totalTime + spentTime[3]
                cat("Spent time:",(spentTime)[3],"sec\n")
            }
            if(all(centers == newCenters))
                break   # converged
            else {
                centers = newCenters
                iteration_counter <- iteration_counter + 1
            }
    }
    # The end of the main loop 

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


	wssFunction<- function(Xi, maski, cluster_infoi, centers = centers)
	{
		suppressMessages({
            	requireNamespace("kmeans.ddR")
	    	})

		if(is.null(maski))
			maski <- rep(1,nrow(Xi))
		good <- maski > 0
		clusteri = cluster_infoi[[1]]$clusteri
		if(class(Xi) == "matrix")
			dwssi <- .Call("calculate_wss", Xi[good,], centers, clusteri, PACKAGE="kmeans.ddR")
		else
			dwssi <- .Call("calculate_wss", as.matrix(Xi[good,]), 
			      centers, clusteri, PACKAGE="kmeans.ddR")	 
		dwssi
	}


	dwss <- dmapply(wssFunction, Xi = parts(X), cluster_infoi = parts(cluster_info), 
	     maski = if(is.null(mask)) rep(NULL,totalParts(X)) else parts(mask),
	     MoreArgs = list(centers = centers),
     	     output.type = "darray", combine = "cbind", nparts = c(1,totalParts(X)))
     

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
	    cluster <- dmapply(function(x) x$clusteri, cluster_info,
	    	 output.type="darray", combine = "rbind", nparts = c(totalParts(cluster_info),1))

    list(cluster=cluster, centers=centers, wss=wss, size=size, iter=iteration_counter)
}

## Applying the cluster centers learned from dkmeans to label samples of a new dataset
# newdata: it is the new dataset. It must be of type either darray or matrix.
# centers: cluster centers that will be used for labeling
# trace: when it is TRUE, displays the progress
predict.dkmeans <- function(object, newdata, trace=FALSE,...) {
  centers <- object$centers
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
    mask <- .createMask(newdata)
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

    # Create a norm darray to fulfil the requirement of "dkmeans_Lloyd"
    if(trace) {
        cat("Calculating the norm of samples\n")
        starttime<-proc.time()
    }
    Norms <- .clone(newdata, ncol=1, sparse=FALSE)
 
    calculateNorm <- function(Xi,maski, Ni)
    {
	suppressMessages({
        requireNamespace("kmeans.ddR")
	})

	if(is.null(maski))
		maski <- rep(1,nrow(Xi))
	good <- maski > 0
        if(class(Xi) == "matrix")
        	.Call("calculate_norm", Xi[good,], Ni[good,], PACKAGE="kmeans.ddR")
        else
		.Call("calculate_norm", as.matrix(Xi[good,]), Ni[good,], PACKAGE="kmeans.ddR")
	Ni
    }

    Norms <- dmapply(calculateNorm, Xi = parts(newdata), Ni = parts(Norms), 
	maski = if(is.null(mask)) rep(NULL,totalParts(newdata)) else parts(mask),
	output.type = "darray", combine = "rbind", nparts = c(totalParts(newdata),1))

    if (trace) {    # timing end
        endtime <- proc.time()
        spentTime <- endtime-starttime
        cat("Spent time:",(spentTime)[3],"sec\n")
    }


    if(trace) {
      cat("Finding labels\n")
      starttime<-proc.time()
    }


    kmeansFunc <- function(xi, maski, Ni, clusteri, centers)
    {
	  suppressMessages({
          requireNamespace("kmeans.ddR")
	  })

	  if(is.null(maski))
		maski <- rep(1,nrow(xi))
          good <- maski > 0
          m <- as.integer(sum(maski))
          cl <- integer(m)
          nc = integer(nrow(centers))
          if(m == 1)
            .Call("dkmeans_Lloyd", matrix(xi[good,],1), matrix(Ni[good,],1), 
	    			     centers, cl, nc, PACKAGE="kmeans.ddR")
          else    
            if(class(xi) == "matrix")
                .Call("dkmeans_Lloyd", xi[good,], Ni[good,], centers, cl, nc, PACKAGE="kmeans.ddR")
            else # When newdata is sparse, class(xi) == "dgCMatrix"
                .Call("dkmeans_Lloyd", as.matrix(xi[good,]), Ni[good,], centers, cl, nc, PACKAGE="kmeans.ddR")
        
          clusteri[good,] <- matrix(cl)
	  
    }

    cluster <- dmapply(kmeansFunc, xi = parts(newdata), Ni = parts(Norms), clusteri = parts(cluster), 
		maski = if(is.null(mask)) rep(NULL,totalParts(newdata)) else parts(mask), 
		MoreArgs = list(centers = centers),
		output.type = "darray", combine = "rbind", nparts = c(totalParts(newdata),1))

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
        .Call("calculate_norm", newdata, normsTemp, PACKAGE="kmeans.ddR")
        .Call("dkmeans_Lloyd", newdata, normsTemp, centersTemp, cl, nc, PACKAGE="kmeans.ddR")
        cluster <- matrix(cl,nrow=nSample, ncol=1)
    } else {
        if(nSample == 1) {
            normsTemp[mask,] <- .Call("calculate_norm", matrix(newdata[mask,],1), 
	    		normsTemp[mask,], PACKAGE="kmeans.ddR")
            .Call("dkmeans_Lloyd", matrix(newdata[mask,],1), normsTemp[mask,], 
	    		centersTemp, cl, nc, PACKAGE="kmeans.ddR")
        } else {
            normsTemp[mask,] <- .Call("calculate_norm", newdata[mask,], 
	    		normsTemp[mask,], PACKAGE="kmeans.ddR")
            .Call("dkmeans_Lloyd", newdata[mask,], normsTemp[mask,], 
	    		centersTemp, cl, nc, PACKAGE="kmeans.ddR")
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



.clone <- function(X, ncol = 1, data = 0,sparse = FALSE)
{
	cloned_obj <- dmapply(function(X,ncol,data) matrix(data,nrow = nrow(X), ncol = ncol), 
		   parts(X), MoreArgs = list(ncol = ncol, data = data),
		   output.type = "darray",combine = "rbind",nparts = c(totalParts(X),1))
}

.createMask <- function(X, data = 0)
{
	dmapply(function(X,data) matrix(data,nrow = nrow(X), ncol = 1), 
		   X = parts(X), MoreArgs = list(data = data),
		   output.type = "darray",combine = "rbind",nparts = c(totalParts(X),1))
}