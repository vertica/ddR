#########################################################
#  File dpagerank.R
#  Distributed version of pagerank 
#
#  This code is a distributed version of pagerank function.
#
#  
#########################################################

#   dgraph: A darray which represents an adjacency matrix of a graph.
#   niter: The maximum number of iterations
#   eps: The calculation is considered as complete if the difference of PageRank values between iterations change less than this value for every vertex.
#   damping: The damping factor
#   personalized: Optional personalization vector (of type darray). When it is NULL, a constant value of 1/N will be used where N is the number of vertices.
#   weights: Optional edge weights (of type darray). When it is NULL, a constant value of 1 will be used.
#   trace: boolean, to show the progress
#   na_action: the desired behaviour in the case of infinite data in dgraph
dpagerank <- function(dgraph, niter = 1000, eps = 0.001, damping=0.85, personalized=NULL, weights=NULL, 
                        trace=FALSE, na_action = c("pass","exclude","fail")) {
  startTotalTime <- proc.time()

  ### Validating the inputs
  if (trace) {
    cat("Validating the inputs\n")
    starttime<-proc.time()
  }
  ### Argument checks
  if (!is.darray(dgraph)) stop("dgraph argument must be of type darray")
  # if (is.invalid(dgraph)) stop("'dgraph' should not be an empty darray")
  nVertices <- nrow(dgraph) # number of vertices in the graph
  if (nVertices != ncol(dgraph)) { stop("The input adjacency matrix must be square") }
  if (nparts(dgraph)[1] != 1) { stop("The input adjacency matrix must be partitioned column-wise") }
  # Checking missed value
  switch(match.arg(na_action),
      "fail" = {
          anyMiss <- .naCheckPageRank(dgraph, trace)$found
          if(anyMiss > 0)
              stop("missing values in dgraph")
      },
      "exclude" = {
          anyMiss <- .naCheckPageRank(dgraph, trace, cover=TRUE)
          if(anyMiss$found > 0) {
              warning(anyMiss, " edges are excluded because of missed value")
              dgraph <- anyMiss$X
          }
      },
      "pass" = {
          # do nothing    
      },
      {
          stop("only 'pass', 'exclude', and 'fail' are valid values for na_action")
      }
  )
  # Checking personalized
  if (!is.null(personalized)) {
      if(!is.darray(personalized)) stop("personalized argument must be a darray")
      if(personalized@type == "sparse_darray") stop("personalized argument must be a dense darray")
      if(any(dim(personalized) != c(1,nVertices)) || (nparts(personalized)[2] != nparts(dgraph)[2]) || 
                                                    any(psize(personalized)[,2] != psize(dgraph)[,2]) )
        stop("dimensions and partitions in personalized should be compatible with dgraph")
      if(.naCheckPageRank(personalized, trace)$found > 0) stop("missed values in personalized darray!") 
  }
  # Checking weights
  if (!is.null(weights)) {
    if(!is.darray(weights)) stop("weights argument must be a darray")
    if(weights@type != dgraph@type) stop("weights should be similar to dgraph for sparse feature")
    if(any(dim(weights) != dim(dgraph)) || any(psize(weights) != psize(dgraph)) )
      stop("weights should have the same dimension and partitioning as dgraph")
    if(.naCheckPageRank(weights, trace)$found > 0) stop("missed values in weights darray!") 
  }


  niter <- as.numeric(niter)
  if (! niter > 0 )
    stop("Invalid iteration count, it should be a positive number")
  eps <- as.numeric(eps)
  if (! eps > 0 )
    stop("Invalid value for 'eps', it should be a positive number")
  damping <- as.numeric(damping)
  if (damping <= 0 || damping >= 1)
    stop("Invalid damping factor, it should be between 0 and 1")

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  ### Initialization
  if (trace) {
    cat("Initialization PR and OutDegree\n")
    starttime<-proc.time()
  }
  maxdiff <- eps
  # initial values for PR is 1/nVertices like most literature (it is 1-damping in igraph package)
  PR <- dmapply(function(data, nrow, ncol) matrix(data=data, nrow=nrow, ncol=ncol), 
                ncol=psize(dgraph)[,2], MoreArgs=list(nrow=1, data=1-damping), output.type="darray", combine="cbind")

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }
  

  #Let's get the number of outgoing edges in each partition
  if (trace) {
    cat("Calculating the number of outgoing edges\n")
    starttime<-proc.time()
  }

  if (is.null(weights)) { # when there is no weight on the edges
    OutDegree <- dmapply(function(X) X, rowSums(dgraph), output.type="darray", nparts=c(1, 1), combine='rbind') 
  } else {  # when there are weights on the edges
    weightedgraph <- dmapply(function(DGi, Wi) DGi * Wi, parts(dgraph), parts(weights), output.type=dgraph@type, nparts=nparts(dgraph), combine='rbind')
    OutDegree <- dmapply(function(X) X, rowSums(weightedgraph), output.type="darray", nparts=c(1, 1), combine='rbind')
  }

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }


  ### iterations
  iteration_counter <- 1
  if (trace)  iterations_start <- proc.time()
  while (niter >= iteration_counter && maxdiff >= eps) {
    if (trace) {
      cat("Iteration: ",iteration_counter,"\n")
      starttime <- proc.time()
    }
    iteration_counter <- iteration_counter + 1

    # the new PR (PageRank vector) is calculated in each iteration
    if(is.null(weights) && is.null(personalized)) {
        PR_new <- dmapply(function(dg, prOld, damping, TPs, peri, wi) {
                        if(class(dg) == "matrix") {
                            .Call("pagerank_vm", prOld, dg, TPs, damping, NULL, NULL, PACKAGE="pagerank.ddR")
                        } else {
                            .Call("pagerank_spvm", prOld, dg, TPs, damping, NULL, NULL, PACKAGE="pagerank.ddR")
                        }                        
                    }, dg=parts(dgraph), 
                    MoreArgs=list(prOld=PR, damping=damping, TPs=OutDegree), output.type="darray", combine="cbind")

    } else if(!is.null(weights) && is.null(personalized)) {
        PR_new <- dmapply(function(dg, prOld, damping, TPs, peri, wi) {
                        if(class(dg) == "matrix") {
                            .Call("pagerank_vm", prOld, dg, TPs, damping, NULL, wi, PACKAGE="pagerank.ddR")
                        } else {
                            .Call("pagerank_spvm", prOld, dg, TPs, damping, NULL, wi, PACKAGE="pagerank.ddR")
                        }                        
                    }, dg=parts(dgraph), wi=parts(weights), 
                    MoreArgs=list(prOld=PR, damping=damping, TPs=OutDegree), output.type="darray", combine="cbind")

    } else if(is.null(weights) && !is.null(personalized)) {
        PR_new <- dmapply(function(dg, prOld, damping, TPs, peri, wi) {
                        if(class(dg) == "matrix") {
                            .Call("pagerank_vm", prOld, dg, TPs, damping, peri, NULL, PACKAGE="pagerank.ddR")
                        } else {
                            .Call("pagerank_spvm", prOld, dg, TPs, damping, peri, NULL, PACKAGE="pagerank.ddR")
                        }                        
                    }, dg=parts(dgraph), peri=parts(personalized), 
                    MoreArgs=list(prOld=PR, damping=damping, TPs=OutDegree), output.type="darray", combine="cbind")

    } else {
        PR_new <- dmapply(function(dg, prOld, damping, TPs, peri, wi) {
                        if(class(dg) == "matrix") {
                            .Call("pagerank_vm", prOld, dg, TPs, damping, peri, wi, PACKAGE="pagerank.ddR")
                        } else {
                            .Call("pagerank_spvm", prOld, dg, TPs, damping, peri, wi, PACKAGE="pagerank.ddR")
                        }                        
                    }, dg=parts(dgraph), wi=parts(weights), peri=parts(personalized), 
                    MoreArgs=list(prOld=PR, damping=damping, TPs=OutDegree), output.type="darray", combine="cbind")
    }

    # normalizing the PR_new vector and finding maxdiff
    sumPR <- sum(PR_new)
    PR_new <- dmapply(function(PR_newi, sumPR) {PR_newi / sumPR}, 
                            PR_newi=parts(PR_new), MoreArgs=list(sumPR=sumPR), output.type="darray", combine="cbind")

    # the array that keeps the track of the convergence
    diffArray <- dmapply(function(PR_newi, PRi) {max(abs(PR_newi - PRi))}, 
                            PR_newi=parts(PR_new), PRi=parts(PR), output.type="darray", combine="cbind")

    maxdiff <- max(diffArray)
    if(is.na(maxdiff)) stop("An error occured in the calculation most likely because of missed values in dgraph. You may use na_action='exclude' to overwrite the missed values with 0.")
    # swaping the PR vectors
    tempPR <- PR
    PR <- PR_new
    PR_new <- tempPR

    if (trace) {    # end of timing step
      endtime <- proc.time()
      #print(distributedR_status())
      spentTime <- endtime-starttime
      cat("Spent time:",(spentTime)[3],"sec\n")
    }
    
  } # while

  if (trace) {
    iterations_finish <- proc.time()
    iterations_totalTime <- iterations_finish[3] - iterations_start[3]
  }

  if (trace) {
    endTotalTime <- proc.time()
    totalTime <- endTotalTime - startTotalTime
    cat("*****************************\n")
    cat("Total running time:",(totalTime)[3],"sec\n")
    iterationTime = iterations_totalTime / (iteration_counter -1)
    cat("Running time of each iteration on average:",iterationTime,"sec\n")
  }

  PR
}

## .naCheckPageRank checks any missed value (NA, NaN, Inf) in X
#   X: the input darray
#   trace: boolean, to show the progress 
#   cover: when it is TRUE, the missed values will be replaced with 0
#   it returns the number of missed values
.naCheckPageRank <- function(X, trace, cover = FALSE) {
  if (trace) {
    cat("Checking for missed values\n")
    starttime<-proc.time()
  }

  tempArray <- dmapply(function(Xi) sum(!is.finite(Xi)),
		        Xi = parts(X),output.type = "darray", combine = "cbind", nparts = c(1,totalParts(X)))   
  
  found <- sum(tempArray)

  if(cover && found != 0) {
    if(X@type == "sparse_darray")
      X <- dmapply(function(Xi) {Xi@x[!is.finite(Xi@x)] <- 0; Xi},
		        Xi = parts(X),output.type = "sparse_darray", combine = "cbind", nparts = c(1,totalParts(X)))
    else
      X <- dmapply(function(Xi) {Xi[!is.finite(Xi)] <- 0; Xi},
		        Xi = parts(X),output.type = "darray", combine = "cbind", nparts = c(1,totalParts(X)))
  }

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  list(X=X, found=found)
}

## finding the top ranked page
dwhich.max <- function(PR, trace=FALSE) {
  if(!is.darray(PR))  stop("PR must be of type darray")
  if(nrow(PR) != 1) stop("PR must have a single row")

  if (trace) {
    cat("Finding the top of each partition\n")
    starttime<-proc.time()
  }
  dTops <- dmapply(function(pri) list(topV=max(pri), topI=which.max(pri)),
                    pri=parts(PR), output.type = "dlist")

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    cat("Spent time:",(spentTime)[3],"sec\n")
  }

  topVI <- unlist(collect(dTops))
  topI_array <- Reduce('c', topVI[which(names(topVI) == "topI")])
  theTop <-   which.max(Reduce('c', topVI[which(names(topVI) == "topV")]))
  local_topI <- topI_array[theTop]

  global_topI <- sum(psize(PR)[,2][1:theTop-1]) + local_topI

  global_topI
}

