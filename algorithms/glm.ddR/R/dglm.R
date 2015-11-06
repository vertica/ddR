#########################################################
#  File dglm.R
#  Distributed version of glm 
#
#  This code is a distributed version of glm function available in R stats package.
#  

#########################################################
## A copy of glm.R license:
#
#  Copyright (C) 1995-2012 The R Core Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  A copy of the GNU General Public License is available at
#  http://www.r-project.org/Licenses/


## responses: The darray that contains the vector of responses. It is single column, with values 0 or 1 for binomial regression.
## predictors: The darray that contains the vector of predictors. It may have many columns, but the number of rows and its number of blocks should be the same as responses.
## weights: It is an optional darray for weights on the samples. It has a single column. The number of rows and its number of blocks should be the same as responses.
## na_action: It is an optional argument which indicates what should happen when the data contain unsuitable values; e.g., NA, NaN, or Inf. The default is set to 'exclude'. The other option is 'fail'.
## start: Starting values for coefficients. It is optional.
## etastart: Starting values for parameter 'eta' which is used for computing deviance. It is optional.
## mustart: Starting values for mu 'parameter' which is used for computing deviance. It is optional.
## offset: An optional darray which can be used to specify an _a priori_ known component to be included in the linear predictor during fitting.
## control: An optional list of controlling arguments. The optional elements of the list and their default values are: epsilon = 1e-8, maxit = 25, trace = FALSE, rigorous = FALSE.
## method: This argument can be used for the future improvement. The only available fitting method at the moment is 'dglm.fit.Newton'. In the future, if we have new developed algorithms, this algorith can be used to switch between them. 
## completeModel: When it is FALSE (default), the output values that are in darray structure will be discarded.

dglm <- function(responses, predictors, family = gaussian, weights = NULL,
                 na_action="exclude", start = NULL, etastart = NULL, mustart = NULL, offset = NULL,		
                 control = list(...), method = "dglm.fit.Newton", completeModel = FALSE, ...)
{

  startTotalTime <- proc.time()

  call <- match.call()

  ## check the type of responses and predictors
  if( is.null(responses) || is.null(predictors) )
    stop("'responses' and 'predictors' are required arguments")
  if(! is.darray(responses) || ! is.darray(predictors))
    stop("'responses' and 'predictors' should be distributed arrays")
  if(is.sparse_darray(responses) || is.sparse_darray(predictors))
    stop("Sparse darray is not supported for 'responses' and 'predictors'")
  if(ncol(responses) != 1)
    stop("Multivariate is not supported")

  ## family
  if(is.character(family))
    family <- get(family, mode = "function", envir = parent.frame())
  if(is.function(family)) family <- family()
  if(is.null(family$family))
    stop("'family' not recognized")
  if(!( (family$family == "binomial" && family$link == "logit") || 
       (family$family == "gaussian" && family$link == "identity") ||
       (family$family == "poisson" && family$link == "log") ))
    stop("Only binomial(logit), poisson(log), and gaussian(identity) are supported for now")
  ## control parameters
  control <- do.call("dglm.control", control)
  trace <- ifelse(control$trace, TRUE, FALSE)
  nparts <- totalParts(responses)

  ## check weights
  binaryWeights <- TRUE
  if(!is.null(weights)){
    if(! is.darray(weights))
      stop("'weights' should be of type darray")
    if(weights@sparse)
      stop("Sparse darray is not supported for 'weights'")

    if( any(psize(weights) != psize(responses)) )
      stop(gettextf("'weights' should have the same partitioning pattern as 'responses', and single column"), domain = NA)
    ## check that there is no negative value for weights
    if (trace) {
      message("Checking weights")
      starttime <- proc.time()
    }
    testArray1 <- dmapply(function(weightsi) as.numeric(all(weightsi >= 0)),
                          weightsi = parts(weights), output.type = "darray", 
                          combine = "cbind", nparts = c(1,totalParts(weights)))
    testArray2 <- dmapply(function(weightsi) if(all(weightsi >= 0)) as.numeric(all(weightsi == 0 || weightsi == 1)) else 0,
                          weightsi = parts(weights), output.type = "darray", 
                          combine = "cbind", nparts = c(1,totalParts(weights)))

    if (trace) {    # end of timing step
      endtime <- proc.time()
      spentTime <- endtime-starttime
      message("Spent time: ",(spentTime)[3]," sec")
    }

    if( ! all( collect(testArray1) == 1 ) )
      stop("all weights should be non-negative")
    if( ! all( collect(testArray2) == 1 ) )
      binaryWeights <- FALSE
  }
  ## check etastart
  if(!is.null(etastart)) {
    if(! is.darray(etastart))
      stop("'etastart' should be of type darray")
    if(etastart@sparse)
      stop("Sparse darray is not supported for 'etastart'")
    if( any(psize(etastart) != psize(responses)) )
      stop(gettextf("'etastart' should have the same pattern as 'respnses'"), domain = NA)
  }
  ## check mustart
  if(!is.null(mustart)) {
    if(! is.darray(mustart))
      stop("'mustart' should be of type darray")
    if(mustart@sparse)
      stop("Sparse darray is not supported for 'mustart'")
    if( any(psize(mustart) != psize(responses)) )
      stop(gettextf("'mustart' should have the same partitioning pattern as 'respnses'"), domain = NA)
    if(family$link == "logit" || family$link == "log") {
      if (trace) {
        message("Checking mustart")
        starttime <- proc.time()
      }
      testArray <- dmapply(function(mustarti) as.numeric(all(mustarti > 0 & mustarti < 1)),
                           mustarti = parts(mustart), output.type = "darray", 
                           combine = "cbind", nparts = c(1,totalParts(mustart)))

      if (trace) {    # end of timing step
        endtime <- proc.time()
        spentTime <- endtime-starttime
        message("Spent time: ",(spentTime)[3]," sec")
      }
      
      if( ! all( collect(testArray) == 1 ) )
        stop("mustart has values out of valid range (0,1)")
    }        
  }

  ## check offset
  if(!is.null(offset)) {
    if(! is.darray(offset))
      stop("'offset' should be of type darray")
    if( any(psize(offset) != psize(responses)) )
      stop(gettextf("'offset' should have the same pattern as 'responses'"), domain = NA)
  }

  ## The call to method (dglm.fit)
  if (trace) {
    startFitTime <- proc.time()
  }
  fit <- dglm.fit(predictors, responses, weights = weights, start = start,
                  etastart = etastart, mustart = mustart,
                  offset = offset, family = family, control = control,
                  intercept = TRUE, method = method, binaryWeights = binaryWeights, 
                  na_action=na_action, completeModel=completeModel)


                                        #V    fit <- call(if(is.function(method)) "method" else "dglm.fit",
                                        #                     predictors, responses, weights = weights, start = start,
                                        #                     etastart = etastart, mustart = mustart,
                                        #                     offset = offset, family = family, control = control,
                                        #                     intercept = TRUE, method = method, binaryWeights = binaryWeights, 
                                        #                     na_action=na_action, completeModel=completeModel)
  
  if (trace) {
    endFitTime <- proc.time()
    message("Fitting time:",(endFitTime - startFitTime)[3]," sec")
  }

  if(! is.null(colnames(predictors)))
    rownames(fit$coefficients) <- c("(Intercept)", colnames(predictors))
  ## adding some parameters to fit
  fit <- c(fit, list(call = call, offset = offset, control = control, method = method))

  class(fit) <- c("dglm", "glm", "lm")

  if (trace) {
    endTotalTime <- proc.time()
    totalTime <- endTotalTime - startTotalTime
    message("*****************************")
    message("Total running time:",(totalTime)[3]," sec")
  }

  fit
}

dglm.control <- function(epsilon = 1e-8, maxit = 25, trace = FALSE, rigorous = FALSE)
{
  if(!is.numeric(epsilon) || epsilon <= 0)
    stop("value of 'epsilon' must be > 0")
  if(!is.numeric(maxit) || maxit <= 0)
    stop("maximum number of iterations must be > 0")
  list(epsilon = epsilon, maxit = maxit, trace = trace, rigorous = rigorous)
}

dglm.fit <-
  function (X, Y, weights = NULL, start = NULL,
            etastart = NULL, mustart = NULL, offset = NULL,
            family = gaussian(), control = list(), intercept = TRUE, method = "dglm.fit.Newton", 
            binaryWeights = TRUE, na_action="exclude", completeModel=FALSE)
{
  control <- do.call("dglm.control", control)
  conv <- FALSE       # converged
  nobs <- nrow(Y)     # number of observations
  nvars <- ncol(X) + 1    # adding 1 to the number of variances because of the intercept
  nparts <- totalParts(X)  # number of partitions (blocks)

  if (nparts != totalParts(Y) || !all(psize(X)[,1] == psize(Y)[,1]))
    stop("'predictors' and 'responses' should be partitioned row-wise with the same pattern. If the samples are loaded from a database, you can use the db2darrays() function to load the darrays with the same partitioning pattern.", call. = FALSE)

  if(nvars < 2) {
    stop("Empty model is not supported", call. = FALSE)
  }
  if(method != "dglm.fit.Newton")
    stop("Unknown method")

  trace <- ifelse(control$trace, TRUE, FALSE)
  if (trace) {
    starttime_preLoop<-proc.time()
  }

  ## define weights
  if (is.null(weights)) 
    weights <- dmapply(function(nrow) matrix(1,nrow=nrow), 
                       nrow = psize(X)[,1],
                       output.type = "darray", combine = "rbind", nparts = c(totalParts(X),1))


                                        # .naCheck function may alter weights
  .naCheck_output <- .naCheck(X, Y, weights, trace)
  nOmit <- .naCheck_output$nOmit
  weights <- .naCheck_output$weights
  ngoodObs <- .naCheck_output$ngoodObs
  
  
                                        # decision about na_action
  switch(na_action,
         "exclude" = {
                                        # do nothing            
         },
         "fail" = {
           if(nOmit > 0)
             stop("missing values in samples")
         },
         {
           stop("only 'exclude' and 'fail' are valid values for na_action")
         }
         )

  ## stop if not enough parameters    
  if (ngoodObs < nvars)
    stop(paste("Number of variables is", nvars, ", but only", ngoodObs, "observations (samples with NA/NaN/Inf values are excluded)"))

  ## define offset
  isInitialOffset <- TRUE
  if (is.null(offset)) {

    offset <- dmapply(function(nrow) matrix(0,nrow = nrow,ncol = 1),
                      nrow = psize(X)[,1],
                      output.type="darray", combine = "rbind", nparts = c(totalParts(X),1))
    isInitialOffset <- FALSE
  }

  ## get family functions:
  variance <- family$variance # parall safe for binomial, gaussian, poisson
  linkinv  <- family$linkinv # parall safe for logit, identity, log, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
  if (!is.function(variance) || !is.function(linkinv) )
    stop("'family' argument seems not to be a valid family object", call. = FALSE)
  dev.resids <- family$dev.resids # parall safe for binomial, gaussian, poisson
  mu.eta <- family$mu.eta # parall safe for logit, identity, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
  unless.null <- function(x, if.null) if(is.null(x)) if.null else x
  valideta <- unless.null(family$valideta, function(eta) TRUE) # parall safe for logit, identity, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
  validmu  <- unless.null(family$validmu,  function(mu) TRUE) # parall safe for binomial, gaussian, poisson


  ## calculates mustart if not present
  if(is.null(mustart)) 
    mustart <- .dglm.fit.initialize.mustart(family, Y, weights, trace)
  else 
    .dglm.fit.initialize.mustart(family, Y, weights, trace)
  
  if(!is.null(start))
    {            
      if (length(start) != nvars)
        stop(paste("length of 'start' should equal", nvars, "and correspond to initial coefs")) 
      coefold <- start
    }
  else
    coefold <- NULL

  ## calculates etastart if not present
  if(!is.null(etastart)) 
    eta <- etastart
  else
    eta <- .dglm.fit.initialize.eta(X, family, start, mustart,  weights, trace)

  ## Setting mu
  if(is.null(etastart)) 
    mu <- mustart
  else 
    mu <- .dglm.fit.initialize.mu(family, eta, weights, trace)


  if(control$rigorous) .dglm.fit.validate.mu_eta(family, mu, eta, weights, trace)


  ## calculate initial deviance and coefficient
  devold <- .dglm.fit.initialize.dev(Y, mu, weights, dev.resids,trace)
  dev <- devold   # the value of deviance

  boundary <- conv <- FALSE   # indicators for boundary situation and convergence
  if (is.null(start))
    start <- matrix(0,nvars)
  coef <- coefold <- start

  ## it should be faster if some darrays are allocated out of the iteration

  if (trace) {
    stoptime_preLoop<-proc.time()
    preLoopTime <- stoptime_preLoop-starttime_preLoop
    message("***** Pre-Loop time:",(preLoopTime)[3],"sec *****")
    startItTime <- proc.time()
  }

  ##------------- THE Iteratively Reweighting L.S. iteration -----------
  for (iter in 1L:control$maxit) {

    if(control$rigorous) .dglm.fit.validate.Vmu(mu, weights, variance, trace)

    if(! binaryWeights || isInitialOffset) {
      ## The routine when weights are not binary or there is an initial offset. It is more time consuming.
      mu.eta.val <- .dglm.fit.calculate.mu.eta.val(eta, mu.eta, weights, trace)
      
      ## drop observations for which w will be zero
      temp_output <- .dglm.fit.calculate.dGood(mu.eta.val, weights, trace)
      dGood <- temp_output$dGood
      if(temp_output$no_valid)
        {
          conv <- FALSE
          warning("no observations informative at iteration ", iter)
          break
        }


                                        #O          z <- (eta - offset)[good] + (y - mu)[good]/mu.eta.val[good]       
      Z <-  .dglm.fit.calculate.Z(Y, mu, eta, mu.eta.val, offset, dGood, trace)
      
      W <- .dglm.fit.calculate.W(variance, mu, mu.eta.val, dGood, weights, trace)
      family = gaussian(identity)
    } # !binaryWeights
    else
      {
        W <- weights # in this routine W and weights are the same
        Z <- Y
      } # binaryWeights
    
    coefficients <- .LSPSolution_Newton(Z, X, start, family, W, trace)

    if (any(!is.finite(coefficients))) {
      conv <- FALSE
      warning(paste("non-finite coefficients at iteration", iter))
      break
    }
    start <- coefficients
    notValid <- TRUE
    boundary <- FALSE
    ii <- 0
    while (notValid) {
      if (ii >= control$maxit)
        stop("inner loop 1; cannot correct step size", call. = FALSE)
      ii <- ii + 1
      if(ii != 1)
        start <- (start + coefold)/2
      ## calculating deviance
      dev_output <- .dglm.fit.calculate.deviance(X, Y, weights, offset,
                                                 eta, mu, devArray, start, linkinv, dev.resids, trace)
      eta = dev_output$eta
      mu = dev_output$mu
      devArray = dev_output$devArray
      rm(dev_output)
      if (control$trace)
        message("Deviance = ", dev, " Iterations = ", iter)

      dev <- sum(devArray)
      if(control$rigorous) 
        notValid <- .dglm.fit.validate.mu_eta(family, mu, eta, weights, trace)
      else
        notValid <- !is.finite(dev)
      if( notValid & is.null(coefold))
        stop("no valid set of coefficients has been found: please supply starting values", call. = FALSE)
      if(notValid)
        boundary <- TRUE
    }


    ## check for convergence
    if (abs(dev - devold)/(0.1 + abs(dev)) < control$epsilon) {
      conv <- TRUE
      coef <- start
      break
    } else {
      devold <- dev
      coef <- coefold <- start
    }

  } ##-------------- end IRLS iteration -------------------------------
  if (trace) {
    endItTime <- proc.time()
    message("***** LS Iteration time:",(endItTime - startItTime)[3],"sec *****")
    starttime_postLoop<-proc.time()
  }

  if(! is.null(colnames(X)))
    rownames(coef) <- c("(Intercept)", colnames(X)) 

  if(!conv && iter==1)
    stop("dglm.fit failed to converge at the first iteration.")
  if (!conv)
    warning("dglm.fit: algorithm did not converge", call. = FALSE)

  if (boundary)
    warning("dglm.fit: algorithm stopped at boundary value", call. = FALSE)

  ## calculate df
  nulldf <- ngoodObs - as.integer(intercept)

  ## The accurate rank cannot be found through our approach. So it is assumed equal to nvars.
  rank <- if(nvars < 2) 0 else min(nvars, ngoodObs)
  resdf  <- ngoodObs - rank

                                        # cov-matrix will be more accurate if we use initial weight when it is binary
  if(binaryWeights && isInitialOffset)
    W = weights

  eps <- 10*.Machine$double.eps

  if(completeModel) {

    if(control$rigorous) .dglm.fit.validate.mu_final(family, mu, weights, eps, trace)


    ## update by accurate calculation
    residuals <- .dglm.fit.calculate.residuals(Y, mu, eta, mu.eta, weights, trace)

    ## calculate null deviance -- corrected in glm() if offset and intercept
    nulldev <- .dglm.fit.calculate.null_deviance(Y, offset, 
                                                 dev.resids, weights, intercept, linkinv,trace)

    ## calculate AIC
    aic.model <- .dglm.fit.calculate.aic(family, Y, mu, weights, dev, trace, ngoodObs) + 2 * rank

    ## effects, Rmat, qr are not computed through our approach in comparison to glm
    
    fit <- list(coefficients = coef, d.residuals = residuals, d.fitted.values = mu,
                family = family, d.linear.predictors = eta, deviance = dev, aic = aic.model,
                null.deviance = nulldev, iter = iter, weights = W,
                prior.weights = weights, df.residual = resdf, df.null = nulldf,
                converged = conv, boundary = boundary, responses = Y, predictors = X)
  }
  else { # test
    fit <- list(coefficients = coef, d.fitted.values = mu, 
                family = family, d.linear.predictors = eta, deviance = dev, aic = NA,
                null.deviance = NA, iter = iter, weights = W,
                prior.weights = weights, df.residual = resdf, df.null = nulldf,
                converged = conv, boundary = boundary, responses = Y, predictors = X)
  }
  
  if(nOmit > 0)
    fit$na_action <- list(type="exclude", numbers=nOmit)

  class(fit) <- c("dglm", "glm", "lm")

  if (trace) {
    endtime_postLoop <- proc.time()
    message("***** Post-Loop time:",(endtime_postLoop - starttime_postLoop)[3],"sec *****")
  }
  fit
} # end of dglm.fit function


print.dglm <- function(x, digits= max(3, getOption("digits") - 3), ...)
{
  cat("\nCall:  ",
      paste(deparse(x$call), sep="\n", collapse = "\n"), "\n\n", sep="")
  if(length(coef(x))) {
    cat("Coefficients")
    if(is.character(co <- x$contrasts))
      cat("  [contrasts: ",
          apply(cbind(names(co),co), 1L, paste, collapse="="), "]")
    cat(":\n")
    print.default(format(x$coefficients, digits=digits),
                  print.gap = 2, quote = FALSE)
  } else cat("No coefficients\n\n")
  cat("\nDegrees of Freedom:", x$df.null, "Total (i.e. Null); ",
      x$df.residual, "Residual\n")
  if(nzchar(mess <- naprint(x$na.action))) cat("  (",mess, ")\n", sep="")
  cat("Null Deviance:	   ",	format(signif(x$null.deviance, digits)),
      "\nResidual Deviance:", format(signif(x$deviance, digits)),
      "\tAIC:", format(signif(x$aic, digits)), "\n")
  invisible(x)
}

summary.dglm <- function(object, dispersion = NULL,
                         correlation = FALSE, symbolic.cor = FALSE, trace=FALSE, ...)
{
  est.disp <- FALSE
  df.r <- object$df.residual
  completeModel <- !is.null(object$d.residuals)

  if(completeModel) { # runs only in complete mode
    if(is.null(dispersion)){	# calculate dispersion if needed
      if(object$family$family %in% c("poisson", "binomial"))
        dispersion <- 1
      else if(df.r > 0) {
        est.disp <- TRUE
        calc_dispersion <- function(wi, ri, df.r)
          {
            tempArrayi = 0
            good <- wi > 0
            tempArrayi <- sum((wi * ri^2)[good,])/ df.r
            return(tempArrayi)
          }
        tempArray <- dmapply(calc_dispersion, wi = parts(object$weights), ri = parts(object$d.residuals), 
                             MoreArgs = list(df.r = df.r), 
                             output.type = "darray", combine = "cbind", nparts = c(totalParts(object$weights),1))
        dispersion <- sum(tempArray)
      } else {
        est.disp <- TRUE
        dispersion <- NaN
      }
    }
  } # runs only in complete mode
  ## calculate scaled and unscaled covariance matrix

  aliased <- is.na(coef(object))  # used in print method
  p <- length(object$coefficients)

  if (p > 0) {
    if(!completeModel) { # incomplete mode
      coef.table <- cbind(object$coefficients, NA, NA, NA)
      dimnames(coef.table) <- list(rownames(object$coefficients), c("Estimate", "Std. Error", "t value","Pr(>|t|)"))
      covmat.unscaled <- covmat <- matrix(, 0L, 0L)           
    } else { # complete mode

      coef.p <- object$coefficients
      covmat.unscaled <- .covariantMatrix(object, trace=trace)
      covmat <- dispersion * covmat.unscaled
      var.cf <- diag(covmat)

      ## calculate coef table

      s.err <- sqrt(var.cf)
      tvalue <- coef.p/s.err

      dn <- c("Estimate", "Std. Error")
      if(!est.disp) { # known dispersion
        pvalue <- 2*pnorm(-abs(tvalue))
        coef.table <- cbind(coef.p, s.err, tvalue, pvalue)
        dimnames(coef.table) <- list(rownames(coef.p),
                                     c(dn, "z value","Pr(>|z|)"))
      } else if(df.r > 0) {
        pvalue <- 2*pt(-abs(tvalue), df.r)
        coef.table <- cbind(coef.p, s.err, tvalue, pvalue)
        dimnames(coef.table) <- list(rownames(coef.p),
                                     c(dn, "t value","Pr(>|t|)"))
      } else { # df.r == 0
        coef.table <- cbind(coef.p, NaN, NaN, NaN)
        dimnames(coef.table) <- list(rownames(coef.p),
                                     c(dn, "t value","Pr(>|t|)"))
      }
    }          
    df.f <- p

  } else {
    coef.table <- matrix(, 0L, 4L)
    dimnames(coef.table) <-
      list(NULL, c("Estimate", "Std. Error", "t value", "Pr(>|t|)"))
    covmat.unscaled <- covmat <- matrix(, 0L, 0L)
    df.f <- length(aliased)
  }
  ## return answer

  ## these need not all exist, e.g. na.action.
  keep <- match(c("call","family","deviance", "aic",
                  "df.residual","null.deviance","df.null",
                  "iter", "na_action"), names(object), 0L)
  if(completeModel) { # complete mode
    deviance.resid <- residuals.dglm(object, type = "deviance", trace=FALSE)
  } else { # incomplete mode
    deviance.resid <- NA
  }
  ans <- c(object[keep],
           list(deviance.resid = deviance.resid,
                coefficients = coef.table,
                aliased = aliased,
                dispersion = dispersion,
                df = c(p, df.r, df.f),
                cov.unscaled = covmat.unscaled,
                cov.scaled = covmat))

  if(correlation && p > 0 && completeModel) {
    dd <- sqrt(diag(covmat.unscaled))
    ans$correlation <-
      covmat.unscaled/outer(dd,dd)
    ans$symbolic.cor <- symbolic.cor
  }

                                        # it was better if it was quantiles
  if(completeModel) { # incomplete mode
    ans$minMax <- drop(cbind(min(ans$deviance.resid, na.rm=TRUE),max(ans$deviance.resid, na.rm=TRUE)))
  } else { # incomplete mode
    ans$minMax <- c(NA,NA)
  }
  names(ans$minMax) <- c("Min", "Max")

  class(ans) <- "summary.dglm"
  return(ans)
}

print.summary.dglm <-
  function (x, digits = max(3, getOption("digits") - 3),
            symbolic.cor = x$symbolic.cor,
            signif.stars = getOption("show.signif.stars"), ...)
{
  cat("\nCall:\n",
      paste(deparse(x$call), sep="\n", collapse = "\n"), "\n\n", sep="")

  xx <- zapsmall(x$minMax, digits + 1)
  cat("Deviance Residuals: \n")
  print.default(xx, digits=digits, na.print = "", print.gap = 2)

  if(length(x$aliased) == 0L) {
    cat("\nNo Coefficients\n")
  } else {
    ## df component added in 1.8.0
    ## partial matching problem here.
    df <- if ("df" %in% names(x)) x[["df"]] else NULL
    if (!is.null(df) && (nsingular <- df[3L] - df[1L]))
      cat("\nCoefficients: (", nsingular,
          " not defined because of singularities)\n", sep = "")
    else cat("\nCoefficients:\n")
    coefs <- x$coefficients
    if(!is.null(aliased <- x$aliased) && any(aliased)) {
      cn <- names(aliased)
      coefs <- matrix(NA, length(aliased), 4L,
                      dimnames=list(cn, colnames(coefs)))
      coefs[!aliased, ] <- x$coefficients
    }
    printCoefmat(coefs, digits=digits, signif.stars=signif.stars,
                 na.print="NA", ...)
  }
  
  cat("\n(Dispersion parameter for ", x$family$family,
      " family taken to be ", format(x$dispersion), ")\n\n",
      apply(cbind(paste(format(c("Null","Residual"), justify="right"),
                        "deviance:"),
                  format(unlist(x[c("null.deviance","deviance")]),
                         digits= max(5, digits+1)), " on",
                  format(unlist(x[c("df.null","df.residual")])),
                  " degrees of freedom\n"),
            1L, paste, collapse=" "), sep="")
  if(nzchar(mess <- naprint(x$na.action))) cat("  (",mess, ")\n", sep="")
  cat("AIC: ", format(x$aic, digits= max(4, digits+1)),"\n\n",
      "Number of Fisher Scoring iterations: ", x$iter,
      "\n", sep="")

  correl <- x$correlation
  if(!is.null(correl)) {
    p <- NCOL(correl)
    if(p > 1) {
      cat("\nCorrelation of Coefficients:\n")
      if(is.logical(symbolic.cor) && symbolic.cor) {# NULL < 1.7.0 objects
        print(symnum(correl, abbr.colnames = NULL))
      } else {
        correl <- format(round(correl, 2), nsmall = 2, digits = digits)
        correl[!lower.tri(correl)] <- ""
        print(correl[-1, -p, drop=FALSE], quote = FALSE)
      }
    }
  }
  cat("\n")
  invisible(x)
}


## GLM Methods for Generic Functions :

residuals.dglm <-
  function(object, type = c("deviance", "pearson", "working", "response", "partial"), 
           trace=FALSE, ...)
{
  if(is.null(object$d.residuals))
    stop("This function is only available for complete models.")
  type <- match.arg(type)
  Y <- object$responses
  r <- object$d.residuals
  mu	<- object$d.fitted.values
  wts <- object$prior.weights
  nparts <- totalParts(mu)

  switch(type,
         deviance=,pearson=,response=
         if(is.null(Y)) {
           mu.eta <- object$family$mu.eta
           eta <- object$d.linear.predictors
	   if(trace)
             message("Building Y")

	   Y <- dmapply(function(mui, ri, etai) matrix(mui + ri * mu.eta(etai),ncol = 1),
                        mui = parts(mu), ri = parts(r), etai = parts(eta),
                        output.type = "darray", combine = "rbind", nparts = c(totalParts(mu),1))
         }
         )
  
  switch(type,
         deviance = if(object$df.residual > 0) {
           if(trace)
             message("Building residuals")

           calculating_d.res <- function(Yi, mui, wtsi, func)
             {
               d.resi <- matrix(0, nrow = nrow(mui), ncol = ncol(mui))
               good <- wtsi > 0
               d.resi[good,] <- sqrt(pmax(func(Yi[good,], mui[good,], wtsi[good,]), 0))
               return(d.resi)
             }
           calculating_res <- function(Yi, mui, wtsi, func, d.resi)
             {
               resi <- matrix(NA, nrow = nrow(mui), ncol = ncol(mui))
               good <- wtsi > 0
               resi[good,] <- ifelse(Yi[good,1] > mui[good,], d.resi[good,], -d.resi[good,])
               return(resi)
             }
           d.res <- dmapply(calculating_d.res, Yi = parts(Y), mui = parts(mu), wtsi = parts(wts),
                            MoreArgs = list(func = object$family$dev.resids),
                            output.type = "darray", combine = "rbind", nparts = c(totalParts(Y),1))
           res <- dmapply(calculating_res, Yi = parts(Y), mui = parts(mu), wtsi = parts(wts),
                          d.resi = parts(d.res), MoreArgs = list(func = object$family$dev.resids),
                          output.type = "darray", combine = "rbind", nparts = c(totalParts(Y),1))
         },
         pearson = {
           if(trace)
             message("Building residuals")

           calculating_resi <- function(Yi, mui, wtsi, func)
             {
               resi <- (Yi[,1]-mui)*sqrt(wtsi)/sqrt(func(mui))
             }
           res <- dmapply(calculating_resi, Yi = parts(Y), mui = parts(mu), wtsi = parts(wts),
                          MoreArgs = list(func = object$family$dev.resids),
                          output.type = "darray", combine = "rbind", nparts = c(totalParts(Y),1))

         },
         working = {
           res <- r
         },
         response = {
           res <- Y - mu
         },
         partial = {
           res <- r
         }
         )
  res
}

weights.dglm <- function(object, type = c("prior", "working"), ...)
{
  type <- match.arg(type)
  res <- if(type == "prior") object$prior.weights else object$weights
  if(is.null(object$na.action)) res
  else naresid(object$na.action, res)
}


#########################################################
                                        #        Functions specifically added for dglm        #
#########################################################

## Distributed version of family.initilize

## Solution provided for Least Square Problem (LSP)
## Ref for the algorithm: http://www.cs.purdue.edu/homes/alanqi/Courses/ML-11/CS59000-ML-13.pdf
.LSPSolution_Newton <- function (Y, X, globalTheta, family, weights, trace=TRUE) {

  nobservation <- NROW(X)
  npredictors <- NCOL(X)
  npart <- totalParts(X)

                                        # distGrad Stores the partial gradiants distributed across workers: (npredictors+1 x 1) matrix
                                        # distHessian Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 

                                        # Calculate gradient and hessian locally
  if (trace) {
    message("Calculating local gradient and hessian")
    starttime<-proc.time()
  }
  calculate_gradient_hessian <- function(x, y, theta, m, weightsi, family, npredictors)
    {
      good <- (weightsi > 0)
      g <- matrix(0,nrow = npredictors+1)
      hess <- matrix(0,nrow = (npredictors+1)*(npredictors+1), ncol = 1)

      if(sum(good) == 1)
        x <- cbind(1, matrix(x[good,],1,)) * weightsi[good,]
      else
        x <- cbind(1, x[good,]) * weightsi[good,]

      trans <- t(x)   # transpose of matrix x

      switch(family$family,
             "binomial"={
                                        # dim(h) = dim(y)
               h <- family$linkinv(x %*% theta)
                                        #Gradient calcualation
               g <- 1/m * (trans %*% (h - y[good,1] * weightsi[good,1]))  
                                        #Hessian calcualation
               hsum <- as.numeric(h * (1 - h))
               hess <- matrix(1/m * (trans %*% (hsum * x ))) # converted into a single column matrix
             },
             "gaussian"={
               g <- 1/m * trans %*% (y[good,1] * weightsi[good,1])
               hess <- matrix(1/m * trans %*% x) # converted into a single column matrix
             },
             "poisson"={
               h <- family$linkinv(x %*% theta)
                                        #Gradient calcualation
               g <- 1/m * (trans %*% (h - y[good,1] * weightsi[good,1]))
                                        #Hessian calcualation
               hsum <- as.numeric(h)
               hess <- matrix(1/m * (trans %*% (hsum * x ))) # converted into a single column matrix                
             },
             stop("The specified family is not supported for now")
             )
      return(list(g=g, hess = hess))              
    }
  grad_hess <- dmapply(calculate_gradient_hessian, x = parts(X), y = parts(Y), 
                       weightsi = parts(weights),
                       MoreArgs = list(theta=globalTheta, m=nobservation, family=family, npredictors=npredictors))

  distGrad <- dmapply(function(x) x[[1]]$g, x = parts(grad_hess),
                      output.type = "darray", combine = "cbind", nparts = c(1,totalParts(grad_hess)))
  distHessian <- dmapply(function(x) x[[1]]$hess, x = parts(grad_hess),
                         output.type = "darray", combine = "cbind", nparts = c(1,totalParts(grad_hess)))

                                        #Fetch all gradients and sum them up to get one vector: 1 x (npredictors+1)
  grad<-rowSums(distGrad)

                                        #Fetch all hessians, sum them up, and convert to a (npredictors+1 x npredictors+1) matrix
                                        #We should idealy use reduce() when number of partitions is large
  hessian<-matrix(rowSums(distHessian),nrow=npredictors+1) 
  
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }

  switch(family$family,
         "binomial"={
           globalTheta = globalTheta - solve(hessian) %*% matrix(grad, ncol=1)
         },
         "gaussian"={
           globalTheta = solve(hessian) %*% matrix(grad, ncol=1)
         },
         "poisson"={
           globalTheta = globalTheta - solve(hessian) %*% matrix(grad, ncol=1)
         },
         stop("The specified family is not supported for now")
         )

}

## Calculating the co-variant matrix
.covariantMatrix <- function (object, trace=TRUE) {
  X <- object$predictors
  nobs <- NROW(X)
  nparts <- totalParts(X)
  npredictors <- NCOL(X)
  W <- object$weights   # samples with weight==0 should not be considered in the calculation

  switch( object$family$family,
         "binomial" = {               
           if (trace) message("Calculating the covariant matrix")

                                        #Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
           calc_error <- function(Xi, theta, Wi, linkinv, npredictors)
             {
               hess <- matrix(0,nrow = (npredictors+1)*(npredictors+1),ncol = 1)
               good <- Wi > 0
               if(sum(good) == 1)
                 x <- cbind(1,matrix(Xi[good,],1,)) * Wi[good,1]
               else
                 x <- cbind(1,Xi[good,]) * Wi[good,1]
               z <- x %*% theta
               h <- linkinv(z)

                                        #Hessian calcualation for the covariance
               hsum <- as.numeric(h * (1 - h))
               hess <- matrix((t(x) %*% (hsum * x ))) # converted into a single column matrix
               return(hess)
             }
         },
         "quasibinomial" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         "poisson" = {
                                        #Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
           calc_error <- function(Xi, theta, Wi, linkinv, npredictors)
             {
               hess <- matrix(0,nrow = (npredictors+1)*(npredictors+1),ncol = 1)
               good <- Wi > 0
               if(sum(good) == 1)
                 x <- cbind(1,matrix(Xi[good,],1,)) * Wi[good,1]
               else
                 x <- cbind(1,Xi[good,]) * Wi[good,1]

               z <- x %*% theta
               h <- linkinv(z)

                                        #Hessian calcualation for the covariance
               hsum <- as.numeric(h)
               hess <- matrix((t(x) %*% (hsum * x ))) # converted into a single column matrix
               return(hess)
               
             }
         },
         "quasipoisson" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         "gaussian" = {
                                        #Stores the partial hessian distributed across workers:  (npredictors+1 x npredictors+1) matrix flattened out 
           calc_error <- function(Xi, theta, Wi, linkinv, npredictors)
             {
               hess <- matrix(0,nrow = (npredictors+1)*(npredictors+1),ncol = 1)
               good <- Wi > 0
               if(sum(good) == 1)
                 x <- cbind(1,matrix(Xi[good,],1,)) * Wi[good,1]
               else
                 x <- cbind(1,Xi[good,]) * Wi[good,1]

                                        #Hessian calcualation for the covariance
               hess <- matrix(t(x) %*% x) # converted into a single column matrix 
               return(hess)
             }

         },
         "Gamma" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         "inverse.gaussian" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         "quasi" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         ## else :
         stop(sQuote(family$family), " family not recognised")
         )# end switch(.)

  distHessian <- dmapply(calc_error, Xi = parts(X), Wi = parts(W),
                         MoreArgs = list(theta = object$coefficients, linkinv = object$family$linkiv,npredictors=npredictors),
                         output.type = "darray", combine = "cbind", nparts = c(1,totalParts(X)))

                                        #Fetch all hessians, sum them up, and convert to a (npredictors+1 x npredictors+1) matrix
  hessian <- matrix(rowSums(distHessian),nrow=npredictors+1)
  covmat <- solve(hessian)
}

## prediction given a model and newdata
## object: A built model of type dglm.
## newdata: A darray or a matrix containing predictors of new samples
## type: The type of prediction required which can be "link" or "response"
## na.action: A function to determine what should be done with missing values (reserved for the future improvement)  
## mask: A darray with a single column, and 0 or 1 as the value of its elements.
##      It indicates which samples (rows) should be considered in the calculation
predict.dglm <-
  function(object, newdata, type = c("link", "response"), na.action = na.pass, mask=NULL, trace=FALSE, ...)
{
  if(missing(newdata) || is.null(newdata)) {
    stop("newdata argument is required!")
  }
  if(!is.darray(newdata) && !is.matrix(newdata))
    stop("newdata should be of type darray or matrix")
  type <- match.arg(type)
  coef <- object$coefficients
  if(ncol(newdata) != length(coef)-1)
    stop("the number of predictors in newdata should fit the number of coefficients in the model")

  if(!is.null(mask))
    {
      if(class(newdata) != class(mask))
        stop("newdata and mask should be of the same type")
      if( nrow(mask) != NROW(newdata) || ncol(mask) != 1 )
        stop("'mask' must have a single column and the same number of rows as newdata")

    }


  ## newdata can be either a darray or a normal array
  if(is.darray(newdata)){  # newdata is a darray
    if (trace) message("Calculating link prediction")
    if(!is.null(mask)) {     # no mask
      if( nrow(mask) != nrow(newdata) || ncol(mask) != 1 || any(psize(mask)[,1] != psize(newdata)[,1]))
        stop("'mask' must have the same partitioning pattern as newdata")
    }
    calc_link <- function(newdatai, coef, maski)
      {                
        if(!is.null(maski))
          {
            predi <- matrix(0,nrow = nrow(newdatai), ncol = ncol(coef))
            good <- maski > 0
            if(sum(good) == 1)
              predi[good,] <- cbind(1,matrix(newdatai[good,],1,)) %*% coef
            else
              predi[good,] <- cbind(1,newdatai[good,]) %*% coef
          }
        else
          predi <- cbind(1,newdatai) %*% coef
        return(predi)
      }
    pred <- dmapply(calc_link, newdatai = parts(newdata), 
                    maski = if(is.null(mask)) rep(list(NULL),totalParts(newdata)) else parts(mask),
                    MoreArgs = list(coef = coef), 
                    output.type = "darray", combine ="rbind", nparts = c(totalParts(newdata),1))

    switch(type,
           response = {
             if (trace) message("Calculating response prediction")

             calc_prediction <- function(predi, func, maski)
               {
                 if(is.null(maski))
                   predi = func(predi)
                 else
                   predi[maski > 0,] <- func(predi[maski > 0,])
                 return(predi)
               }

             pred <- dmapply(calc_prediction, predi = parts(pred), 
                             maski = if(is.null(mask)) rep(list(NULL),totalParts(newdata)) else parts(mask),
                             MoreArgs =list(func =  family(object)$linkinv),
                             output.type = "darray", combine ="rbind", nparts = c(totalParts(newdata),1))

           },
           link = )

  } else {                # newdata is a normal array
    if(is.null(mask)) { # no mask
      if (trace) message("Calculating link prediction")
      pred <- cbind(1,newdata) %*% coef
      switch(type,
             response = {
               if (trace) message("Calculating response prediction")
               pred <- family(object)$linkinv(pred)
             },
             link = )
    } else {            # with mask
      if(class(newdata) != class(mask))
        stop("newdata and mask should be of the same type")
      if( nrow(mask) != NROW(newdata) || ncol(mask) != 1 )
        stop("'mask' must have a single column and the same number of rows as newdata")
      pred <- matrix(NA, NROW(newdata), 1)
      if (trace) message("Calculating link prediction")
      pred[mask > 0,] <- cbind(1,newdata[mask > 0,]) %*% coef
      switch(type,
             response = {
               if (trace) message("Calculating response prediction")
               pred[mask > 0,] <- family(object)$linkinv(pred[mask > 0,])
             },
             link = )
    }
  }
  pred
}

## it overwrites generic fitted function and will be used in validation
fitted.dglm <- function (object, ...) {
  object$d.fitted.values
}


                                        # .naCheck checks any missed value (NA, NaN, or Inf) in X, and Y
                                        # This function may alter weights
.naCheck <- function(X, Y, weights, trace) {
  if (trace) {
    message("Checking for missed values")
    starttime<-proc.time()
  }
  check_na <- function(xi,yi)
    {
      yTest <- !is.finite(yi)
      xTest <- matrix(rowSums(!is.finite(xi)),nrow(yi),ncol(yi)) != 0
      test <- yTest | xTest
      return(as.numeric(sum(test)))
    }
  nOmits <- dmapply(check_na, xi = parts(X), yi = parts(Y),
                    output.type = "darray", combine = "rbind", nparts = c(totalParts(X),1))
  nOmits <- sum(nOmits) # number of excluded samples


  set_weights <- function(xi, yi, wi)
    {
      yTest <- !is.finite(yi)
      xTest <- matrix(rowSums(!is.finite(xi)),nrow(yi),ncol(yi)) != 0
      wi[yTest | xTest] <- 0
      return(wi)
    }
  weights <- dmapply(set_weights, xi = parts(X), yi = parts(Y), wi = parts(weights),
                     output.type = "darray", combine = "rbind", nparts = c(totalParts(X),1))


  testArray <- dmapply(function(weightsi) as.numeric(sum(weightsi > 0)),
                       weightsi = parts(weights),
                       output.type="darray", combine = "cbind", nparts = c(totalParts(weights),1)) 
  ngoodObs <- sum(testArray)

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(list(nOmits = nOmits,weights = weights, ngoodObs = ngoodObs))
}
