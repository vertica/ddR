.dglm.fit.initialize.mustart <- function (family, Y, weights, trace) {
  switch( family$family,
         "binomial" = {
           ## anything, e.g. NA/NaN, for cases with zero weight is OK.
           responses_error <- "y values must be 0 <= y <= 1"
           check_valid_responses <- function(Yi, weightsi, link)
             {
               good <- weightsi > 0
               return(as.numeric(any(Yi[good,] < 0 | Yi[good,] > 1))) 
             }
           init_mu <- function(Yi, weightsi)
             {
               mustarti <- matrix(0, nrow = nrow(Yi), ncol = 1)
               good <- weightsi > 0
               mustarti[good,] <- (weightsi[good,] * Yi[good,] + 0.5)/(weightsi[good,] + 1)
               return(mustarti)
             }
         },
         "quasibinomial" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         "poisson" = {
           responses_error <- "negative values not allowed for the Poisson family"                    
           check_valid_responses <- function(Yi, weightsi, link)
             {
               good <- weightsi > 0
               as.numeric(any(Yi[good,] < 0))
             }
           init_mu <- function(Yi, weightsi)
             {
               mustarti <- matrix(0, nrow = nrow(Yi), ncol = 1)
               good <- weightsi > 0
               mustarti[good,] <- Yi[good,] + 0.1
               return(mustarti)
             }
         },
         "quasipoisson" = {
           stop(sQuote(family$family), " family not supported yet")
         },
         "gaussian" = {
           responses_error <- "invalid responses for the Gaussian family"
           check_valid_responses <- function(Yi, weightsi, link)
             {
               testArrayi <- as.numeric(0)
               good <- weightsi > 0
               as.numeric((link == "inverse" && any(Yi[good,] == 0)) || (link == "log" && any(Yi[good,] <= 0)))
             }
           init_mu <- function(Yi, weightsi)
             {
               mustarti <- matrix(0, nrow = nrow(Yi), ncol = ncol(Yi))
               good <- weightsi > 0
               mustarti[good,] <- Yi[good,]
               return(mustarti)
             }
#V                if(is.null(etastart) && is.null(start) && is.null(mustart) &&
#                          (any(collect(testArray) == 1)))
#                               stop("cannot find valid starting values: please specify some")
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

  if (trace) {
    message("Initilizing mustart")
    starttime<-proc.time()
  }
  
  valid_responses <- dmapply(check_valid_responses, Yi = parts(Y), weightsi = parts(weights),
                             MoreArgs = list(link = family$link),
                             output.type="darray", combine = "rbind", nparts = c(totalParts(Y),1))
  
  if(any(collect(valid_responses)>0))
    stop(responses_error)
  
  mustart <- dmapply(init_mu, Yi = parts(Y), weightsi = parts(weights),
                     output.type="darray", combine = "rbind", nparts = c(totalParts(Y),1))
  
  
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(mustart)
}

.dglm.fit.initialize.eta <- function(X, family, start, mustart,  weights, trace)
{
  ## Setting eta
  if (trace) {
    message("Initializing eta")
    starttime<-proc.time()
  }
  
  if(!is.null(start)) {
    init_eta_from_start <- function(offseti, Xi, st, weightsi)
      {
        etai <- matrix(0,nrow = nrow(Xi),ncol = 1)
        good <- weightsi > 0
        if(sum(good) == 1)
          etai[good,] = as.matrix(offseti[good]) + cbind(1,matrix(Xi[good,],1,)) %*% st
        else
          etai[good,] = as.matrix(offseti[good]) + cbind(1,Xi[good,]) %*% st
        return(etai)
      }
    
    eta <- dmapply(init_eta_from_start,  offseti = parts(offset),
                   Xi = parts(X), weights = parts(weights), MoreArgs = list(st = as.matrix(start)),
                   output.type = "darray", combine = "rbind",nparts = c(totalParts(weights),1))
    
  } else {
    ## mustart is already initialized
    
    init_eta_from_mu <- function(mustarti, func, weightsi)
      {
        etai <- matrix(0,nrow = nrow(weightsi),ncol = 1)
        good <- weightsi > 0
        etai[good,] <- func(mustarti[good,])
        return(etai)
      }
    eta <- dmapply(init_eta_from_mu, weights = parts(weights),
                   mustarti = parts(mustart), MoreArgs = list(func = family$linkfun),
                   output.type = "darray", combine = "rbind",nparts = c(totalParts(weights),1))
    
  }
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  
  return(eta)
}


.dglm.fit.initialize.mu <- function(family, eta, weightsi, trace)
{
  if (trace) {
    message("Updating mu")
    starttime<-proc.time()
  }
  
  setting_mu <- function(etai, func, weightsi)
    {
      mui <- matrix(0,nrow = nrow(weightsi), ncol = 1)
      good <- weightsi > 0
      mui[good,] <- func(etai[good,])
      return(mui)
    }
  
  mu <- dmapply(setting_mu, etai = parts(eta), weightsi = parts(weights),
                MoreArgs = list(func = family$linkinv), 
                output.type = "darray", combine = "rbind", nparts = c(totalParts(weights),1))
  
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(mu)
}


.dglm.fit.validate.mu_eta <- function(family, mu, eta, weights, trace)
{
  
  unless.null <- function(x, if.null) if(is.null(x)) if.null else x
  valideta <- unless.null(family$valideta, function(eta) TRUE) # parall safe for logit, identity, probit, cauchit, cloglog, sqrt, 1/mu^2, inverse
  validmu  <- unless.null(family$validmu,  function(mu) TRUE) # parall safe for binomial, gaussian, poisson
  
  ## validating mu and eta
  if (trace) {
    message("Validating mu and eta")
    starttime<-proc.time()
  }
  
  validate_mu_eta <- function(mui, etai, validmu, valideta, weightsi)
    {
      good <- weightsi > 0
      testArrayi <- as.numeric(1)
      if(any(good))
        testArrayi <- as.numeric(validmu(mui[good,]) && valideta(etai[good,]))
      return(testArrayi)
    }
  
  testArray <- dmapply(validate_mu_eta, mui = parts(mu), etai = parts(eta), 
                       weightsi = parts(weights), MoreArgs = list(validmu = validmu, valideta = valideta),
                       output.type = "darray", combine = "cbind", nparts = c(1,totalParts(weights)))
  
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  valid <-  all(collect(testArray) == 1)
  if( !valid )
    stop("cannot find valid starting values: please specify some", call. = FALSE)
  return(!valid)
}


.dglm.fit.initialize.dev <- function(Y, mu, weights, dev.resids,trace)
{
  if (trace) {
    message("Calculating initial deviance")
    starttime<-proc.time()
  }
  init_deviance <- function(Yi, mui, weightsi, dev.resids)
    {
      good <- weightsi > 0
      devArrayi <- as.numeric(0)
      if(any(good)) 
        devArrayi <- sum(dev.resids(Yi[good,1], mui[good,1], 
                                    weightsi[good,1]))
      return(devArrayi)
    }
  devArray <- dmapply(init_deviance, Yi = parts(Y), mui = parts(mu), weightsi = parts(weights),
                      MoreArgs = list(dev.resids=dev.resids), output.type = "darray", 
                      combine = "rbind", nparts = c(totalParts(Y),1))
  
  dev <- sum(devArray)
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(dev)
}


.dglm.fit.validate.Vmu <- function(mu, weights, variance, trace)
{
  if (trace) {
    message("Checking any NAs or 0s in V(mu)")
    starttime<-proc.time()
  }
  
  check_na_mu <- function(mui, weightsi, variance)
    {
      good <- weightsi > 0
      if(any(good)) {
        varmu <- variance(mui[good])
        return(as.numeric(any(is.na(varmu)) || any(varmu == 0)))
      } 
      return(as.numeric(0))
    }
  testArray <- dmapply(check_na_mu, mui = parts(mu), weightsi = parts(weights),
                       MoreArgs = list(variance = variance), 
                       output.type = "darray", combine = "rbind", nparts = c(totalParts(mu),1)) 
  
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  if( any( collect(testArray) != 0 ) )
    stop("NAs or 0s in V(mu)")
  return(TRUE)
}



.dglm.fit.calculate.mu.eta.val <- function(eta, mu.eta,  weights, trace)
{
  if (trace) {
    message("Calculating mu.eta.val")
    starttime<-proc.time()
  }
  calc_mu.eta.val <- function(etai, weightsi, mu.eta)
    {
      good <- (weightsi > 0)
      mu.eta.val.i <- matrix(0,nrow = nrow(weightsi))
      mu.eta.val.i[good,] <- mu.eta(etai[good,])
      return(mu.eta.val.i)
    }
  mu.eta.val <- dmapply(calc_mu.eta.val, etai = parts(eta), 
                        weightsi = parts(weights),
                        MoreArgs = list(mu.eta = mu.eta),
                        output.type = "darray", combine = "rbind", 
                        nparts = c(totalParts(weights),1))

  check_na <- function(etai, weightsi, mu.eta.val.i)
    {
      good <- (weightsi > 0)
      testArrayi <- as.numeric(any(is.na(mu.eta.val.i[good,])))
      return(testArrayi)
    }
  testArray <- dmapply(check_na, etai = parts(eta), 
                       weightsi = parts(weights),
                       mu.eta.val.i = parts(mu.eta.val),
                       output.type = "darray", combine = "cbind", 
                       nparts = c(1,totalParts(weights)))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  if( any( collect(testArray) == 1 ) )
    stop("NAs in d(mu)/d(eta)")
  return(mu.eta.val)
}


.dglm.fit.calculate.dGood <- function(mu.eta.val, weights, trace)
{
  if (trace) {
    message("Calculating dGood")
    starttime<-proc.time()
  }

  dGood <- dmapply(function(weightsi, mu.eta.val.i) 
                   matrix(as.numeric((weightsi > 0) & (mu.eta.val.i != 0)), nrow(weightsi), ncol(weightsi)),
                   weightsi = parts(weights),mu.eta.val.i = parts(mu.eta.val),
                   output.type = "darray", combine = "rbind", nparts = c(totalParts(weights),1))

  testArray <- dmapply(function(dGoodi) as.numeric(any(dGoodi == 1)), dGoodi = parts(dGood),
		       output.type = "darray", combine = "cbind", nparts = c(1,totalParts(weights)))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(list(dGood = dGood, no_valid = !all(collect(testArray)>0)))
}

.dglm.fit.calculate.Z <- function(Y, mu, eta, mu.eta.val, offset, dGood, trace)
{
  if (trace) {
    message("Calculating Z")
    starttime<-proc.time()
  }

  calc_z <- function(Yi, mui, etai, offseti, mu.eta.val.i, dGoodi)
    {
      Zi <- matrix(0,nrow = nrow(Yi), ncol = ncol(Yi))
      good <- (dGoodi == 1)
      Zi[good,] = (etai[good,] - as.matrix(offseti[good])) + (Yi[good,1] - mui[good,])/mu.eta.val.i[good,]
      return(Zi)

    }
  Z <- dmapply(calc_z, Yi = parts(Y), mui = parts(mu), 
               etai = parts(eta), offset = parts(offset),
               mu.eta.val.i = parts(mu.eta.val), dGoodi = parts(dGood), 
               output.type = "darray", combine = "rbind", nparts = c(totalParts(Y),1))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(Z)
}


.dglm.fit.calculate.W <- function(variance, mu, mu.eta.val, dGood, weights, trace)
{
  if (trace) {
    message("Calculating W")
    starttime<-proc.time()
  }

  calc_w <- function(weightsi, dGoodi, mu.eta.val.i, mui, variance)
    {
      good <- (dGoodi == 1)
      Wi <- matrix(0,nrow = nrow(weightsi), ncol = ncol(weightsi))
      Wi[good,] <- sqrt((weightsi[good,] * mu.eta.val.i[good,]^2)/variance(mui[good,]))
      return(Wi)
    }
  W <- dmapply(calc_w, weightsi = parts(weights), dGoodi = parts(dGood), 
               mu.eta.val.i = parts(mu.eta.val),
               mui = parts(mu), MoreArgs = list(variance = variance),
               output.type = "darray", combine = "rbind", nparts = c(totalParts(weights),1))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(W)
}



.dglm.fit.calculate.deviance <- function(X, Y, weights, offset,
                                         eta, mu, devArray, start, linkinv, dev.resids,
                                         trace = FALSE)
{
  if (trace) {
    message("Calculate updated values of eta and mu")
    starttime<-proc.time()
  }

  calculate_eta <- function(etai, Xi, start, offseti, weightsi)
    {
      good <- weightsi > 0
      if(all(good)) {
        etai <- (Xi %*% start[-1,] + start[1,1]) + as.matrix(offseti)
      } else if(any(good)) {
        if(sum(good) == 1) {
          etai[good,] <- cbind(1,matrix(Xi[good,],1,)) %*% start + as.matrix(offseti[good])
        } else {
          etai[good,] <- cbind(1,Xi[good,]) %*% start + as.matrix(offseti[good])
        }
      }
      return(etai)
    }
  eta <- dmapply(calculate_eta, etai = parts(eta), Xi = parts(X), 
                 offseti = parts(offset), weightsi = parts(weights),
                 MoreArgs = list(start = start),
                 output.type = eta@type, combine = "rbind",nparts = c(totalParts(weights),1))


  calculate_mu <- function(weightsi, mui, etai, linkinv)
    {
      good <- weightsi > 0
      if(all(good)) {
        mui <- linkinv(etai)
      } else if(any(good)) {
        mui[good,] <- linkinv(etai[good,])
      }
      return(mui)
    }
  mu <- dmapply(calculate_mu, weightsi = parts(weights), mui = parts(mu), etai = parts(eta),
                MoreArgs = list(linkinv = linkinv), 
                output.type = mu@type, combine = "rbind",nparts = c(totalParts(weights),1))
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }

  if (trace) {
    message("Calculating deviance")
    starttime<-proc.time()
  }

  calculate_devArray <- function(Yi, mui, weightsi, dev.resids)
    {
      good <- weightsi > 0
      devArrayi <- sum(dev.resids(Yi[good,1], mui[good,], weightsi[good,]))
    }
  devArray <- dmapply(calculate_devArray, Yi = parts(Y), 
                      mui = parts(mu), weightsi = parts(weights), MoreArgs = list(dev.resids = dev.resids),
                      output.type = "darray", combine = "rbind",nparts = c(totalParts(weights),1))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")

  }


  return(list(eta = eta, mu = mu, devArray = devArray))
}



.dglm.fit.calculate.residuals <- function(Y, mu, eta, mu.eta, weights, trace)
{
  if (trace) {
    message("Calculating residuals")
    starttime<-proc.time()
  }
  calc_resid <- function(Yi, mui, etai, mu.eta, weightsi)
    {
      resi = matrix(NA, nrow = nrow(Yi), ncol = ncol(Yi))
      good <- weightsi > 0
      resi[good,] <- (Yi[good,1] - mui[good,])/mu.eta(etai[good,])
      return(resi)
    }
  residuals <- dmapply(calc_resid, Yi = parts(Y), mui = parts(mu),
                       etai = parts(eta), weightsi = parts(weights),
                       MoreArgs = list(mu.eta = mu.eta), 
                       output.type = "darray", combine = "rbind", nparts = c(totalParts(Y),1))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(residuals)
}

.dglm.fit.calculate.null_deviance <- function(Y, offset, dev.resids, weights, intercept, linkinv, trace)
{
  if (intercept) {
    if (trace) {
      message("Calculating wtdmu")
      starttime<-proc.time()
    }
    sumWeight <- dmapply(function(weightsi) sum(weightsi[weightsi > 0]), 
                         weightsi = parts(weights),
                         output.type = "darray", combine = "rbind", nparts = c(totalParts(weights),1))

    sumMuli <- dmapply(function(weightsi, Yi)
                       {
                         return(sum(weightsi[weightsi > 0]*Yi[weightsi>0,1]))
                       }, 
                       weightsi = parts(weights),Yi = parts(Y),
                       output.type = "darray", combine = "rbind", nparts = c(totalParts(weights),1))

    wtdmu <- sum(sumMuli)/sum(sumWeight)
    if (trace) {    # end of timing step
      endtime <- proc.time()
      spentTime <- endtime-starttime
      message("Spent time: ",(spentTime)[3]," sec")
    }
    calc_nullArray  <- function(Yi, wtdmu, weightsi, dev.resids,offseti, linkinv)
      {
        good <- weightsi > 0
        nullArrayi <- sum(dev.resids(Yi[good,1], wtdmu, weightsi[good,]))
        return(nullArrayi)
      }
  }
  else
    {
      calc_nullArray  <- function(Yi, wtdmu, weightsi, dev.resids, offseti, linkinv)
        {
          good <- weightsi > 0
          nullArrayi <- sum(dev.resids(Yi[good,1], 
                                       linkinv(as.matrix(offseti[good])),
                                       weightsi[good,]))
          return(nullArrayi)
        }
    }
  if (trace) {
    message("Calculating null deviance")
    starttime<-proc.time()
  }
  nullArray <- dmapply(calc_nullArray, Yi = parts(Y), weightsi  = parts(weights), 
                       MoreArgs = list(wtdmu = wtdmu, dev.resids = dev.resids, linkinv = linkinv),
                       output.type = "darray", combine = "cbind", nparts = c(totalParts(weights),1))

  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  nulldev <- sum(nullArray)
}

.dglm.fit.validate.mu_final <- function(family, mu, eps, weights, trace)
{
  if (trace) {
    message("Checking the quality of the result")
    starttime<-proc.time()
  }


  if (family$family == "binomial") {
    test_mu <- function(mui, weightsi, eps)
      {
        good <- weightsi > 0
        testArrayi <- as.numeric(any(mui[good,] > 1 -eps) || any(mui[good,] < eps))
        return(testArrayi)
      }
    warning_statement <- "dglm.fit: fitted probabilities numerically 0 or 1 occurred"
  }
  else if (family$family == "poisson") {

    test_mu <- function(mui, weightsi, eps)
      {
        good <- weightsi > 0
        testArrayi <- as.numeric(any(mui[good,] < eps))
        return(testArrayi)
      }
    testArray <- dmapply(test_mu, weightsi = parts(weights), mui = parts(mu), 
                         MoreArgs = list(eps = eps), 
                         output.type = "darray", combine = "cbind", nparts = c(1,totalParts(weights)))
    warning_statement <- "dglm.fit: fitted rates numerically 0 occurred"
  }
  else
    return(TRUE)
  testArray <- dmapply(test_mu, weightsi = parts(weights), mui = parts(mu), 
                       MoreArgs = list(eps = eps), 
                       output.type = "darray", combine = "cbind", nparts = c(1,totalParts(weights)))
  if(any(collect(testArray) == 1))
    warning(warning_statement, call. = FALSE)


  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }
  return(TRUE)
}

.dglm.fit.calculate.aic <- function(family, y, mu, wt, dev, trace=TRUE, nobs) {
  if (trace) {
    message("Calculating aic")
    starttime<-proc.time()
  }

  switch(family$family,
         "binomial" = {            
           calc_aic <- function(yi, mui, wti, offset)
             {
               mi <- wti
               good <- wti > 0
               tempArrayi <- -2*sum(ifelse(mi[good,] > 0, (wti[good,]/mi[good,]), 0)*
                                    dbinom(round(mi[good,]*yi[good,1]),
                                           round(mi[good,]),
                                           mui[good,],
                                           log=TRUE))
               return(tempArrayi)
             }
         },
         "gaussian" = {
           calc_aic <- function(yi, mui, wti, offset)
             {
               good <- wti > 0
               tempArrayi <- sum(log(wti[good,]))
               return(offset - tempArrayi)
             }
         },
         "poisson" = {
           calc_aic <- function(yi, mui, wti, offset)
             {
               good <- wti > 0
               tempArrayi <- -2*sum(dpois(yi[good,], mui[good,], log=TRUE) * wti[good,])
               return(tempArrayi)
             }
         },
         stop(sQuote(family$family), " family not supported yet")
         )
  aic <- dmapply(calc_aic, yi = parts(y), mui = parts(mu), wti = parts(wt),
                 MoreArgs = list(offset = (nobs*(log(dev/nobs*2*pi)+1)+2)/totalParts(y)),
                 output.type = "darray", combine = "rbind", nparts = c(totalParts(y),1))
  aic <- sum(aic)
  if (trace) {    # end of timing step
    endtime <- proc.time()
    spentTime <- endtime-starttime
    message("Spent time: ",(spentTime)[3]," sec")
  }

  return(aic)
}
