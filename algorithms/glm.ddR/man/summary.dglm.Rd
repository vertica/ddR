\name{summary.dglm}
\alias{summary.dglm}
\alias{print.summary.dglm}
\title{
Summarizing The Model Made by dglm
}
\description{
This function prints a summary of the learned model.
}
\usage{
\method{summary}{dglm}(object, dispersion = NULL, correlation = FALSE, 
               symbolic.cor = FALSE, trace=FALSE, ...)
}
\arguments{
  \item{object}{
    a result of a call to \code{\link{dglm}}.
  }
  \item{dispersion}{
    the dispersion parameter for the family used. Either a single numerical value or \code{NULL} (the default), when it is inferred from \code{object}).
  }
  \item{correlation}{
    logical; if \code{TRUE}, the correlation matrix of the estimated parameters is returned and printed.
  }
  \item{symbolic.cor}{
    logical. If \code{TRUE}, print the correlations in a symbolic form (see \code{\link{symnum}}) rather than as numbers.
  }
  \item{trace}{
    when TRUE, intermediate steps of the progress are displayed.
  }
  \item{\dots}{
    further arguments passed to or from other methods.
  }
}
\value{
\code{summary.dglm} returns an object of class \code{"summary.dglm"}, a list with components

\item{call}{the component from \code{object}.}
\item{family}{the component from \code{object}.}
\item{deviance}{the component from \code{object}.}
\item{df.residual}{the component from \code{object}.}
\item{null.deviance}{the component from \code{object}. 
    It is NA for an incomplete model.}
\item{df.null}{the component from \code{object}.}
\item{deviance.resid}{(only when completeModel=TRUE) the deviance residuals:
    see \code{\link{residuals.dglm}}. It is NA for an incomplete model.}
\item{coefficients}{the matrix of coefficients, standard errors,
    z-values and p-values.  Aliased coefficients are omitted. 
    Except coefficients, other values are NA for an incomplete model.}
\item{aliased}{named logical vector showing if the original
    coefficients are aliased.} 
\item{dispersion}{either the supplied argument or the inferred/estimated
    dispersion if the latter is \code{NULL}.}
\item{df}{a 3-vector of the rank of the model and the number of
    residual degrees of freedom, plus number of non-aliased coefficients.}
\item{cov.unscaled}{the unscaled (\code{dispersion = 1}) estimated covariance
    matrix of the estimated coefficients. It is not available for an incomplete model.}
\item{cov.scaled}{ditto, scaled by \code{dispersion}. 
    It is not available for an incomplete model.}
\item{correlation}{(only if \code{correlation} is true.)  The estimated
    correlations of the estimated coefficients.}
\item{symbolic.cor}{(only if \code{correlation} is true.)  The value
    of the argument \code{symbolic.cor}.}
\item{minMax}{it contains the minimum and the maximum of the deviance residuals.
    They are NA for an incomplete model.}
}
\author{
    Vishrut Gupta, Arash Fard
}
\examples{
 \dontrun{
    require(glm.ddR)

    Y <- as.dobject(as.matrix(faithful$eruptions),
                    c(ceiling(length(faithful$eruption)/4),1))
    X <- as. dobject (as.matrix(faithful$waiting),
                    c(ceiling(length(faithful$waiting)/4),1))

    myModel <- dglm(responses=Y, predictors=X)
    summary(myModel)
 }
}

\keyword{ dglm models }
\keyword{ Distributed regression }
