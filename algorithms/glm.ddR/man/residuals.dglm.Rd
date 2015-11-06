\name{residuals.dglm}
\alias{residuals.dglm}
\alias{residuals}
\alias{resid}

\title{
    Extract Residuals of an dglm Model
}
\description{
    This function extracts model residuals of an dglm model in a darray. The abbreviated function is resid. This function is only available for complete models.
}
\usage{
\method{residuals}{dglm}(object, type = c("deviance", "pearson", 
         "working", "response", "partial"), trace=FALSE, ...)
}
\arguments{
  \item{object}{an dglm model}
  \item{type}{can be "deviance", "pearson", "working", "response", or "partial".}
  \item{trace}{
    when TRUE, intermediate steps of the progress are displayed.
  }
  \item{\dots}{
    further arguments passed to or from other methods.
  }
  
}
\value{
    darray of residuals
}

\examples{
 \dontrun{
    library(glm.ddR)

    Y <- as.darray(as.matrix(mtcars$am),
                   c(ceiling(length(mtcars$am)/4),1))
    X <- as.darray(as.matrix(cbind(mtcars$wt,mtcars$hp)),
                   c(ceiling(length(mtcars$hp)/4),2))

    myModel <- dglm(responses=Y, predictors=X, 
                      family=binomial(logit), completeModel=TRUE)
    res <- resid(myModel)
 }
}

\keyword{ dglm model }
