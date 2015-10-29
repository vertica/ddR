\name{predict.drandomForest}
\alias{predict.drandomForest}
\title{distributed predict method for applying a random forest objects on a darray or a dframe}
\description{
  This function can be used to apply a model of type \code{drandomForest} or \code{randomForest} to a new data for prediction.
}
\usage{
  \method{predict}{drandomForest}(object, newdata, trace=FALSE, ...)
}
\arguments{
  \item{object}{an object of class \code{randomForest}, as that
    created by the function \code{randomForest} or 
    \code{drandomForest}.}
  \item{newdata}{a darray, a dframe, a data.frame, or a matrix
    that contains new data. darray is highly recommended to dframe 
    when there is no categorial data}
  \item{trace}{when this argument is true, intermediate steps of the progress are displayed.}
  \item{...}{additional arguments to be passed to predict.randomForest}

}

\value{
  It returns predicted classes in a distributed or non-distributed objects 
    depending on the type of the input. When the newdata is of type darray,
  the type of returned value will be also darray unless the output is categorical data.
  When the output is a dframe when the newdata is of type dframe.
}
\references{
  Breiman, L. (2001), \emph{Random Forests}, Machine Learning 45(1),
  5-32.
}
\author{
    Vishrut Gupta, Arash Fard, Winston Li, Matthew Saltz
}

\seealso{\code{\link{drandomForest}}}

\examples{
 \dontrun{
# example for darray
library(ddR.randomForest)

nExecutor <- 2
iris.rf <- drandomForest(Species ~ ., iris,nExecutor = nExecutor)
iris.predictions <- predict(iris.rf,iris)
    
}
}
\keyword{classification}
\keyword{regression}
