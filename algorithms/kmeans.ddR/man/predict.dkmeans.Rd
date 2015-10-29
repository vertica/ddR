\name{predict.dkmeans}
\alias{predict.dkmeans}
\title{distributed predict method for a kmeans model}
\description{
This function finds cluster label of a set of samples according to a given set of centers specified in the model object.
}
\usage{
\method{predict}{dkmeans}(object, newdata, trace=FALSE, ...)
}

\arguments{
  \item{object}{
    an object returned by dkmeans function
  }
  \item{newdata}{
    a darray (dense or sparse) or a matrix which contains the samples.
  }
  \item{trace}{
    when this argument is true, intermediate steps of the progress are displayed.
  }
  \item{...}{
    additional arguments to be passed (None are supported).
  }

}
\details{
This function applies the centers found by dkmeans on a new set of samples in order to label them.
}
\value{
This function returns a darray or a matrix based on the type of newdata which contains the corresponding label of each sample.
}
\author{
    Vishrut Gupta, Arash Fard
}

\examples{
 \dontrun{
    iris2 <- iris
    iris2$Species <- NULL

    library(kmeans.ddR)

    X <- as.darray(data.matrix(iris2))

    mykm <- dkmeans(X,centers=3)

    newdata <- matrix(c(5,4,3,5,7,1,0,8),2,4)
    labels <- predict(mykm,newdata)
 }
}

\keyword{clustering}
\keyword{kmeans}
\keyword{distributed}
\keyword{distributed clustering}
\keyword{distributed kmeans}
