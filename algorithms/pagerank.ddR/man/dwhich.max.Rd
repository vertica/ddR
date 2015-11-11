\name{dwhich.max}
\alias{dwhich.max}
\title{Distributed which.max}
\description{
dwhich.max function is a distributed version of which.max function for a 1D-array which has darray as its input argument.
}
\usage{
dwhich.max(PR, trace=FALSE)
}

\arguments{
  \item{PR}{
    a darray (dense or sparse). It must have only a single row.
  }
  \item{trace}{
    when this argument is TRUE, intermediate steps of the progress are displayed.
  }
}
\details{
This function finds and returns the index of the maximum value stored in a darray. The darray is assumed to have a single row which is similar to the pagerank vector returned by dpagerank. Therefore, it is suitable for finding the index of the page with the highest rank in the pagerank vector produced by dpagerank.
}
\value{
it returns the index of the maximum value stored in a darray.
}
\author{
    Arash Fard, Vishrut Gupta
}

\examples{
 \dontrun{
    
    library(pagerank.ddR)
    
    graph <- matrix(0, 6,6)
    graph[2,1] <- 1L;graph[2,3] <- 1L;graph[3,1] <- 1L;graph[3,2] <- 1L;
    graph[3,4] <- 1L;graph[4,5] <- 1L;graph[4,6] <- 1L;graph[5,4] <- 1L; 
    graph[5,6] <- 1L;graph[6,4] <- 1L

    dgraph <- as.darray(graph, c(6,3))
    pr <- dpagerank(dgraph)
    dwhich.max(pr)
 }
}

\keyword{Big Data Analytics}
\keyword{distributed pagerank}
