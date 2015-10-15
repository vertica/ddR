\name{dds}
\alias{dds}
\docType{package}
\title{Distributed Data-structures in R}

\description{\pkg{dds} simplifies large-scale data analysis. It
  includes new language constructs to express distributed programs in R.
  Distributed programs writted in \pkg{dds} can work across multiple execution engines such
 as \pkg{parallel}, \pkg{distributedR}, and others. \pkg{dds} provides
  data-structures such as distributed array \code{\link{darray}} to
  partition and share data across multiple R instances. Users can
  express parallel execution using \code{\link{dmapply}}.}

\section{Commands}{ \pkg{dds} contains the following commands. For
more details use help function on each command.

\subsection{Session manangement}{
\itemize{
\item \code{\link{useBackend}} - choose execution engine
}
}

\subsection{Distributed array, data.frame, and list}{
\itemize{
  \item \code{\link{darray}} - create distributed array
  \item \code{\link{dframe}} - create distributed data frame
  \item \code{\link{dlist}} - create distributed list
  \item \code{\link{as.darray}} - create darray object from matrix object
  \item \code{\link{is.darray}} - check if object is distributed array
  \item \code{\link{parts}} - obtain partitions of an object
  \item \code{\link{nparts}} - number of partitions as vector
  \item \code{\link{totalParts}} - obtain total number of partitions
  \item \code{\link{psize}} - obtain dimensions of partitions
  \item \code{\link{collect}} - fetch darray, dframe or dlist object at the master
  \item \code{\link{repartition}} - repartition input object
}
}

\subsection{Distributed execution}{
\itemize{
  \item \code{\link{dmapply}} - execute function on cluster
  \item \code{\link{dlapply}} - execute function on cluster
}
}

}

\author{HP Vertica Development Team}

\references{
  \itemize{
    \item Prasad, S., Fard, A., Gupta, V., Martinez, J., LeFevre, J., Xu,
     V., Hsu, M., Roy, I. Large scale predictive analytics in Vertica:
     Fast data transfer, distributed model creation and in-database
     prediction (2015). _Sigmod 2015_, 1657-1668.

    \item Venkataraman, S., Bodzsar, E., Roy, I., AuYoung, A., and Schreiber,
    R. (2013) Presto: Distributed Machine Learning and Graph Processing
    with Sparse Matrices. \emph{EuroSys'13}, 197--210.
    \item Homepage: https://github.com/vertica/DistributedR
%    \item Mailing list: distributedRTeam@external.groups.hp.com
}
}
\examples{
\dontrun{                  
  library(dds)
  useBackend(parallel)
  a <- dmapply(function(x,y) x+y, 1:5, 2:6, nparts=3) 
  collect(a)
}}
\keyword{distributed R}
\keyword{parallel R}
\keyword{Big Data}