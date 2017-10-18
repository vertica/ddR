#' @import methods ddR
#' @importFrom sparklyr spark_connect

setClass("spark.ddR", contains = "ddRDriver", slots = c(sc = "spark_connection"))

init_spark <- function(master = "local", ...) {
  message("Backend switched to Spark. Initializing the Spark context...")

  sc = sparklyr::spark_connect(master , ...)

  new("spark.ddR",
      DListClass = "ParallelObj",
      DFrameClass = "ParallelObj",
      DArrayClass = "ParallelObj",
      name = "spark",
      sc = sc
  )

}
