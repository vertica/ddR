


setClass("spark.ddR", contains = "ddRDriver")

init_spark <- function(master = "local", ...) {
  message("Backend switched to Spark. Initializing the Spark context...")

  sc = sparklyr::spark_connect(master , ...)

  # TODO
  # Executors

  new("spark.ddR",
      DListClass = "ParallelObj",
      DFrameClass = "ParallelObj",
      DArrayClass = "ParallelObj",
      name = "spark",
      sc = sc
  )

}
