## Contains the default "Parallel" driver

# Using default "Parallel" as dummy backend for now.
setClass("ParallelDriver", contains = "DDSDriver")

setClass("ParallelDList", contains = "DObject", 
      prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))
setClass("ParallelDArray", contains = "DObject",
prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L))
)
setClass("ParallelDFrame", contains = "DObject",
prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))

setMethod("init",c(x = "ParallelDriver"),
      function(x) print("Init'ing ParallelDriver"))

# Singleton driver
parallel <- new("ParallelDriver", DListClass = "ParallelDList", DFrameClass = "ParallelDFrame", DArrayClass = "ParallelDArray", backendName = "Parallel (Dummy)")

dds.env$driver <- parallel
