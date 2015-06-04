## Contains the default "Parallel" driver

# Using default "Parallel" as dummy backend for now.
setClass("ParallelDriver", contains = "DDSDriver")

setClass("ParallelDList", contains = "Backend", 
      prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))
setClass("ParallelDArray", contains = "Backend",
prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L))
)
setClass("ParallelDFrame", contains = "Backend",
prototype = prototype(nparts = 1L, psize = matrix(1,1),
      dim = c(1L,1L)
))

setMethod("init",c(x = "ParallelDriver"),
      function(x) print("Init'ing ParallelDriver"))

# Singleton driver
parallel <- new("ParallelDriver")
dds.env$driver <- parallel

setMethod("create.dobj",c(x = "ParallelDriver"),
   function(x, type) {
     if(type == "DList") return(new("ParallelDList"))
     else if (type == "DArray") return(new("ParallelDArray"))
     else return(new("ParallelDFrame"))
   })
