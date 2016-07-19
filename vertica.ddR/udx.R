loadNamespace("vertica.ddR")
vertica.ddR.env = list()
vertica.ddR.env$long_var_binary = "varbinary"
vertica.ddR.chunk_size <- 64*1000


vertica.ddR.worker.OutputCallback <- function(x)
{
	 ret = data.frame(datatype = c("int", "int", 
	       			   vertica.ddR.env$long_var_binary, 
				   vertica.ddR.env$long_var_binary), 
	       length = c(1,1,vertica.ddR.chunk_size, vertica.ddR.chunk_size), 
	       scale = c(1,1,1,1), 
	       name = c("partition_id","chunk_id",
	       	    "partition_values", "meta_data"))

}

vertica.ddR.worker.factory <- function()
{
	list(name=vertica.ddR:::vertica.ddR.worker,
		udxtype=c("transform"),
		intype=c("int", "int", "int", vertica.ddR.env$long_var_binary), 
		outtype=c("int", "int", vertica.ddR.env$long_var_binary, 
				 vertica.ddR.env$long_var_binary),
		outtypecallback = vertica.ddR.worker.OutputCallback,
		outnames = c("partition_id", "chunk_id", "partition_values",
			 "meta_data"))

}


vertica.ddR.serialize.OutputCallback <- function(x)
{
	 ret = data.frame(datatype = c("int", "int", "varchar", "varchar"),	
	       length = c(1,1,vertica.ddR.chunk_size, vertica.ddR.chunk_size), 
	       scale = c(1,1,1,1), 
	       name = c("partition_id","chunk_id",
	       	    "partition_values", "meta_data"))

}

vertica.ddR.serialize.factory <- function()
{	
	list(name=vertica.ddR:::vertica.ddR.serialize.udx,
		udxtype=c("transform"),
		intype=c("any"), 
		outtype=c("int", "int", 	       			   
				 vertica.ddR.env$long_var_binary, 
				 vertica.ddR.env$long_var_binary), 
		outtypecallback = vertica.ddR.worker.OutputCallback,
		outnames = c("partition_id", "chunk_id", "partition_values",
			 "meta_data"))

}

