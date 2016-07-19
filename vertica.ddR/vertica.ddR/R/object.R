setOldClass("tbl_vertica")

setClass(vertica.ddR.env$vertica.dobject.name,contains="DObject",
    representation(vertica_table = "tbl_vertica", partitions = "integer"),
    prototype = prototype(vertica_table = NULL, nparts = c(1L,1L), psize = matrix(1,1), 
      dim = c(1L,1L), vertica_table = structure(list(), class = "tbl_vertica")
))

#' @export
setMethod("initialize", vertica.ddR.env$vertica.dobject.name, function(.Object, ...) {
	.Object <- callNextMethod(.Object, ...)
	vertica.ddR.env$dobject_count <- vertica.ddR.env$dobject_count+1

	if(length(.Object@vertica_table)==0)
	{	
		if(.Object@type == "dlist") {
			new_dobject = list()
		}
		if(.Object@type == "darray"){
			new_dobject = matrix(nrow = 0, ncol = 0)
		}
		if(.Object@type == "sparse_darray"){
			new_dobject = sparseMatrix(dims = c(0,0), 
				    i = integer(0), 
				    j = integer(0),
				    x = integer(0))
		}

		if(.Object@type == "dframe"){
			new_dobject = data.frame()
		}

		meta_data = list(dim = dim(new_dobject), 
			  length = length(new_dobject))


		serialized_objects = vertica.ddR.serialize(new_dobject)
		serialized_meta_data = vertica.ddR.serialize(meta_data)
		serialized_meta_data <- enlarge_meta_data(serialized_meta_data, 
			  length(serialized_objects))


		.Object@partitions = as.integer(1:totalParts(.Object))
		dobject = lapply(.Object@partitions, function(x) 
		{
			partition <- data.frame(partition_id = x,
			chunk_id = 1:length(serialized_objects))
			partition$partition_values = serialized_objects
			partition$meta_data  = serialized_meta_data
			return(partition)
		})
		dobject <- do.call(rbind,dobject)

		if(db_has_table(vertica.ddR.env$vertica_dsn$con, 
			vertica.ddR.env$get_current_dobj()))
		{
			db_drop_table(vertica.ddR.env$vertica_dsn$con, 
				vertica.ddR.env$get_current_dobj())
		}


		types <- c("integer", "integer",  
	      	      paste0(vertica.ddR.env$long_var_binary,"(",
				vertica.ddR.env$chunk_size,")"),
	      	      paste0(vertica.ddR.env$long_var_binary,"(",
				vertica.ddR.env$chunk_size,")") )
		names(types) <- c("partition_id", "chunk_id", 
			     "partition_values", "meta_data")
		db_create_table(vertica.ddR.env$vertica_dsn$con, 
			vertica.ddR.env$get_current_dobj(), types)

		copy_object_to_table(dobject, 
			table_name = vertica.ddR.env$get_current_dobj())

		
		.Object@vertica_table <- tbl(vertica.ddR.env$vertica_dsn, 
			vertica.ddR.env$get_current_dobj()) 

		partition_psize <- partition_size(meta_data)

		.Object@psize = matrix(partition_psize, 
			ncol = length(partition_psize),
			byrow = TRUE, nrow = totalParts(.Object))
		.Object@dim = partition_psize * totalParts(.Object)

	}
	return(.Object)

})

#' @export
setMethod("get_parts",signature(vertica.ddR.env$vertica.dobject.name,"missing"),
	function(x, ...){

	get_parts(x,x@partitions, ...)

})

#' @export
setMethod("get_parts",signature(vertica.ddR.env$vertica.dobject.name,"integer"),
	function(x, index, ...){
	desired_partitions = index[index %in% x@partitions]
	lapply(desired_partitions, function(i)
	{
		partition = filter(x@vertica_table,partition_id == i)
		new(vertica.ddR.env$vertica.dobject.name, 
			vertica_table = partition, 
			partitions = as.integer(i), 
			psize = matrix(x@psize[i,],nrow = 1), 
			dim = as.integer(x@psize[i,]))
	})

})

#' @export
setMethod("do_collect",signature(vertica.ddR.env$vertica.dobject.name,"integer"),function(x, parts) {
	localized_object = NULL
	temp <- lapply(x@partitions[parts], function(i)
	{
		partition = filter(x@vertica_table,partition_id == i)
		partition = arrange(partition, chunk_id)
		partition = dplyr::collect(partition)$partition_values
		partition = vertica.ddR.deserialize(partition)
		return(partition)
	})
	object_parts = vector(mode = "list", length = length(x@partitions))
	object_parts[x@partitions[parts]] = temp
	
	assemble_from_parts(object_parts, type = x@type, nparts = x@nparts)
})


