

vertica.ddR.worker <- function(x)
{
	start_deserialization_time <- proc.time()

	tryCatch({

	names(x) <- c("task","arg","chunk_id","val")
	task_id = x$task[1]
	task_args = NULL
	task_func = NULL
	dobject_args <- vector("list",length(x[[1]])-1)

	unique_args = unique(x$arg)
	combined_args = lapply(unique_args, function(arg, x)
	{
		valid_indices = which(x$arg == arg)
		valid_indices = valid_indices[order(x$chunk_id[valid_indices])]
		combined_arg = x$val[valid_indices]
		c(combined_arg,recursive = TRUE)
	}, x = x)


	combined_args = lapply(combined_args,vertica.ddR.deserialize)

	for(i in 1:length(unique_args))
	{
		ddR_obj = combined_args[[i]]
		if(unique_args[i] == -1)
			task_func = ddR_obj
		else if(unique_args[i] == 0)
		     	task_args = ddR_obj
		else
			dobject_args[[unique_args[i]]] <- ddR_obj
	}

	
	.Call("rapply_alt", task_args, function(func) 
	{
		par_env <- parent.env(environment()) 
		return(func(par_env$dobject_args))		
	},"Vertica.dobject.pointer", PACKAGE = "vertica.ddR")


	output.type = task_args$output.type
	combine = task_args$combine
	MoreArgs = task_args$MoreArgs
	task_args$output.type <- NULL
	task_args$combine <- NULL
	task_args$MoreArgs <- NULL
	environment(task_func) <- globalenv()

	margs = c(list(FUN = task_func), task_args, SIMPLIFY = FALSE,
	      MoreArgs = list(MoreArgs))

	if(output.type == "sparse_darray")
		library(Matrix)

	stop_deserialization_time <- proc.time()



	start_execution_time <- proc.time()
	output = do.call(mapply, margs)

	if(output.type == "sparse_darray")
	{
		#TODO: try to coerce as sparse matrix or throw an error
	}
	else if(output.type == "darray")
	{
		output = lapply(output,as.matrix)
	}
	else if(output.type == "dframe")
	{		
		output = lapply(output,as.data.frame)
	}

	if(output.type != "dlist")
	{
		if(combine == "c")
			   combine = "cbind"
		if(combine == "default")
			   combine = "rbind"
		output = do.call(combine, output)
	}
	else
	{
		if(combine == "c")
			output = as.list(unlist(output,recursive = FALSE))
	}
	
	stop_execution_time <- proc.time()


	start_serialization_time <- proc.time()	
	dimension = dim(output)
	if(output.type == "dlist")
		dimension = length(output)
	meta_data = list(dim = dimension, length = length(output))


	}, error = function(e)
	{
		output <<- e
		meta_data <<- list(dim = NULL, length = NULL)	
		stop_deserialization_time <<- c(0,0,0)
		start_deserialization_time <<- c(0,0,0)
		stop_execution_time <<- c(0,0,0)
		start_execution_time <<- c(0,0,0)
		stop_serialization_time <<- c(0,0,0)
		start_serialization_time <<- c(0,0,0)
	})

	output = vertica.ddR.serialize(output)
	stop_serialization_time <- proc.time()

	meta_data$deserialization_time = 
		  	stop_deserialization_time - start_deserialization_time
	meta_data$execution_time = 
		  	stop_execution_time - start_execution_time
	meta_data$serialization_time = 
		  	stop_serialization_time - start_serialization_time
	meta_data = vertica.ddR.serialize(meta_data)
	meta_data <- enlarge_meta_data(meta_data,length(output))

	out_data <- data.frame(partition_id = task_id, 
		 chunk_id = 1:length(output))

	out_data$partition_values = output
	out_data$meta_data = meta_data
	return(out_data)

}

assemble_from_parts <- function(parts, type, nparts)
{
	if(type == "dlist")
	{
		localized_object <- do.call(base::c, parts)

	}
	else 
	{
		skeleton = matrix(1:prod(nparts), 
			  nrow = nparts[1], ncol = nparts[2], byrow = TRUE)
		localized_object = lapply(1:nrow(skeleton), 
			function(row_id, skeleton, parts)
				{
					ids = skeleton[row_id,]
					do.call(cbind,
					parts[ids][!sapply(parts[ids],is.null)])
				},
				skeleton = skeleton, parts = parts)
		localized_object = do.call(rbind, localized_object)
	}
	return(localized_object)
}

partition_size <- function(meta_data)
{

	partition_psize = as.integer(meta_data$dim)
	if(is.null(partition_psize))
		partition_psize  = meta_data$length
	return(partition_psize)

}

enlarge_meta_data <- function(meta_data, new_len)
{
	if(length(meta_data) < new_len)
		meta_data[(length(meta_data)+1):new_len] = 
			lapply((length(meta_data)+1):new_len, 
			function(x) raw(0))
	return(meta_data)
}

vertica.ddR.serialize.combine <- function(x)
{
	return(c(x, recursive = TRUE))
	#return(paste(x,collapse = ":"))
}

vertica.ddR.serialize <- function(x, ascii = FALSE)
{
	suppressWarnings({
	full_serialization <- serialize(x,NULL, xdr = FALSE, ascii = ascii )
	})
	chunk_size <- vertica.ddR.chunk_size - 2
	start <- seq(0,length(full_serialization),chunk_size)
	start <- c(start,length(full_serialization))
	end <- start[2:length(start)]
	start <- start[1:(length(start)-1)]+1
	chunked_serialization <- lapply(1:length(start), 
		function(i, start, end, full_serialization)
		{
			chunk_start = start[i]
			chunk_end = end[i]
			return (full_serialization[chunk_start:chunk_end])
		}, start = start, end = end,
		   full_serialization = full_serialization)

	chunked_serialization = lapply(chunked_serialization,
		vertica.ddR.serialize.combine)
}

vertica.ddR.deserialize <- function(x)
{	
	full_serialization <- vertica.ddR.serialize.combine(x)
	partition_object = unserialize(full_serialization)
	return(partition_object)
}



vertica.ddR.serialize.udx <-function(x)
{
	partition_id = 1


	start_serialization_time <- proc.time()	
	meta_data = list(dim = dim(x), length = length(x))
	output = vertica.ddR.serialize(x)
	stop_serialization_time <- proc.time()
	
	meta_data$serialization_time = 
		  	stop_serialization_time - start_serialization_time
	meta_data = vertica.ddR.serialize(meta_data)
	meta_data <- enlarge_meta_data(meta_data,length(output))

	out_data <- data.frame(partition_id = partition_id, 
		 chunk_id = 1:length(output))

	out_data$partition_values = output
	out_data$meta_data = meta_data

	return(out_data)
}


