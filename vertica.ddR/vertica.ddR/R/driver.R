setOldClass("vertica_src")

vertica.ddR.env <- new.env()
vertica.ddR.env$dmapply_function_execution_table <- "dmapply_function_execution"
vertica.ddR.env$dmapply_function_execution_view <- "dmapply_function_execution_view"



vertica.ddR.env$vertica.driver.name <- "Vertica.ddR"
vertica.ddR.env$vertica.dobject.name <- "Vertica.dobject"
vertica.ddR.env$vertica.dobject.pointer.name <- "Vertica.dobject.pointer"
vertica.ddR.env$chunk_size <- vertica.ddR.chunk_size <- 64*1000
vertica.ddR.env$long_var_binary <- "varbinary"
vertica.ddR.env$dobject_count <- 0



setClass(vertica.ddR.env$vertica.driver.name, contains="ddRDriver")

#' @export 
vertica <- new(vertica.ddR.env$vertica.driver.name,
	DListClass = vertica.ddR.env$vertica.dobject.name,
	DFrameClass = vertica.ddR.env$vertica.dobject.name,
	DArrayClass = vertica.ddR.env$vertica.dobject.name,
	backendName = "Vertica" )

setMethod("init", vertica.ddR.env$vertica.driver.name, 
	function(x, VerticaDSN = "VerticaDSN", ...)
{
	vertica.ddR.env$dobject_count <- 0
	vertica.ddR.env$vertica_dsn <- src_vertica(VerticaDSN)
	vertica.ddR.env$get_current_dobj <- function() 
		paste("vertica_dobject",vertica.ddR.env$dobject_count,sep = "")
})

setMethod("shutdown", vertica.ddR.env$vertica.driver.name, function(x)
{
	for(i in 1:vertica.ddR.env$dobject_count)
	{
		table_name = paste("vertica_dobject",i,sep="")
		if(db_has_table(vertica.ddR.env$vertica_dsn$con, table_name))
		{
			db_drop_table(vertica.ddR.env$vertica_dsn$con, 
			table_name)
		}
	}
	vertica.ddR.env$dobject_count = 0
})

setMethod("do_dmapply", 
	signature(driver=vertica.ddR.env$vertica.driver.name,func="function"), 
	function(driver,func,...,MoreArgs=list(),
		output.type =  c("dlist","dframe","darray","sparse_darray"),
		nparts=NULL,
 		combine=c("default","c","rbind","cbind")) 
{


	#check if any dobjects is not partitioned by col
	margs = list(...)
	margs = lapply(margs, function(arg) 
	{
		if(is.dobject(arg))
		{
		if(nparts(arg)[length(nparts(arg))] != prod(nparts(arg)))
		{
			if(arg@type == "dlist")
				return(arg)
			if(nparts(arg)[1] == 1)
				return(arg)
			if(arg@type == "darray")
				    skeleton = darray(nparts = totalParts(arg))
			if(arg@type == "sparse_darray")
				    skeleton = darray(nparts = totalParts(arg),
				    	     sparse = TRUE)
			if(arg@type == "dframe")
				    skeleton = dframe(nparts = totalParts(arg))
			skeleton@dim = arg@dim
			cols = seq(0,skeleton@dim[2],
			     length = totalParts(skeleton)+1)
			cols = ceiling(cols)
			cols = cols[-1] - cols[-length(cols)]
			skeleton@psize = cbind(arg@dim[1],cols)
			new_arg = repartition(arg,skeleton)
			return(new_arg) 
		}
		}
		return (arg)
	})
	dobject = simple_dmapply(driver,func,margs,MoreArgs=MoreArgs,
		output.type = output.type, nparts=nparts, combine=combine, 
		ascii = FALSE)
		return(dobject)

	return(dobject)
})

setMethod("combine",signature(driver=vertica.ddR.env$vertica.driver.name,
	items="list"), function(driver,items){

	source_tables = unique(lapply(items, function(x) x@vertica_table$from))
	if(length(source_tables) > 1)
		stop("can not combine partitions from different dobjects")

	partitions = unique(unlist(sapply(items, function(x) x@partitions)))
	
	vertica_table = tbl(vertica_dsn, source_tables[[1]])
	filter(vertica_table, partition_id %in% partitions) 
	
})


simple_dmapply <- function(driver,func, margs, 
		output.type =  c("dlist","dframe","darray","sparse_darray"),
		nparts=NULL, MoreArgs = list(), 
 		combine=c("default","c","rbind","cbind"), ascii = TRUE) 
{
	a <- proc.time()
	parallelism = prod(nparts)
	ntasks = length(margs[[1]])
	tasks_per_instance = seq(from = 0, to = ntasks, length = parallelism+1)
	tasks_per_instance = ceiling(tasks_per_instance)

	if(db_has_table(vertica.ddR.env$vertica_dsn$con, 
		vertica.ddR.env$dmapply_function_execution_table))
	{
		db_drop_table(vertica.ddR.env$vertica_dsn$con, 
			vertica.ddR.env$dmapply_function_execution_table)
	}


	types <- c("integer", "integer", "integer", 
	      paste0(vertica.ddR.env$long_var_binary,"(",
		vertica.ddR.env$chunk_size,")"))
	names(types) <- c("task", "arg", "chunk_id", "val")
	db_create_table(vertica.ddR.env$vertica_dsn$con, 
		vertica.ddR.env$dmapply_function_execution_table, types)


	start_loading_args <- proc.time()

	execution_table <- load_data_into_execution_table(
			func, margs, tasks_per_instance,
			output.type = output.type,
			combine = combine, MoreArgs = MoreArgs,
			ascii = ascii)

	stop_loading_args <- proc.time()
	loading_args_time = (stop_loading_args - start_loading_args)[3]


	if(db_has_table(vertica.ddR.env$vertica_dsn$con, 
		vertica.ddR.env$get_current_dobj()))
	{
		db_drop_table(vertica.ddR.env$vertica_dsn$con, 
			vertica.ddR.env$get_current_dobj())
	}


	start_udx <- proc.time()
	execution_table = tbl(vertica.ddR.env$vertica_dsn,
		  vertica.ddR.env$dmapply_function_execution_view)

	new_dobject <- select(execution_table, ddR_worker(task,arg, 
		       		chunk_id, val, 
				partition = task, order = chunk_id),
				transform.UDF = TRUE, evalNames = TRUE,
				collapse = FALSE)

	compute(new_dobject, name = vertica.ddR.env$get_current_dobj(), 
			     temporary = FALSE)
	stop_udx <- proc.time()
	udx_time <- (stop_udx - start_udx)[3]

	output = list()
	output$vertica_table = tbl(vertica.ddR.env$vertica_dsn, 
			     vertica.ddR.env$get_current_dobj())
	output$nparts = nparts
	output$output.type = output.type
	
	meta_data_col = filter(output$vertica_table,chunk_id == 1)
	meta_data_col = arrange(meta_data_col,partition_id)
	meta_data_col = select(meta_data_col, meta_data)
	meta_data = dplyr::collect(meta_data_col)$meta_data
	meta_data = lapply(meta_data,vertica.ddR.deserialize)

	udx_deserialization_time = 
		sapply(meta_data,function(x) x$deserialization_time)[3,]
	udx_function_execution_time = 
		sapply(meta_data,function(x) x$execution_time)[3,]
	udx_serialization_time = 
		sapply(meta_data,function(x) x$serialization_time)[3,]

	suppressWarnings({
	timing_info <- rbind(loading_args_time, 
		    udx_time, 
		    udx_deserialization_time, 
		    udx_function_execution_time, 
		    udx_serialization_time)})
	#print(timing_info)

	partition_psize = lapply(meta_data, partition_size)
	output$psize = do.call(rbind,partition_psize)
	possible_errors = sapply(partition_psize,length) == 0
	if(any(possible_errors))
	{
		failed_tasks = which(possible_errors)
		error_msgs = filter(output$vertica_table,
			   partition_id == failed_tasks[1])
		error_msgs = arrange(error_msgs,chunk_id)
		error_msgs = select(error_msgs, partition_values)
		error_msgs = dplyr::collect(error_msgs)$partition_values
		error_msgs = vertica.ddR.deserialize(error_msgs)
		error_msgs = paste("Error occured in Vertica UDX:",
			   error_msgs, sep = "\n")
		stop(error_msgs)
	}
	else if(output.type == "dlist")
	{
		output$dim = as.integer(sum(output$psize))
	}
	else
	{
		
		ncol = sum(output$psize[1:nparts[2],2])
		nrow = sum(output$psize[(1:nparts[1]-1)*nparts[2]+1,1])
		output$dim = as.integer(c(nrow,ncol))
	}

	result = new(vertica.ddR.env$vertica.dobject.name, 
	       vertica_table = output$vertica_table,
	       partitions = as.integer(1:prod(output$nparts)),
	       type = output$output.type,
	       nparts = nparts,
	       psize = output$psize,
	       dim = output$dim)


	return(result)
}




load_data_into_execution_table <- function(func, margs, tasks_per_instance,
			       output.type, combine, MoreArgs, ascii = TRUE)
{
	parallelism = length(tasks_per_instance) - 1
	arg_names <- names(margs) 
	execution_params = list()

	execution_table = data.frame(task = integer(0), arg = integer(0), 
			chunk_id = integer(0),
			val = character(0),stringsAsFactors=FALSE)

	for(i in 1:parallelism)
	{
		#assigned_tasks=(tasks_per_instance[i]+1):
			tasks_per_instance[i+1]
		task_params = substitute_task_dobjects(margs, MoreArgs,
			    start_task = tasks_per_instance[i]+1, 
			    end_task = tasks_per_instance[i+1])

		task_args = task_params[[1]]
		task_dobject_list = task_params[[2]]

		task_args$combine = combine
		task_args$output.type = output.type

		task_args <- insert_object_into_execution_table(task_args, 
		      task = i, arg = 0, ascii = ascii)
		task_func <- insert_object_into_execution_table(func, 
		      task = i, arg = -1, ascii = ascii)

		execution_table <- rbind(execution_table, task_args, task_func)

		if(length(task_dobject_list)>0)
		{ 
		for( j in 1:length(task_dobject_list))
		{

			temp <- mutate(task_dobject_list[[j]]@vertica_table, 
					task = i, arg = j, 
					collapse = FALSE)
			execution_params[[length(execution_params)+1]] <- 
				select(temp, task, arg, 
				chunk_id, val = partition_values)
		}
		}
	}

	query = paste("SELECT task, arg, chunk_id, val from ",
	      vertica.ddR.env$dmapply_function_execution_table) 
	queries = sapply(execution_params, function(x) x$query$sql)
	queries = c(query,queries)
	query = paste(queries, collapse = "\n UNION \n")

	query = paste("CREATE OR REPLACE VIEW ", 
	      vertica.ddR.env$dmapply_function_execution_view,
	      " AS ", 
	      query)

	sqlQuery(vertica.ddR.env$vertica_dsn$con@conn, query)

	copy_object_to_table(execution_table, 
		table_name = vertica.ddR.env$dmapply_function_execution_table,
		ascii = ascii)
	return(query)

}

#task_args should be list of args for a given task
substitute_task_dobjects <- function(margs, MoreArgs, start_task, end_task)
{
	task_dobject_list = list()

	MoreArgs = lapply(MoreArgs, function(arg)
	{
		par_env <- parent.env(environment())
		dobject_count=length(par_env$task_dobject_list)

		if(is.dobject(arg))
		{
			assembly_function = function(dobject_list, 
					  required_parts,
					  assemble_from_parts, type, nparts)
			{
				parts = dobject_list[required_parts]
				assemble_from_parts(parts, type, nparts)
			}
			indices = dobject_count + 1:totalParts(arg)
			formals(assembly_function)[["assemble_from_parts"]] = 
				assemble_from_parts
			formals(assembly_function)[["nparts"]] = nparts(arg)
			formals(assembly_function)[["type"]] = arg@type
			formals(assembly_function)[["required_parts"]] = 
				indices
			par_env$task_dobject_list[indices] <-
				parts(arg)
			class(assembly_function) <- 
				vertica.ddR.env$vertica.dobject.pointer.name
			return(assembly_function)
		}
		return(arg)
	})


	task_args = lapply(margs, function(arg, start_task, end_task) 
		  {
			par_env <- parent.env(environment())
			dobject_count=length(par_env$task_dobject_list)

			if(is.dobject(arg))
			{

				temp = necessaryPartitions(arg,
					start_task,end_task)
				required_partitions = temp[[1]]
				assembly_function = temp[[2]]
				par_env$task_dobject_list[dobject_count+
					1:length(required_partitions)] <- 
					required_partitions
				formals(assembly_function)[["required_partitions"]] <-
					dobject_count + 
					1:length(required_partitions)
				class(assembly_function) <- 
				vertica.ddR.env$vertica.dobject.pointer.name
				return(assembly_function)
			}
			else
			{
				return(arg[start_task:end_task])
			}
		  },
		  start_task = start_task,
		  end_task = end_task)

	names(task_args) <- names(margs)
	task_args = c(task_args,MoreArgs = list(MoreArgs))

	.Call("rapply_alt", task_args, function(x) 
	      	{
			par_env <- parent.env(environment()) 		
			dobject_count=length(par_env$task_dobject_list)
			assembly_function=function(dobject_list,
				required_partitions)
			{
				return(dobject_list[[required_partitions]])
			}
			formals(assembly_function)[["required_partitions"]] = 
				dobject_count+1
			par_env$task_dobject_list[dobject_count+1] = list(x)
			class(assembly_function) <- 
				vertica.ddR.env$vertica.dobject.pointer.name
			return(assembly_function)
	      	},
	      	classes = vertica.ddR.env$vertica.dobject.name,
		PACKAGE = "vertica.ddR")

	return(list(task_args, task_dobject_list))

}



insert_object_into_execution_table <- function(object, task, arg, ascii = TRUE)
{
	environment(object) <- globalenv()
	attr(object, ".Environment") <- NULL

	
	object = list(object)
	.Call("rapply_alt",object, function(x) {
		environment(x) <- globalenv()
		y <-unenclose(x);
		environment(y) <- globalenv()
		class(y) <- class(x)
		return(y)
		}, 
		classes = c(vertica.ddR.env$vertica.dobject.pointer.name,
			"function"),
		PACKAGE = "vertica.ddR")
	object = object[[1]]


	serialized_object = vertica.ddR.serialize(object, ascii = ascii)

	local_obj = data.frame(task = task, arg = arg, 
	     chunk_id = 1:length(serialized_object), 
	     stringsAsFactors = FALSE)
	local_obj$val[1:length(serialized_object)] = serialized_object


	return(local_obj)
}



#function should return a list of necessary partitions
#and also a function that combines the partitions and gets the necessary data
necessaryPartitions <- function(dobject, start_index, end_index)
{
	if(is.dlist(dobject))
	{
		partitions = c(0,cumsum(dobject@psize))
		assembly_function <- function(dobject_list,
				  required_partitions, 
				  start_index, end_index)
		{
			required_partitions = dobject_list[required_partitions]
			temp <- required_partitions[[1]]
			temp = temp[start_index:length(temp)]
			required_partitions[[1]] <- temp
			temp<-required_partitions[[length(required_partitions)]]
			temp = temp[1:end_index]
			required_partitions[[length(required_partitions)]]<-temp
			return(do.call(c,required_partitions))
		}
	}
	else if(is.dframe(dobject))
	{
		#assume that dobject is column partitioned
		partitions = c(0,cumsum(dobject@psize[,2]))
		assembly_function <- function(dobject_list,
				  required_partitions, 
				  start_index, end_index)
		{
			required_partitions = dobject_list[required_partitions]
			temp <- required_partitions[[1]]
			temp = temp[start_index:length(temp)]
			required_partitions[[1]] <- temp
			temp<-required_partitions[[length(required_partitions)]]
			temp = temp[1:end_index]
			required_partitions[[length(required_partitions)]]<-temp
			return(do.call(cbind,required_partitions))
		}
	}
	else if(is.darray(dobject))
	{
		#assume that dobject is column partitioned
		partitions = apply(dobject@psize,1,prod)
		partitions = c(0,cumsum(partitions))
		assembly_function <- function(dobject_list,
				  required_partitions, 
				  start_index, end_index)
		{
			required_partitions = dobject_list[required_partitions]
			temp <- required_partitions[[1]]
			temp = temp[start_index:length(temp)]
			required_partitions[[1]] <- temp
			temp<-required_partitions[[length(required_partitions)]]
			temp = temp[1:end_index]
			required_partitions[[length(required_partitions)]]<-temp
			return(unlist(required_partitions))
		}
	}
	
	start_partition = max(which(partitions < start_index))
	end_partition = min(which(partitions >= end_index))-1
	start_index = start_index-partitions[start_partition]
	end_index = end_index - partitions[end_partition]
	required_partitions = parts(dobject,
			    start_partition:end_partition)

	formals(assembly_function)[["start_index"]] = start_index
	formals(assembly_function)[["end_index"]] = end_index

	return(list(required_partitions, assembly_function))
}



copy_object_to_table <- function(object, table_name, 
	temp_file = "/tmp/vertica.ddR.tmp", 
	delimiter = ",", 
	endofline = "vertica_raw_eol",
	ascii = TRUE)
{
	col_names = colnames(object)
	col_names = mapply(function(x, col_name) 
	{
		  if(inherits(x,"list"))
			return(paste(col_name,"format","'hex'"))
		  else return(col_name)
	}, x = object, col_name = col_names)

	col_types = sapply(object,function(x)
	{
		  if(typeof(x) == "double")
		  return("float")
		  if(typeof(x) == "integer")
		  return("int")
		  return(paste0(vertica.ddR.env$long_var_binary,"(",
			vertica.ddR.env$chunk_size,")"))
	})


	if(file.exists(temp_file))
		file.remove(temp_file)

	if(ascii == TRUE)
	{
		write.csv.raw(object, temp_file, delimiter, endofline)
		copy_query = paste0("copy ",table_name,"(",
			   paste(colnames(object),collapse =", ")
	      		   ,")", " from '",temp_file,
			   "' delimiter '",delimiter,
			   "' record terminator '",endofline,"';")
	}
	else
	{
		.Call("write_csv_raw",object, temp_file,
				delimiter, endofline,
				PACKAGE = "vertica.ddR")
		copy_query = paste0("copy ",table_name,"(",
			   paste(col_names,collapse =", ")
	      		   ,")", " from '",temp_file,
			   "' delimiter '",delimiter,
			   "' record terminator '",endofline,"';")
	}

	con = vertica.ddR.env$vertica_dsn$con@conn

	query = paste0("create table ", table_name,"(",
	      paste(names(object),col_types,collapse = ", "),")",
	      " segmented by ",names(object)[1]," ALL NODES;")
	sqlQuery(con, query)

	sqlQuery(con, copy_query)
}

write.csv.raw <- function(object, filename, delimiter, endline)
{
	to.write = file(filename, "wb")
	for( i in 1:nrow(object))
	{
		for( j in 1:ncol(object))
		{
			if(class(object[[j]]) != "list")
				cat(as.integer(object[[j]][[i]]),file = to.write)
			else
				writeBin(object[[j]][[i]],to.write)
			if(j != ncol(object))
			    	cat(delimiter,file = to.write)
			else	
				cat(endline,file = to.write)
		}
 
	}
	close(to.write)
}


