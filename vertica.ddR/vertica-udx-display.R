
vertica.ddR.display.factory <- function()
{
	list(name = vertica.ddR.display,
		  udxtype=c("transform"),
		  intype = c("varbinary"),
		  outtype=c("varchar"),
		  outtypecallback = vertica.ddR.display.OutputCallback,
		  outnames = c("output"))
}

vertica.ddR.display.OutputCallback <- function(x)
{
	data.frame(datatype = c("varchar"), 
	   length = c(6400), 
	   scale = c(1), 
	   name = c("output"))

}

vertica.ddR.display <- function(x)
{
	loadNamespace("vertica.ddR")
	combined_args <- c(x[,1],recursive = TRUE)
#	combined_args <- vertica.ddR:::vertica.ddR.deserialize(combined_args)

	output <- "test"
	output <- capture.output(print(x))
	output <- data.frame(output = output)
}