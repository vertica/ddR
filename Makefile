# This Makefile has no purpose in actual installation, it is only for
# convenience and automation when developing, ie run the bash command
# $ make ddR 
# from this directory to install the local version and run all the tests


ALGOS = $(shell find algorithms -d 1)


# roxygen generates the docs first because it may modify NAMESPACE.
#
ddR: $(shell find ddR/R -type f)
	R -q -e "roxygen2::roxygenize('ddR')"
	R CMD INSTALL ddR
	cd ddR/tests; Rscript test-all.R; cd ../..


pdf: $(shell find ddR/man -type f)
	R CMD Rd2pdf ddR


# Run all the tests for the algorithms
#
test_algorithms: $(ALGOS)
	for d in $^; do cd $$d/tests; Rscript test-all.R; cd ../../..; done


# Runs all R scripts in the examples directory
# Could improve this to detect errors
#
example: $(shell find examples -type f)
	for f in $^; do Rscript $$f; done
