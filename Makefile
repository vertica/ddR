ddR: $(shell find ddR/R -type f)
	R CMD INSTALL ddR
	cd ddR/tests; Rscript test-all.R; cd ../..

algorithms:
	cd algorithms/kmeans.ddR/tests; Rscript test-all.R; cd ../../..
	cd algorithms/glm.ddR/tests; Rscript test-all.R; cd ../../..
	cd algorithms/pagerank.ddR/tests; Rscript test-all.R; cd ../../..
	cd algorithms/randomForest.ddR/tests; Rscript test-all.R; cd ../../..

# Runs all R scripts in the examples directory
#
example: $(shell find examples -type f)
	for f in $^; do Rscript $$f; done
