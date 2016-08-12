test:
	cd ddR/tests; Rscript test-all.R; cd ../..
	cd algorithms/kmeans.ddR/tests; Rscript test-all.R; cd ../../..
	cd algorithms/glm.ddR/tests; Rscript test-all.R; cd ../../..
	cd algorithms/pagerank.ddR/tests; Rscript test-all.R; cd ../../..
	cd algorithms/randomForest.ddR/tests; Rscript test-all.R; cd ../../..
