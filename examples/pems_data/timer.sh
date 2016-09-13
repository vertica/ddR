#!/bin/sh

echo "Sequential version base R\n"
#time Rscript pems_base.R

echo "R's standard parallel library\n"
time Rscript pems_parallel.R

echo "ddR\n"
#time Rscript pems_ddR.R
