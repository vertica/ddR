# See pems_common.R for a description of the data and task
#
# Loads `read1`, `station_files`, and `breaks`
source("pems_common.R")

library(ddR)

# Can try various options with ddR: useBackend(parallel, type = 'PSOCK')


system.time({
    # Distributed station dataframe:
    ds <- dmapply(read1, station_files, output.type = "dframe", nparts = c(4, 1),
        combine = "rbind")
})

# About 15 seconds
system.time({
    ds1 = collect(ds, 1)
})

# Why is this 2 GB? It should be around 400 MB. Looks like row names are to blame.
print(object.size(ds1), units = "GB")

# Now translate the base R workflow into ddR

allrows <- seq.int(nrow(ds))
speed1 <- ds[allrows, 5]
speed2 <- ds[allrows, 8]
in50_90 <- 50 <= speed1 & speed1 <= 90 &
           50 <= speed2 & speed2 <= 90 &
           !is.na(speed1) & !is.na(speed2)

# This has been converted to data.frame.  So convert it back.
s <- ds[which(in50_90), seq.int(ncol(ds))]

# These are 1 x n matrices, aka row vectors
s1 <- as.darray(matrix(s$speed1), psize = c(50000, 1))
s2 <- as.darray(matrix(s$speed2), psize = c(50000, 1))


# More robust version should find its way into ddR
setMethod("-", signature(e1 = "ParallelObj", e2 = "ParallelObj"), function(e1, e2) {
    dmapply(`-`, e1, e2, output.type = "darray", combine = "cbind")
})


delta <- s1 - s2


# Parallelized binning for histogram like plot
cut.DObject <- function(x, breaks, labels = NULL) {
    localcut <- function(y) {
        table(cut(y, breaks = breaks, labels = labels))
    }
    tabs <- collect(dlapply(x, localcut))
    Reduce(`+`, tabs)
}


dtable <- cut.DObject(delta, breaks)

# Shows the distribution of speed differences
plot(dtable)
