# California PEMS data can be downloaded as the txt.gz files here:
# http://www.stat.ucdavis.edu/~clarkf/
#
# Duplicating the analysis from base R, but this time using ddR for
# computation.

library(ddR)

# Trying various options with ddR:
#useBackend(parallel, type = "PSOCK")

# There are 4 gzipped files here
station_files = list.files("~/data/pems/", full.names = TRUE)

read1 <- function(file){
    read.table(file, header = FALSE, sep = ",", row.names = NULL
        , col.names = c("timestamp", "station", "flow1", "occupancy1", "speed1"
                        , "flow2", "occupancy2", "speed2" , rep("NULL", 18))
        , colClasses = c("character", "factor", "integer", "numeric", "integer"
                        , "integer", "numeric", "integer", rep("NULL", 18))
        #, nrows = 1e6L # Uncomment this if you're running short on memory
    )
}


system.time({
# Distributed station dataframe:
ds <- dmapply(read1, station_files, output.type = "dframe"
              , nparts = c(4, 1),  combine = "rbind"
              )
})

# About 15 seconds
system.time({
ds1 = collect(ds, 1)
})

# Why is this 2 GB? It should be around 400 MB
# Looks like row names are to blame.
print(object.size(ds1), units="GB")

# Now translate the base R workflow into ddR

allrows = seq.int(nrow(ds))
speed1 = ds[allrows, 5]
speed2 = ds[allrows, 8]
in50_90 <- 50 <= speed1 & speed1 <= 90 &
           50 <= speed2 & speed2 <= 90 &
           !is.na(speed1) & !is.na(speed2)

# This has been converted to data.frame.
# So convert it back.
s = ds[which(in50_90), seq.int(ncol(ds))]

# These are 1 x n matrices, aka row vectors
s1 = as.darray(matrix(s$speed1), psize = c(50000, 1))
s2 = as.darray(matrix(s$speed2), psize = c(50000, 1))


# More robust version should find its way into ddR
setMethod("-", signature(e1="ParallelObj", e2="ParallelObj"),
function(e1, e2){
    dmapply(`-`, e1, e2, output.type = "darray", combine = "cbind")
})


delta = s1 - s2


# Parallelized binning for histogram like plot
cut.DObject = function(x, breaks, labels = NULL){
    localcut = function(y){
        table(cut(y, breaks = breaks, labels = labels))
    }
    tabs = collect(dlapply(x, localcut))
    Reduce(`+`, tabs)
}


breaks = c(-Inf, 3 * seq.int(-5, 5), Inf)
dtable = cut.DObject(delta, breaks)

# Shows the distribution of speed differences
plot(dtable)
