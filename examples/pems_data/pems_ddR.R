# See pems_common.R for a description of the data and task
#
# Loads `read1`, `pems_files`, and `breaks`
source("pems_common.R")

library(ddR)

# Can try various options with ddR: useBackend(parallel, type = 'PSOCK')


# Distributed station dataframe:
station <- dmapply(read1, pems_files, output.type = "dframe", nparts = c(4, 1),
    combine = "rbind")


allrows <- seq.int(nrow(station))
speed1 <- station[allrows, 5]
speed2 <- station[allrows, 8]

# This operation is not happening in ddR
in50_90 <- 45 <= speed1 & speed1 <= 90 &
           45 <= speed2 & speed2 <= 90 &
           !is.na(speed1) & !is.na(speed2)

# This has been converted to data.frame.  So convert it back.
s <- station[which(in50_90), seq.int(ncol(station))]

# These are 1 x n matrices, aka row vectors
s1 <- as.darray(matrix(s$speed1), psize = c(50000, 1))
s2 <- as.darray(matrix(s$speed2), psize = c(50000, 1))


# More robust version should find its way into ddR
setMethod("-", signature(e1 = "ParallelObj", e2 = "ParallelObj"), function(e1, e2) {
    dmapply(`-`, e1, e2, output.type = "darray", combine = "cbind")
})

delta <- s1 - s2


# Parallelized binning
cut.DObject <- function(x, breaks, labels = NULL) {
    localcut <- function(y) {
        table(cut(y, breaks = breaks, labels = labels))
    }
    tabs <- collect(dlapply(x, localcut))
    Reduce(`+`, tabs)
}

binned <- cut.DObject(delta, breaks, break_names)


pdf('ddR_plot.pdf')
plot(binned)
dev.off()
