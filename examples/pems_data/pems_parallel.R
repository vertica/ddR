# Use R's built in parallel library

# Loads `read1` function and `pems_files`
source("pems_common.R")

ncores <- parallel::detectCores(logical = FALSE)

# Read files in parallel
slist <- parallel::mclapply(pems_files, read1, mc.cores = ncores)

station <- do.call(rbind, slist)

s1 <- station$speed1
s2 <- station$speed2

# Define the subset
in50_90 <- 45 <= s1 & s1 <= 90 &
           45 <= s2 & s2 <= 90 &
           !is.na(s1) & !is.na(s2)

delta <- s1[in50_90] - s2[in50_90]

# Parallelized version of cut
pcut <- function(x, breaks, labels = NULL, mc.cores = ncores) {
    localcut <- function(y) {
        table(cut(y, breaks = breaks, labels = labels))
    }
    n <- length(x)
    # For `fork` systems it makes sense to have data structures like the
    # one created here: the base objects augmented with a split for
    # parallelization
    sp <- rep_len(seq.int(ncores), length.out = n)
    grouped <- split(x, sp)
    tabs <- parallel::mclapply(grouped, localcut, mc.cores = mc.cores)
    Reduce(`+`, tabs)
}

# About 1 second
system.time(
binned <- pcut(delta, breaks, break_names)
)

pdf('parallel_plot.pdf')
plot(binned)
dev.off()
