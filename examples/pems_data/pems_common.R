# California PEMS data can be downloaded as the txt.gz
# files here: 
# http://www.stat.ucdavis.edu/~clarkf/ 
#
# The goal here is to plot
# the speed differences between the first and second lanes.

# My machine has 4 physical cores and 16 GB memory. The full objects will be 1.3
# GB each, which is a little too big for comfort. Better just read the first two
# lanes in to cut this size in half.


# There are 4 gzipped files here
station_files = list.files("~/data/pems/", full.names = TRUE)

# Read a single one of these
# Vary nrows if you run short on memory
read1 <- function(file, nrows = -1) {
    read.table(file, header = FALSE, sep = ",", nrows = nrows,
        col.names = c("timestamp", "station", "flow1", "occupancy1",
                      "speed1", "flow2", "occupancy2", "speed2",
                      rep("NULL", 18)),
        colClasses = c("character", "factor", "integer", "numeric",
                       "integer", "integer", "numeric", "integer",
                       rep("NULL", 18))
    )
}

# After selecting a subset we'll use these to bin the speed differences
breaks <- c(-Inf, seq(from = -19.5, to = 19.5, by = 3), Inf)
break_names <- as.character(breaks[-c(1, 2, length(breaks))] - 1.5)
break_names <- c("< -20", break_names, "> 20")
