# This is how I would do the task sequentially in base R

# Loads `read1` function and `pems_files`
source("pems_common.R")


# Read files in sequentially
slist <- lapply(pems_files, read1)

station <- do.call(rbind, slist)

# 1.6 GB
#print(object.size(station), units = "GB")

# 40 million rows x 8 columns
#dim(station)


s1 <- station$speed1
s2 <- station$speed2

# Define the subset
in50_90 <- 45 <= s1 & s1 <= 90 &
           45 <= s2 & s2 <= 90 &
           !is.na(s1) & !is.na(s2)

delta <- s1[in50_90] - s2[in50_90]

# About 2 seconds
system.time(
binned <- cut(delta, breaks, break_names)
)

pdf('base_plot.pdf')
plot(binned)
dev.off()
