# Analysis on local machine California PEMS data can be downloaded as the txt.gz
# files here: 
# http://www.stat.ucdavis.edu/~clarkf/ 
#
# The goal here is to look at
# the speed differences between the first and second lanes.

# My machine has 4 physical cores and 16 GB memory. The full objects will be 1.3
# GB each, which is a little too big for comfort. Better just read the first two
# lanes in to cut this size in half.

station_files <- list.files("~/data/pems/", full.names = TRUE)

# Takes 47 seconds to read a single gzipped file.
system.time({
    station <- read.table(station_files[1], header = FALSE, sep = ",", 
        col.names = c("timestamp", "station", "flow1", "occupancy1",
                      "speed1", "flow2", "occupancy2", "speed2",
                      rep("NULL", 18)),
        colClasses = c("character", "factor", "integer", "numeric",
                       "integer", "integer", "numeric", "integer",
                       rep("NULL", 18)))
})

# 400 MB
print(object.size(station), units = "MB")

# 10 million rows x 8 columns
dim(station)

sapply(station, class)

# Simple questions: 1) Is one lane faster than the the other?  2) Does one lane
# have more traffic than the other?  To answer: 1) Only look at rows that don't
# contain NA's 2) Compare the means of the last 6 columns

s2 <- station[complete.cases(station), -c(1, 2)]

# About 6.4 million
dim(s2)

colMeans(s2)

# A little more formally:
t.test(s2$speed2 - s2$speed1)

# Suppose the traffic is between 50 and 90 mph. Then is one lane faster than the
# other?
in50_90 <- with(s2, 50 <= speed1 & speed1 <= 90 & 50 <= speed2 & speed2 <= 90)

s <- s2[in50_90, ]

# Down to 2.1 million
dim(s)

median(s$speed1)
median(s$speed2)

# So the average speed for fast traffic is about 4.1 mph faster in the 1st lane.
delta <- s$speed1 - s$speed2

t.test(delta)

hist(delta)

# Leave out the long tails
d2 <- delta[abs(delta) < 17]
length(d2)

plot(density(d2, bw = 1))

breaks = 2 * seq.int(-9, 8) + 1

# This one is my favorite plot
hist(d2, freq = FALSE, breaks = breaks)

breaks <- c(-Inf, 3 * seq.int(-5, 5), Inf)
# Too bad hist() doesn't do this...
plot(cut(delta, breaks))
