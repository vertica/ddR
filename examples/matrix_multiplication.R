###################################################################
# Copyright 2015 Hewlett-Packard Development Company, L.P.
# This program is free software; you can redistribute it 
# and/or modify it under the terms of the GNU General Public 
# License, version 2 as published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU 
# General Public License for more details.

# You should have received a copy of the GNU General Public License 
# along with this program; if not, write to the Free Software 
# Foundation, Inc., 59 Temple Place, Suite 330, 
# Boston, MA 02111-1307 USA.
###################################################################
library(dds)

## Uncomment the following two lines to use Distributed R
# library(distributedR.dds)
# useBackend(distributedR)

MatrixMultiply <- function(a,b)
{
	if(nparts(a)[2] != nparts(b)[1])
		stop("'a','b' are not compatible matrices")
	n <- nparts(a)[1]
	m <- nparts(a)[2]
	p <- nparts(b)[2]

        c <- darray(dim=c(n,p),psize=c(1,1),data=0)

	for(i in 1:m)
	{
	      c <- dmapply(function(a,b,c) c+a%*%b, 
	     		parts(a,sapply(0:(n-1),function(x) rep(x*m+i,p))),
			parts(b,rep(1:p,n)+p*(i-1)),
			parts(c),
			output.type = "DArrayClass", 
			combine = "row", nparts = nparts(c))
	}
	return(c)	
}


#Create two 3x3 matrix:  
a<- dmapply(function(i) matrix(i), i = 1:9,
    output.type = "DArrayClass",combine = "row", nparts = c(3,3))
b<- dmapply(function(i) matrix(i), i = 1:9,
    output.type = "DArrayClass",combine = "row", nparts = c(3,3))

print("Multiplying these two matrices: ")
print(collect(a))
print(collect(b))

c<-MatrixMultiply(a,b)
c<-collect(c)

print("Distributed Computation Answer: ")
print(c)

print("Local Computation Answer: ")
print(collect(a) %*% collect(b))
