library(distributedR.dds)
useBackend(distributedR)

#Matrix Multiplication function using dlists to hold partial matrices
#computes c = a %*% b
#column major format for dlists is assumed similar to R
#a,b are the dlists containing the partitions of the matrices
#nrow_partitions_a, nrow_partitions_b indicate how many parts are in each column
#output is a dlist also in column major format
MatrixMultiply <- function(a, b, nrow_partitions_a, nrow_partitions_b)
{

                #This function converts x,y indexing of matrix to partition index
                get_indices <- function(x,y,nrow) 
                {
                                as.integer(sapply(x, function(i) sapply(y, function(j)
                                                i+(j-1)*nrow )))
                }

                #This function multiplies the matrices and adds to previous result
                local_mult <- function(a,b,c){
                                        list(c[[1]]+a[[1]]%*%b[[1]])}

                #initialize output matrix
                c <- dlist(nparts = nrow_partitions_a * nparts(b)/ nrow_partitions_b)
                c <- dmapply(function(x) {list(0)},parts(c))  

                #C_ik = \sum_j A_ij * B_jk
                #initializing vectors i,k
                #there will be multiple iterations (1 iteration per value of j)
                i = 1:nrow_partitions_a
                k = 1:(nparts(b)/nrow_partitions_b)

                for(index in 1:nrow_partitions_b)
                {
                                j <- rep(index,nrow_partitions_b)
                                indices <-(get_indices(i,j,nrow_partitions_a))
                                indices <- cbind(indices,get_indices(j,k,nrow_partitions_b))
                                indices <- cbind(indices,get_indices(i,k,nrow_partitions_a))
                                indices <- indices[order(indices[,3]),]

                                c <- dmapply(local_mult, parts(a,indices[,1]),
                                                                                 parts(b,indices[,2]),
                                                                                parts(c,indices[,3]))
                }
                return(c)

}

#Create two 3x3 matrix:  
a <- dlist(nparts = 9)
b <- dlist(nparts = 9)
a<- dmapply(function(a,i){list(matrix(i))}, parts(a), i = as.list(1:9))
b<- dmapply(function(b,i){list(matrix(i))}, parts(b), i = as.list(1:9))
nrow_partitions_a = 3
nrow_partitions_b = 3


c<-MatrixMultiply(a,b,nrow_partitions_a,nrow_partitions_b)
c<-collect(c)

c
