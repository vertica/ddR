/********************************************************************
 *A scalable and high-performance platform for R.
 *Copyright (C) [2015] Hewlett-Packard Development Company, L.P.
 *This program is free software; you can redistribute it and/or modify
 *it under the terms of the GNU General Public License as published by
 *the Free Software Foundation; either version 2 of the License, or (at
 *your option) any later version.
 *This program is distributed in the hope that it will be useful, but
 *WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *General Public License for more details.  You should have received a
 *copy of the GNU General Public License along with this program; if
 *not, write to the Free Software Foundation, Inc., 59 Temple Place,
 *Suite 330, Boston, MA 02111-1307 USA
 ********************************************************************/

#include <Rcpp.h>

using namespace Rcpp;

// To be used per dimension
void dimensionIdsAndOffsets(bool row, NumericVector& indices, const NumericMatrix::Column& psizes, 
                            const NumericVector& nparts, std::vector<double>& partNums,
                            std::vector<NumericVector>& offsetNums) {
  
  int num_partitions; 

  if(row) { 
    num_partitions = nparts[0];
  } else {
    num_partitions = nparts[1];
  }

  NumericVector partitionLimits(num_partitions);
  partitionLimits[0] = psizes[0];

  int count = 1;
  int i, stride;
  
  if(row) {
    i = nparts[1];
    stride = nparts[1];
  } else {
    i = 1;
    stride = 1;
  }

  while(count < num_partitions) {
    partitionLimits[count] = partitionLimits[count-1] + psizes[i]; 
    i += stride;
    count++;
  }

  //Binary search to find the smallest partition that could possibly be included
  NumericVector::iterator partitions_start = std::lower_bound(partitionLimits.begin(),partitionLimits.end(),indices[0]);
  if(partitions_start == partitionLimits.end())
	  stop("The index must be exceeding the dimensions of the input object");

  //Binary search to find the largest partition that could possibly be included
  NumericVector::iterator partitions_end = std::lower_bound(partitions_start,partitionLimits.end(),indices[indices.size()-1]);
  if(partitions_end == partitionLimits.end())
    stop("The index must be exceeding the dimensions of the input object");

  partitions_end++;

  NumericVector::const_iterator temp = indices.begin();
  NumericVector::iterator partIt; 

  for(NumericVector::iterator it = partitions_start; it != partitions_end; it++) {
    partIt = std::upper_bound(indices.begin(),indices.end(),*it); 
    if(partIt != temp) {
      partNums.push_back(it-partitionLimits.begin());
      NumericVector offsets(partIt-temp);
      int ind = 0;
      int base;
      while(temp != partIt) {
        if(it == partitionLimits.begin()) {
          base = 0;
        } else {
          base = *(it-1);
        }
        offsets[ind++] = *temp-base;
        temp++;
      }
      offsetNums.push_back(offsets);
    }
    if(partIt == indices.end()) break;
  }
}

//' Gets the internal set of partitions, and offsets within each
//' partition, of a set 1d or 2d-subset indices for a distributed
//' object
//'
//' It returns a list of 3 elements, where the first element is a list
//' of partitions, the second is a list of row indices, and third a
//' a list of column indices.
//'
//' Note: This is an internal helper function of ddR.
//'
//' @param indices A sorted list of sorted vectors, where the first element
//' are the row indices, and the second (if 2d), column indices.
//' @param psizes Partition-sizes matrix of the distributed object.
//' @param nparts nparts vector of the distributed object.
// [[Rcpp::export]]
List getPartitionIdsAndOffsets(List indices, NumericMatrix psizes, NumericVector nparts) {
   
   std::vector<double> rowPartNums;
   std::vector<NumericVector> rowOffsetNums;
    
   std::vector<double> colPartNums;
   std::vector<NumericVector> colOffsetNums;

   if(indices.length() < 0 || indices.length() > 2)
     stop("indices should be a list of length one or two");

   bool two_d = (indices.length() == 2);

   NumericVector rowIndices = NumericVector(indices[0]);

   dimensionIdsAndOffsets(true, rowIndices, psizes(_,0), nparts, rowPartNums, rowOffsetNums);

   if(two_d) {
     NumericVector colIndices = NumericVector(indices[1]);
     dimensionIdsAndOffsets(false, colIndices, psizes(_,1), nparts, colPartNums, colOffsetNums);
   }

   List ret;
 
   std::vector<Rcpp::NumericVector> partition_offsets_row;
   std::vector<Rcpp::NumericVector> partition_offsets_col;
   std::vector<double> partition_ids;

   for(size_t i = 0; i < rowPartNums.size(); i++) {
     double rowPart = rowPartNums[i] * nparts[1];

     if(two_d) {
       for(size_t j = 0; j < colPartNums.size(); j++) {
         partition_ids.push_back(rowPart + colPartNums[j]);
         partition_offsets_col.push_back(colOffsetNums[j]);
         partition_offsets_row.push_back(rowOffsetNums[i]);      
       }
     } else {
         partition_offsets_row.push_back(rowOffsetNums[i]);
         partition_ids.push_back(rowPart);
     }
   }

   unsigned numElem = partition_ids.size();  

   List partitions(numElem);
   List row_offsets(numElem);  

   for(unsigned i = 0; i < numElem; i ++) {
     partitions[i] = partition_ids[i] + 1;
     row_offsets[i] = NumericVector(partition_offsets_row[i]);
   }

   if(!two_d) {
     ret = List::create(partitions,row_offsets);
     ret.names() = CharacterVector::create("partitions", "row_offsets");
   } else {
     List col_offsets(numElem);

     for(unsigned i = 0; i < numElem; i ++) {
       col_offsets[i] = NumericVector(partition_offsets_col[i]);
     }

     ret = List::create(partitions,row_offsets,col_offsets);
     ret.names() = CharacterVector::create("partitions", "row_offsets", "col_offsets");
   }

   return ret;
}
