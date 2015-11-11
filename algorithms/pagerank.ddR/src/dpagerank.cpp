/********************************************************************
 *  Functions related to the dpagerank algorithm
 ********************************************************************/

#include <Rcpp.h>
#include <Rinternals.h>

using namespace std;
using namespace Rcpp;

static SEXP RSymbol_dim = NULL;
static SEXP RSymbol_Dim = NULL;
static SEXP RSymbol_i = NULL;
static SEXP RSymbol_j = NULL;
static SEXP RSymbol_p = NULL;
static SEXP RSymbol_x = NULL;

#define INSTALL_SYMBOL(symbol) \
  if (RSymbol_##symbol == NULL) { \
    RSymbol_##symbol = Rf_install(#symbol); \
  }

/*
** The calculation in each iteration of pagerank when the graph (mx) is sparse
**** Input arguments
** PR: the old pagerank vector
** mx: a split of the graph
** TP: the number of outgoing edges of each vertex
** damping: damping factor
** personalized: personalized vector
** weights: matrix of weights
**** Output argument
** newPR: the new pagerank vector
*/
RcppExport SEXP pagerank_spvm(SEXP PR, SEXP mx, SEXP TP, SEXP damping, SEXP personalized, SEXP weights) {
  BEGIN_RCPP
  if (RSymbol_dim == NULL)
    RSymbol_dim = R_DimSymbol;
  INSTALL_SYMBOL(Dim);
  INSTALL_SYMBOL(i);
  INSTALL_SYMBOL(p);
  INSTALL_SYMBOL(x);

  SEXP dim = Rf_getAttrib(mx, RSymbol_Dim);
  int nVertices = INTEGER(dim)[0]; // the splits are column-wise partitioned, so #rows == nVertices
  int y = INTEGER(dim)[1];

  NumericMatrix newPR(1, y);

  int *dim_pr = INTEGER(Rf_getAttrib(PR, R_DimSymbol));
  if (dim_pr[0] != 1 || dim_pr[1] != nVertices)
    Rcpp::stop("The dimensions of PR are not correct");
  int *dim_tp = INTEGER(Rf_getAttrib(TP, R_DimSymbol));
  if (dim_tp[0] != nVertices || dim_tp[1] != 1)
    Rcpp::stop("The dimensions of TP are not correct");

  bool no_Per = Rf_isNull(personalized);
  double *personalized_vec;
  if (! no_Per) {
    int *dim_persona = INTEGER(Rf_getAttrib(personalized, R_DimSymbol));
    if (dim_persona[0] != 1 || dim_persona[1] != y)
      Rcpp::stop("The dimensions of personalized are not correct");
    personalized_vec = REAL(personalized); // personlized vector
  }

  bool no_weight = Rf_isNull(weights);
  double *weights_vec;
  int *iw_vec;
  int *pw_vec;
  if (! no_weight) {
    int *dim_weights = INTEGER(Rf_getAttrib(weights, RSymbol_Dim));
    if (dim_weights[0] != nVertices || dim_weights[1] != y)
      Rcpp::stop("The dimensions of weights are not correct");
    weights_vec = REAL(Rf_getAttrib(weights, RSymbol_x)); // weights matrix
    iw_vec = INTEGER(Rf_getAttrib(weights, RSymbol_i));
    pw_vec = INTEGER(Rf_getAttrib(weights, RSymbol_p));
  }

  double damp = Rcpp::as<double>(damping);

  double *v1 = REAL(PR);
  double *v2 = REAL(newPR);
  double *tp = REAL(TP);
  double *x_vec = REAL(Rf_getAttrib(mx, RSymbol_x));
  int *i_vec = INTEGER(Rf_getAttrib(mx, RSymbol_i));
  int *p_vec = INTEGER(Rf_getAttrib(mx, RSymbol_p));
  double tempRes;

  for (int i = 0; i < y; i++) {
    v2[i] = 0;
    for (int j = p_vec[i]; j < p_vec[i+1]; j++) {
      tempRes = x_vec[j] * v1[i_vec[j]];

      if (! no_weight) {
        bool found = false;
        // to find the corresponding element of weights
        for (int jw = pw_vec[i]; jw < pw_vec[i+1]; jw++) {
            if (iw_vec[jw] == i_vec[j]) {
                tempRes *= weights_vec[jw];
                found = true;
                break;
            }
        }
        if (! found)
            tempRes = 0;
      }

      if (tp[i_vec[j]] != 0)
        tempRes /= tp[i_vec[j]];

      v2[i] +=  tempRes ;
    }
    v2[i] *= damp;
    if (no_Per) {
      v2[i] += ((1 - damp) / nVertices);
    } else {
      v2[i] += ((1 - damp) * personalized_vec[i]);
    }
  }
  return newPR;
  END_RCPP
}

/*
** The calculation in each iteration of pagerank when the graph (mx) is dense
**** Input arguments
** PR: the old pagerank vector
** mx: a split of the graph
** TP: the number of outgoing edges of each vertex
** damping: damping factor
** personalized: personalized vector
** weights: matrix of weights
**** Output argument
** newPR: the new pagerank vector
*/
RcppExport SEXP pagerank_vm(SEXP PR, SEXP mx, SEXP TP, SEXP damping, SEXP personalized, SEXP weights) {
  BEGIN_RCPP

  SEXP dim = Rf_getAttrib(mx, R_DimSymbol);
  int nVertices = INTEGER(dim)[0]; // the splits are column-wise partitioned, so #rows == nVertices
  int y = INTEGER(dim)[1];

  NumericMatrix newPR(1, y);

  int *dim_pr = INTEGER(Rf_getAttrib(PR, R_DimSymbol));
  if (dim_pr[0] != 1 || dim_pr[1] != nVertices)
    Rcpp::stop("The dimensions of PR are not correct");
  int *dim_tp = INTEGER(Rf_getAttrib(TP, R_DimSymbol));
  if (dim_tp[0] != nVertices || dim_tp[1] != 1)
    Rcpp::stop("The dimensions of TP are not correct");

  bool no_Per = Rf_isNull(personalized);
  double *personalized_vec;
  if (! no_Per) {
    int *dim_persona = INTEGER(Rf_getAttrib(personalized, R_DimSymbol));
    if (dim_persona[0] != 1 || dim_persona[1] != y)
      Rcpp::stop("The dimensions of personalized are not correct");
    personalized_vec = REAL(personalized); // personlized vector
  }

  bool no_weight = Rf_isNull(weights);
  double *weights_mx;
  if (! no_weight) {
    int *dim_weights = INTEGER(Rf_getAttrib(weights, R_DimSymbol));
    if (dim_weights[0] != nVertices || dim_weights[1] != y)
      Rcpp::stop("The dimensions of weights are not correct");
    weights_mx = REAL(weights); // weights matrix
  }

  double damp = Rcpp::as<double>(damping);
  double *v1 = REAL(PR);
  double *v2 = REAL(newPR);
  double *tp = REAL(TP);

  mx = Rf_coerceVector(mx, INTSXP);
  int *x_vec = INTEGER(mx);
  double tempRes;

  for (int i = 0; i < y; i++) {
    v2[i] = 0;
    for (int j = 0; j < nVertices; j++) {
      int k = j+ i * nVertices;
      tempRes = x_vec[k] * v1[j];

      if (! no_weight)
        tempRes *= weights_mx[k];
      if (tp[j] != 0)
        tempRes /= tp[j];

      v2[i] +=  tempRes ;
    }
    v2[i] *= damp;
    if (no_Per) {
      v2[i] += ((1 - damp) / nVertices);
    } else {
      v2[i] += ((1 - damp) * personalized_vec[i]);
    }

  }
  return newPR;
  END_RCPP
}

