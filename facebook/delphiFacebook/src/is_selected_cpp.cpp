#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
LogicalVector is_selected_cpp(List responses, String target) {
  LogicalVector out(responses.size());

  for (int i = 0; i < responses.size(); ++i) {
    if (responses[i] == R_NilValue) {
      out[i] = NA_LOGICAL;
      continue;
    }

    StringVector response(responses[i]);
    if (response.size() == 0) {
      out[i] = NA_LOGICAL;
      continue;
    }

    for (int j = 0; j < response.size(); ++j ) {
      if (StringVector::is_na(response[j])) {
        out[i] = NA_LOGICAL;
        break;
      }
      if (response[j] == target) {
        out[i] = true;
        break;
      }
    }
  }
  return out;
}