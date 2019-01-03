package utils

import (
	"net/http"
)

func HealthChecker(rcsTotal *RequestCountService) func(http.ResponseWriter, *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {
		if rcsTotal.MetricsIsAvailable() {
			w.WriteHeader(http.StatusOK)
		} else {
			// w.WriteHeader(http.StatusServiceUnavailable)
			// w.Write([]byte("503 - Burrow stop sending metrics!"))
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		}
	}
}
