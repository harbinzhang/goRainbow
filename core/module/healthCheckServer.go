package module

import (
	"net/http"
)

func HealthChecker(cc *CountService) func(http.ResponseWriter, *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {
		if cc.IsCountServiceAvailable() {
			w.WriteHeader(http.StatusOK)
		} else {
			// w.WriteHeader(http.StatusServiceUnavailable)
			// w.Write([]byte("503 - Burrow stop sending metrics!"))
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		}
	}
}
