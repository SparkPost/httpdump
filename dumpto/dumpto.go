package dumpto

import (
	"fmt"
	iou "io/ioutil"
	"log"
	"net/http"
	httpu "net/http/httputil"
	"time"
)

type RequestDumper interface {
	DumpRequest(head, data []byte, now time.Time) error
}

func HandlerFactory(dumper RequestDumper) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get all HTTP headers.
		headBytes, err := httpu.DumpRequest(r, false)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}

		// Get HTTP body.
		defer r.Body.Close()
		bodyBytes, err := iou.ReadAll(r.Body)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}

		now := time.Now()

		err = dumper.DumpRequest(headBytes, bodyBytes, now)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}
	}
}
