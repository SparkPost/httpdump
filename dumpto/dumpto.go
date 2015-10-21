package dumpto

import (
	"fmt"
	iou "io/ioutil"
	"log"
	"net/http"
	httpu "net/http/httputil"
	"time"
)

// RequestDumper allows an incoming HTTP request to be stored locally, for more processing later on.
type RequestDumper interface {
	DumpRequest(*Request) error
}

// Request contains the various pieces of one http.Request, packaged up for easy reading or writing.
// The id field is intended to be read-only, to uniquely identify a request to MarkProcessed.
type Request struct {
	ID   *int
	Head []byte
	Data []byte
	When time.Time
}

// RequestLoader reads stored HTTP requests and stores them elsewhere, marking them as processed.
type RequestLoader interface {
	ReadRequest() (*Request, error)
	MarkProcessed(int) error
}

func HandlerFactory(dumper RequestDumper) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		req := &Request{}
		// Get method, path, protocol, and all HTTP headers.
		req.Head, err = httpu.DumpRequest(r, false)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}

		// Get HTTP body.
		defer r.Body.Close()
		req.Data, err = iou.ReadAll(r.Body)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}

		req.When = time.Now()

		err = dumper.DumpRequest(req)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}
	}
}
