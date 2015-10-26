package dumpto

import (
	"fmt"
	iou "io/ioutil"
	"log"
	"net/http"
	httpu "net/http/httputil"
	"time"
)

// Dumper allows an incoming HTTP request to be stored locally, for more processing later on.
type Dumper interface {
	Dump(*Request) error
}

// Request contains the various pieces of one http.Request, packaged up for easy reading or writing.
// The id field is intended to be read-only, to uniquely identify a request to Batcher.BatchDone.
type Request struct {
	ID    *int64
	Head  []byte
	Data  []byte
	When  time.Time
	Batch *int
}

func (req *Request) String() string {
	var idStr, batchStr string
	if req.ID == nil {
		idStr = "(nil)"
	} else {
		idStr = fmt.Sprintf("%d", *req.ID)
	}
	if req.Batch == nil {
		batchStr = "(nil)"
	} else {
		batchStr = fmt.Sprintf("%d", *req.Batch)
	}

	return fmt.Sprintf("ID:\t%s\nHead:\n%sWhen:\t%s\nBatch:\t%s\n",
		idStr, string(req.Head), req.When.Format(time.RFC3339), batchStr)
}

// Batcher reads stored HTTP requests in a batch, marking them as processed when done.
type Batcher interface {
	MarkBatch() (batchID int64, err error)
	ReadRequests(batchID int64) (reqs []Request, err error)
	BatchDone(batchID int64) error
}

// Processor takes a list of HTTP requests and processes them somehow.
type Processor interface {
	ProcessRequests(reqs []Request) error
}

type DumpBatcher interface {
	Dumper
	Batcher
}

func ProcessBatch(b Batcher, p Processor) (int, error) {
	batchID, err := b.MarkBatch()
	if err != nil {
		return 0, err
	}
	if batchID == 0 {
		return 0, nil
	}

	reqs, err := b.ReadRequests(batchID)
	if err != nil {
		return 0, err
	}
	if len(reqs) == 0 {
		return 0, nil
	}

	err = p.ProcessRequests(reqs)
	if err != nil {
		return 0, err
	}

	err = b.BatchDone(batchID)
	if err != nil {
		return 0, err
	}

	return len(reqs), nil
}

func HandlerFactory(d Dumper) func(http.ResponseWriter, *http.Request) {
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

		err = d.Dump(req)
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}

	}
}
