package main

import (
	"bytes"
	"flag"
	"fmt"
	iou "io/ioutil"
	"log"
	"net/http"
	httpu "net/http/httputil"
	"os"
	re "regexp"
	"time"

	"github.com/SparkPost/httpdump/storage"
	"github.com/SparkPost/httpdump/storage/pg"
)

// Command line option declarations.
var port = flag.Int("port", 80, "port to listen for requests")
var batchInterval = flag.Int("batch-interval", 10, "how often to process stored requests")

// Loggly contains all the information needed to submit messages.
type Loggly struct {
	Endpoint string
	Client   *http.Client
	BatchMax int64
	EventMax int64
	buf      *bytes.Buffer
}

// SendRequest does a POST to Loggly with the provided data.
func (l *Loggly) SendRequest() error {
	reqLen := len(l.buf.String())
	req, err := http.NewRequest("POST", l.Endpoint, l.buf)
	if err != nil {
		return err
	}

	reqDump, err := httpu.DumpRequestOut(req, true)
	if err != nil {
		return err
	}

	res, err := l.Client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		resHeaders, err := httpu.DumpResponse(res, false)
		if err != nil {
			return err
		}
		resBody, err := iou.ReadAll(res.Body)
		if err != nil {
			return err
		}
		log.Printf("%s\n\n%s%s\n", string(reqDump), string(resHeaders), string(resBody))

	} else {
		log.Printf("Sent %d bytes with status %s\n", reqLen, res.Status)
	}

	return nil
}

var lineBreak *re.Regexp = re.MustCompile(`\r?\n`)

// ProcessRequests formats storage.Request objects on one line and
// submits to Loggly in appropriately-sized batches.
func (l *Loggly) ProcessRequests(reqs []storage.Request) error {
	var size, esize int64
	for _, req := range reqs {
		head := lineBreak.ReplaceAll(req.Head, []byte(`\n`))
		data := lineBreak.ReplaceAll(req.Data, []byte(`\n`))
		esize = int64(len(head) + len(data))

		if esize > l.EventMax {
			log.Printf("WARNING: event size %d > event max %d\n%s%s\n",
				esize, l.EventMax, string(head), string(data))
			continue
		}

		l.buf.Write(head)
		l.buf.Write(data)
		if (size + esize) > l.BatchMax {
			err := l.SendRequest()
			if err != nil {
				return err
			}
			l.buf.Reset()
		}

		size += esize
	}

	if size > 0 {
		err := l.SendRequest()
		if err != nil {
			return err
		}
	}

	return nil
}

var uuid *re.Regexp = re.MustCompile(`^[0-9a-f]{8}\-(?:[0-9a-f]{4}\-){3}[0-9a-f]{12}$`)
var word *re.Regexp = re.MustCompile(`^\w*$`)
var pass *re.Regexp = re.MustCompile(`^\S*$`)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	// Env vars we'll be checking for, mapped to the regular expressions
	// we'll use to validate their values.
	envVars := map[string]*re.Regexp{
		"LOGGLY_TOKEN":      uuid,
		"POSTGRESQL_DB":     word,
		"POSTGRESQL_USER":   word,
		"POSTGRESQL_PASS":   pass,
		"POSTGRESQL_SCHEMA": word,
	}
	opts := map[string]string{}
	for k, v := range envVars {
		opts[k] = os.Getenv(k)
		if !v.MatchString(opts[k]) {
			log.Fatalf("Unexpected value for %s, double check your parameters.", k)
		}
	}

	// Configure the PostgreSQL dumper.
	pgDumper := &pg.PgDumper{
		Db:     opts["POSTGRESQL_DB"],
		Schema: opts["POSTGRESQL_SCHEMA"],
		User:   opts["POSTGRESQL_USER"],
		Pass:   opts["POSTGRESQL_PASS"],
	}
	err := pg.DbConnect(pgDumper)
	if err != nil {
		log.Fatal(err)
	}

	// Configure the Loggly processor
	loggly := &Loggly{
		Endpoint: fmt.Sprintf("https://logs-01.loggly.com/bulk/%s/tag/bulk/", opts["LOGGLY_TOKEN"]),
		Client:   &http.Client{},
		BatchMax: 5 * 1024 * 1024,
		EventMax: 1024 * 1024,
	}
	loggly.buf = bytes.NewBuffer(make([]byte, 0, loggly.BatchMax))

	// Set up our handler which writes to, and reads from PostgreSQL.
	reqDumper := storage.HandlerFactory(pgDumper)

	// Start up recurring job to process events stored in PostgreSQL.
	interval := time.Duration(*batchInterval) * time.Second
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					_, err := storage.ProcessBatch(pgDumper, loggly)
					if err != nil {
						log.Printf("%s\n", err)
					}
				}()
			}
		}
	}()

	// Spin up HTTP listener on the requested port.
	http.HandleFunc("/", reqDumper)
	portSpec := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(portSpec, nil))
}
