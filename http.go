package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	re "regexp"
	"time"

	"github.com/yargevad/http/dumpto"
	"github.com/yargevad/http/dumpto/pg"
)

// Command line option declarations.
var port = flag.Int("port", 80, "port to listen for requests")
var batchInterval = flag.Int("batch-interval", 10, "how often to process stored requests")

type Loggly struct {
	Endpoint string
	Client   *http.Client
	BatchMax int64
	EventMax int64
}

var lineBreak *re.Regexp = re.MustCompile(`\r?\n`)

func (l *Loggly) ProcessRequests(reqs []dumpto.Request) error {
	size := 0
	for _, req := range reqs {
		head := lineBreak.ReplaceAll(req.Head, []byte(`\n`))
		data := lineBreak.ReplaceAll(req.Data, []byte(`\n`))
		size += len(head) + 1 + len(data)
		log.Printf("[%s] [%s]\n", head, data)
		return fmt.Errorf("[%s]", head)
	}
	return nil
}

var uuid *re.Regexp = re.MustCompile(`^[0-9a-f]{8}\-(?:[0-9a-f]{4}\-){3}[0-9a-f]{12}$`)
var word *re.Regexp = re.MustCompile(`^\w*$`)
var pass *re.Regexp = re.MustCompile(`^\S*$`)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	envVars := map[string]*re.Regexp{
		//"LOGGLY_TOKEN":      uuid,
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

	loggly := &Loggly{
		Endpoint: fmt.Sprintf("https://logs-01.loggly.com/bulk/%s/tag/bulk/", opts["LOGGLY_TOKEN"]),
		Client:   &http.Client{},
		BatchMax: 5 * 1024 * 1024,
		EventMax: 1024 * 1024,
	}

	reqDumper := dumpto.HandlerFactory(pgDumper)
	interval := time.Duration(*batchInterval) * time.Second
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					n, err := dumpto.ProcessBatch(pgDumper, loggly)
					if err != nil {
						log.Printf("%s\n", err)
					} else {
						log.Printf("Processed %d items\n", n) // DEBUG
					}
				}()
			}
		}
	}()

	http.HandleFunc("/", reqDumper)
	portSpec := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(portSpec, nil))
}
