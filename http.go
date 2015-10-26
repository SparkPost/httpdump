package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/yargevad/http/dumpto"
	"github.com/yargevad/http/dumpto/pg"
)

// Command line option declarations.
var port = flag.Int("port", 80, "port to listen for requests")
var batchInterval = flag.Int("batch-interval", 10, "how often to process stored requests")

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	pgDumper := &pg.PgDumper{Db: "postgres"}
	err := pg.DbConnect(pgDumper)
	if err != nil {
		log.Fatal(err)
	}

	reqDumper := dumpto.HandlerFactory(pgDumper)
	interval := time.Duration(*batchInterval) * time.Second
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					n, err := dumpto.ProcessBatch(pgDumper)
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
