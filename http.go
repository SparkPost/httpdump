package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/yargevad/http/dumpto"
	"github.com/yargevad/http/dumpto/sqlite3"
)

// Command line option declarations.
var port = flag.Int("port", 80, "port to listen for requests")
var dateFmt = flag.String("datefmt", "day", "how much of the date to include in db filename")
var batchInterval = flag.Int("batch-interval", 10, "how often to process stored requests")

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	sqlDumper, err := sqlite3.NewDumper(*dateFmt, "./")
	if err != nil {
		log.Fatal(err)
	}

	reqDumper := dumpto.HandlerFactory(sqlDumper)
	interval := time.Duration(*batchInterval) * time.Second
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					n, err := dumpto.ProcessBatch(sqlDumper)
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
