package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/yargevad/http/sqlite3"
)

// Command line option declarations.
var port = flag.Int("port", 80, "port to listen for requests")
var dateFmt = flag.String("datefmt", "day", "how much of the date to include in db filename")

func main() {
	flag.Parse()

	sqliteDumper, err := sqlite3.HandlerFactory(*dateFmt)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", sqliteDumper)
	portSpec := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(portSpec, nil))
}
