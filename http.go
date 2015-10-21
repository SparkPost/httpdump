package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/yargevad/http/dumpto"
	"github.com/yargevad/http/dumpto/sqlite3"
)

// Command line option declarations.
var port = flag.Int("port", 80, "port to listen for requests")
var dateFmt = flag.String("datefmt", "day", "how much of the date to include in db filename")

func main() {
	flag.Parse()

	sqlDumper, err := sqlite3.NewDumper(*dateFmt, "./")
	if err != nil {
		log.Fatal(err)
	}

	reqDumper := dumpto.HandlerFactory(sqlDumper)

	http.HandleFunc("/", reqDumper)
	portSpec := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(portSpec, nil))
}
