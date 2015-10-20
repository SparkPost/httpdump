// Package sqlite3 allows storing http request data in sqlite databases.
package sqlite3

import (
	"database/sql"
	"fmt"
	iou "io/ioutil"
	"log"
	"net/http"
	httpu "net/http/httputil"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Map shortcuts strings to verbose date formats.
var DateFormats = map[string]string{
	"day":    "2006-01-02T-MST",
	"hour":   "2006-01-02T15-MST",
	"minute": "2006-01-02T15-04-MST",
}

type context struct {
	dateFormat    string
	curDate       string
	curDateRWLock *sync.RWMutex
	dbh           *sql.DB
	dbhRWLock     *sync.RWMutex
}

// reopenDBFile opens a database handle and initializes the schema if necessary.
func (ctx *context) reopenDBFile(dbfile string) error {
	mustInit := false
	file, err := os.Open(dbfile)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// DB file didn't exist, we need to init schema.
		mustInit = true
	} else {
		file.Close()
	}

	ctx.dbh, err = sql.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}

	if mustInit {
		log.Printf("Initializing schema for [%s]\n", dbfile)
		_, err := ctx.dbh.Exec(`
			CREATE TABLE raw_requests (head blob, data blob, date text)
		`, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// setCurDate writes a new value into the curDate global.
func (ctx *context) setCurDate(nowstr string) {
	ctx.curDateRWLock.Lock()
	defer ctx.curDateRWLock.Unlock()
	ctx.curDate = nowstr
}

// getCurDate reads the current value from the curDate global.
func (ctx *context) getCurDate() string {
	ctx.curDateRWLock.RLock()
	defer ctx.curDateRWLock.RUnlock()
	return fmt.Sprintf("%s", ctx.curDate)
}

// updateCurDate makes sure we're writing to the correct db file.
func (ctx *context) updateCurDate() (time.Time, error) {
	cur := ctx.getCurDate()
	now := time.Now()
	nowstr := now.Format(DateFormats[ctx.dateFormat])
	// If the date has changed since the last time we checked, open the new file.
	if cur != nowstr {
		ctx.setCurDate(nowstr)
		dbfile := fmt.Sprintf("%s.db", nowstr)
		var err error
		ctx.dbhRWLock.Lock()
		defer ctx.dbhRWLock.Unlock()
		if ctx.dbh != nil {
			ctx.dbh.Close()
		}
		err = ctx.reopenDBFile(dbfile)
		if err != nil {
			return now, err
		}

	}
	return now, nil
}

// HandlerFactory returns an http.HandlerFunc that dumps request data to an SQLite db file.
func HandlerFactory(dateFmt string) (func(http.ResponseWriter, *http.Request), error) {
	if dateFmt != "day" && dateFmt != "hour" && dateFmt != "minute" {
		return nil, fmt.Errorf("`datefmt` must be one of (`day`, `hour`, `minute`), not [%s]", dateFmt)
	}

	// Set up a context, configured with the date granularity param.
	ctx := &context{
		dateFormat:    dateFmt,
		curDateRWLock: &sync.RWMutex{},
		dbhRWLock:     &sync.RWMutex{},
	}

	// Close over the context.
	handler := func(w http.ResponseWriter, r *http.Request) {
		// Make sure we're using the db file for "right now"
		now, err := ctx.updateCurDate()
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}

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

		// Get a "read lock" on our db pool.
		ctx.dbhRWLock.RLock()
		defer ctx.dbhRWLock.RUnlock()

		// Insert data for the current request.
		_, err = ctx.dbh.Exec(`
			INSERT INTO raw_requests (head, data, date)
			VALUES ($1, $2, $3)
		`, string(headBytes), string(bodyBytes), now.Format(time.RFC3339))
		if err != nil {
			log.Printf("%s\n", err)
			http.Error(w, fmt.Sprintf("%s", err), http.StatusInternalServerError)
			return
		}
	}

	return handler, nil
}
