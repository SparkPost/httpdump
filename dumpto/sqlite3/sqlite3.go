// Package sqlite3 allows storing http request data in sqlite databases.
package sqlite3

import (
	"database/sql"
	"fmt"
	"log"
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

// TODO: specify path where db file should be created

type SQLiteDumper struct {
	dateFormat    string
	curDate       string
	curDateRWLock *sync.RWMutex
	dbh           *sql.DB
	dbhRWLock     *sync.RWMutex
}

// reopenDBFile opens a database handle and initializes the schema if necessary.
func (ctx *SQLiteDumper) reopenDBFile(dbfile string) error {
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
func (ctx *SQLiteDumper) setCurDate(nowstr string) {
	ctx.curDateRWLock.Lock()
	defer ctx.curDateRWLock.Unlock()
	ctx.curDate = nowstr
}

// getCurDate reads the current value from the curDate global.
func (ctx *SQLiteDumper) getCurDate() string {
	ctx.curDateRWLock.RLock()
	defer ctx.curDateRWLock.RUnlock()
	return fmt.Sprintf("%s", ctx.curDate)
}

// updateCurDate makes sure we're writing to the correct db file.
func (ctx *SQLiteDumper) updateCurDate(now time.Time) error {
	cur := ctx.getCurDate()
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
			return err
		}

	}
	return nil
}

// NewDumper returns an initialized SQLiteDumper that dumps request data to an SQLite db file.
func NewDumper(dateFmt string) (*SQLiteDumper, error) {
	if dateFmt != "day" && dateFmt != "hour" && dateFmt != "minute" {
		return nil, fmt.Errorf("`datefmt` must be one of (`day`, `hour`, `minute`), not [%s]", dateFmt)
	}

	// Set up a dumper, configured with the provided date granularity.
	sqld := &SQLiteDumper{
		dateFormat:    dateFmt,
		curDateRWLock: &sync.RWMutex{},
		dbhRWLock:     &sync.RWMutex{},
	}

	return sqld, nil
}

func (sqld *SQLiteDumper) DumpRequest(head, data []byte, now time.Time) error {
	// Make sure we're using the db file for "right now"
	err := sqld.updateCurDate(now)
	if err != nil {
		return err
	}

	// Get a "read lock" on our db pool.
	sqld.dbhRWLock.RLock()
	defer sqld.dbhRWLock.RUnlock()

	// Insert data for the current request.
	_, err = sqld.dbh.Exec(`
			INSERT INTO raw_requests (head, data, date)
			VALUES ($1, $2, $3)
		`, string(head), string(data), now.Format(time.RFC3339))
	if err != nil {
		return err
	}
	return nil
}
