// Package sqlite3 allows storing http request data in sqlite databases.
package sqlite3

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/yargevad/http/dumpto"
)

// Map shortcuts strings to verbose date formats.
var DateFormats = map[string]string{
	"day":    "2006-01-02T-MST",
	"hour":   "2006-01-02T15-MST",
	"minute": "2006-01-02T15-04-MST",
}

// TODO: no date in db filename, reuse same file for every event, delete as they're processed

type SQLiteDumper struct {
	dbPath        string
	dateFormat    string
	curDate       string
	curDateRWLock *sync.RWMutex
	dbh           *sql.DB
	dbhRWLock     *sync.RWMutex
}

// reopenDBFile opens a database handle and initializes the schema if necessary.
func (ctx *SQLiteDumper) reopenDBFile(dbfile string) error {
	mustInit := false
	filePath := fmt.Sprintf("%s/%s", ctx.dbPath, dbfile)
	file, err := os.Open(filePath)
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
			CREATE TABLE raw_requests (
				id    integer primary key autoincrement,
				head  blob,
				data  blob,
				date  timestamp,
				batch int
			)
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
func NewDumper(dateFmt string, dbPath string) (*SQLiteDumper, error) {
	// TODO: if dateFmt matches "\w+.db", use that as the db filename
	if dateFmt != "day" && dateFmt != "hour" && dateFmt != "minute" {
		return nil, fmt.Errorf("`datefmt` must be one of (`day`, `hour`, `minute`), not [%s]", dateFmt)
	}

	// Set up a dumper, configured with the provided date granularity.
	sqld := &SQLiteDumper{
		dbPath:        dbPath,
		dateFormat:    dateFmt,
		curDateRWLock: &sync.RWMutex{},
		dbhRWLock:     &sync.RWMutex{},
	}

	// Make sure we're using the db file for "right now"
	err := sqld.updateCurDate(time.Now())
	if err != nil {
		return nil, err
	}

	return sqld, nil
}

func (sqld *SQLiteDumper) Dump(req *dumpto.Request) error {
	// Get a "read lock" on our db pool.
	sqld.dbhRWLock.RLock()
	defer sqld.dbhRWLock.RUnlock()

	// Insert data for the current request.
	_, err := sqld.dbh.Exec(`
			INSERT INTO raw_requests (head, data, date)
			VALUES ($1, $2, $3)
		`, string(req.Head), string(req.Data), req.When)
	if err != nil {
		return err
	}
	return nil
}

func (sqld *SQLiteDumper) MarkBatch() (int, error) {
	// Get value of largest ID
	rows, err := sqld.dbh.Query(`
		SELECT max(id) FROM raw_requests
		 WHERE (batch == 0 OR batch IS NULL)
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	rv := rows.Next()
	err = rows.Err()
	if rv == false {
		return 0, err
	}
	var maxID sql.NullInt64
	err = rows.Scan(&maxID)
	if err != nil {
		return 0, err
	}
	rows.Close()

	if maxID.Valid == false {
		return 0, nil
	}

	// Update batch to the value of the largest ID in the current batch
	res, err := sqld.dbh.Exec(`
		UPDATE raw_requests SET batch = $1
		 WHERE (batch == 0 OR batch IS NULL)
		   AND id <= $1
	`, maxID)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	} else if n <= 0 {
		return 0, nil
	}

	return int(maxID.Int64), nil
}

func (sqld *SQLiteDumper) ReadRequests(batchID int) ([]dumpto.Request, error) {
	// TODO: make initial size configurable
	reqs := make([]dumpto.Request, 0, 32)
	n := 0

	// Get all requests for this batch
	rows, err := sqld.dbh.Query(`
			SELECT id, head, data, date
			  FROM raw_requests
			 WHERE batch == $1
			 ORDER BY date ASC
		`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tmpID int
	for rows.Next() {
		if rows.Err() == io.EOF {
			break
		}
		req := &dumpto.Request{}
		err = rows.Scan(&tmpID, &req.Head, &req.Data, &req.When)
		if err != nil {
			return nil, err
		}
		req.ID = &tmpID

		reqs = append(reqs, *req)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	return reqs, nil
}

func (sqld *SQLiteDumper) BatchDone(batchID int) error {
	_, err := sqld.dbh.Exec(`
		UPDATE raw_requests SET batch=-1
		 WHERE batch = $1
	`, batchID)
	if err != nil {
		return err
	}
	return nil
}
