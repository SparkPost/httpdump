// Package sqlite3 allows storing http request data in sqlite databases.
package sqlite3

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	re "regexp"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/yargevad/httpdump/storage"
)

// https://www.sqlite.org/rescode.html
const (
	SQLITE_LOCKED = 6
)

// Map shortcuts strings to verbose date formats.
var DateFormats = map[string]string{
	"day":    "2006-01-02T-MST",
	"hour":   "2006-01-02T15-MST",
	"minute": "2006-01-02T15-04-MST",
}

type SQLiteDumper struct {
	dbPath        string
	inMemory      bool
	dateFormat    string
	curDate       string
	curDateRWLock *sync.RWMutex
	dbh           *sql.DB
	dbhRWLock     *sync.RWMutex
}

// reopenDBFile opens a database handle and initializes the schema if necessary.
func (ctx *SQLiteDumper) reopenDBFile(dbfile string) error {
	mustInit := false
	if ctx.inMemory == true {
		if ctx.dbh == nil {
			mustInit = true
		} else {
			return nil
		}

	} else {
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
	}

	dbh, err := sql.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}
	if err = dbh.Ping(); err != nil {
		return err
	}

	if mustInit {
		log.Printf("Initializing schema for [%s]\n", dbfile)
		ddls := []string{`
			CREATE TABLE raw_requests (
				id    integer primary key autoincrement,
				head  blob,
				data  blob,
				date  timestamp,
				batch int
			)`,
			`CREATE INDEX raw_requests_batch_idx ON raw_requests (batch)`,
		}
		for _, ddl := range ddls {
			_, err := dbh.Exec(ddl, nil)
			if err != nil {
				return err
			}
		}
	}

	ctx.dbh = dbh
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
	if ctx.inMemory == true {
		if ctx.dbh == nil {
			log.Printf("Opening database [%s]\n", ctx.dateFormat)
			err := ctx.reopenDBFile(ctx.dateFormat)
			if err != nil {
				return err
			}
		}

	} else {
		cur := ctx.getCurDate()
		nowstr := now.Format(DateFormats[ctx.dateFormat])
		// If the date has changed since the last time we checked, open the new file.
		if cur != nowstr {
			ctx.setCurDate(nowstr)
			dbfile := fmt.Sprintf("%s.db", nowstr)
			ctx.dbhRWLock.Lock()
			defer ctx.dbhRWLock.Unlock()
			if ctx.dbh != nil {
				ctx.dbh.Close()
			}
			err := ctx.reopenDBFile(dbfile)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var dbPattern *re.Regexp = re.MustCompile(`\w+.db`)

// NewDumper returns an initialized SQLiteDumper that dumps request data to an SQLite db file.
func NewDumper(dateFmt, dbPath string) (*SQLiteDumper, error) {
	inMemory := false
	if dateFmt == "memory" {
		// Use an in-memory database
		inMemory = true
		// With a shared cache: http://www.sqlite.org/sharedcache.html
		dateFmt = "file:foo.db?cache=shared&mode=memory"

		//} else if dbPattern.MatchString(dateFmt) {
		// TODO: if dateFmt matches "\w+.db", use that as the db filename

	} else if dateFmt != "day" && dateFmt != "hour" && dateFmt != "minute" {
		// Use a dynamic filename based on the current time.
		return nil, fmt.Errorf("`datefmt` must be one of (`day`, `hour`, `minute`), not [%s]", dateFmt)
	}

	// Set up a dumper, configured with the provided date granularity.
	sqld := &SQLiteDumper{
		dbPath:        dbPath,
		inMemory:      inMemory,
		dateFormat:    dateFmt,
		curDateRWLock: &sync.RWMutex{},
		dbhRWLock:     &sync.RWMutex{},
	}

	// Make sure we're using the db file for "right now", and
	// make sure the database handle is initialized right away
	err := sqld.updateCurDate(time.Now())
	if err != nil {
		return nil, err
	}

	return sqld, nil
}

func QueryRetry(db *sql.DB, codes map[int]bool, after time.Duration, query string, args ...interface{}) (*sql.Rows, error) {
	for {
		rows, err := db.Query(query, args...)
		if err != nil {
			if sqlErr, ok := err.(sqlite3.Error); ok {
				if _, ok := codes[int(sqlErr.Code)]; ok {
					// delay for the specified amount of time before retrying
					select {
					case <-time.After(after):
					}
				} else {
					return nil, fmt.Errorf("%s: %d/%d", err, int(sqlErr.Code), int(sqlErr.ExtendedCode))
				}
			} else {
				log.Printf("Couldn't convert error to sqlite3.Error")
				return nil, err
			}
		} else {
			return rows, err
		}
	}
}

func ExecRetry(db *sql.DB, codes map[int]bool, after time.Duration, query string, args ...interface{}) (sql.Result, error) {
	for {
		res, err := db.Exec(query, args...)
		if err != nil {
			if sqlErr, ok := err.(sqlite3.Error); ok {
				if _, ok := codes[int(sqlErr.Code)]; ok {
					// delay for the specified amount of time before retrying
					select {
					case <-time.After(after):
					}
				} else {
					return nil, fmt.Errorf("%s: %d/%d", err, int(sqlErr.Code), int(sqlErr.ExtendedCode))
				}
			} else {
				log.Printf("Couldn't convert error to sqlite3.Error")
				return nil, err
			}
		} else {
			return res, err
		}
	}
}

func (sqld *SQLiteDumper) Dump(req *storage.Request) error {
	// Get a "read lock" on our db pool, if needed.
	// The in-memory db doesn't need a lock since it won't change after the first init.
	if sqld.inMemory == false {
		sqld.dbhRWLock.RLock()
		defer sqld.dbhRWLock.RUnlock()
	}

	// Insert data for the current request, retrying on SQL_LOCKED.
	_, err := ExecRetry(sqld.dbh, map[int]bool{SQLITE_LOCKED: true}, (10 * time.Millisecond), `
			INSERT INTO raw_requests (head, data, date)
			VALUES ($1, $2, $3)
		`, string(req.Head), string(req.Data), req.When)
	if err != nil {
		return err
	}
	return nil
}

func (sqld *SQLiteDumper) MarkBatch() (int64, error) {
	if sqld.dbh == nil {
		log.Printf("MarkBatch: Can't write to nil database handle!\n")
		return 0, nil
	}

	// Get value of largest ID, retrying on SQL_LOCKED.
	rows, err := QueryRetry(sqld.dbh, map[int]bool{SQLITE_LOCKED: true}, (10 * time.Millisecond), `
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

	// Update batch to the value of the largest ID in the current batch, retrying on SQL_LOCKED.
	res, err := ExecRetry(sqld.dbh, map[int]bool{SQLITE_LOCKED: true}, (10 * time.Millisecond), `
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

	return maxID.Int64, nil
}

func (sqld *SQLiteDumper) ReadRequests(batchID int64) ([]storage.Request, error) {
	// TODO: make initial size configurable
	reqs := make([]storage.Request, 0, 32)
	n := 0

	// Get all requests for this batch, retrying on SQL_LOCKED.
	rows, err := QueryRetry(sqld.dbh, map[int]bool{SQLITE_LOCKED: true}, (10 * time.Millisecond), `
			SELECT id, head, data, date
			  FROM raw_requests
			 WHERE batch == $1
			 ORDER BY date ASC
		`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tmpID int64
	for rows.Next() {
		if rows.Err() == io.EOF {
			break
		}
		req := &storage.Request{}
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

func (sqld *SQLiteDumper) BatchDone(batchID int64) error {
	_, err := ExecRetry(sqld.dbh, map[int]bool{SQLITE_LOCKED: true}, (10 * time.Millisecond), `
		DELETE FROM raw_requests
		 WHERE batch = $1
	`, batchID)
	if err != nil {
		return err
	}
	return nil
}
